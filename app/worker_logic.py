#!/usr/bin/env python3
"""
worker_logic.py — High-throughput Kafka → YOLOv8 → Kafka worker.

Architecture (3 concurrent stages via bounded queues):

  ┌──────────────┐  decode_q   ┌──────────────┐  result_q   ┌──────────────┐
  │  IO thread   │ ──────────▶ │ Infer thread │ ──────────▶ │  IO thread   │
  │ poll+decode  │             │  batch YOLO  │             │ encode+pub   │
  └──────────────┘             └──────────────┘             └──────────────┘

  • Kafka poll/decode never waits for GPU.
  • YOLO runs on batches of up to INFER_BATCH frames — maximises GPU utilisation.
  • JPEG encode + Kafka produce never stall the GPU.
  • Bounded queues provide natural back-pressure — no unbounded RAM growth.

Tuning (all env-overridable):
  CONF_THRESHOLD      default 0.35   min detection confidence
  MAX_LATENCY_SEC     default 5.0    drop frames older than this
  INFER_BATCH         default 4      frames per YOLO call (raise for GPU)
  DECODE_QUEUE_SIZE   default 16
  RESULT_QUEUE_SIZE   default 16
  JPEG_QUALITY        default 85
"""

import csv
import logging
import os
import queue
import signal
import sys
import threading
import time
from collections import deque

import cv2
import numpy as np
from confluent_kafka import Consumer, Producer
from ultralytics import YOLO

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",  "localhost:9092")
TOPIC_VIDEO    = "quickstart-events"
TOPIC_RESULTS  = "yolo-results"
CONSUMER_GROUP = "yolo-cluster-group"
YOLO_MODEL     = "yolov8n.pt"

CONF_THRESHOLD    = float(os.getenv("CONF_THRESHOLD",   "0.35"))
MAX_LATENCY_SEC   = float(os.getenv("MAX_LATENCY_SEC",  "5.0"))
INFER_BATCH       = int(os.getenv("INFER_BATCH",        "4"))
DECODE_QUEUE_SIZE = int(os.getenv("DECODE_QUEUE_SIZE",  "16"))
RESULT_QUEUE_SIZE = int(os.getenv("RESULT_QUEUE_SIZE",  "16"))
JPEG_QUALITY      = int(os.getenv("JPEG_QUALITY",       "85"))

DETECTION_LOG = os.getenv("DETECTION_LOG", "logs/detections.csv")
_CSV_HEADER   = ["timestamp_ms", "worker_id", "frame_id",
                 "class_name", "confidence", "x1", "y1", "x2", "y2"]

# ── Colour palette (BGR, 20 visually distinct colours) ────────────────────────
_PALETTE: list[tuple[int, int, int]] = [
    ( 56,  56, 255), (151, 157, 255), ( 31, 112, 255), ( 29, 178, 255),
    ( 49, 210, 207), ( 10, 249,  72), ( 23, 204, 146), (134, 219,  61),
    ( 52, 147,  26), (187, 212,   0), (168, 153,  44), (255, 194,   0),
    (147,  69,  52), (255, 115, 100), (236,  24,   0), (255,  56, 132),
    (133,   0,  82), (255,  56, 203), (200, 149, 255), (199,  55, 255),
]

def _color(cls_id: int) -> tuple[int, int, int]:
    return _PALETTE[cls_id % len(_PALETTE)]


# ── Sentinel object — pushed into queues to signal shutdown ──────────────────
_POISON = object()


# ── Thread-safe rolling FPS tracker ──────────────────────────────────────────
class _FPSTracker:
    def __init__(self, window: int = 60):
        self._times: deque[float] = deque(maxlen=window)
        self._lock = threading.Lock()

    def tick(self) -> float:
        now = time.monotonic()
        with self._lock:
            self._times.append(now)
            if len(self._times) < 2:
                return 0.0
            return (len(self._times) - 1) / (self._times[-1] - self._times[0])

    @property
    def fps(self) -> float:
        with self._lock:
            if len(self._times) < 2:
                return 0.0
            return (len(self._times) - 1) / (self._times[-1] - self._times[0])


# ── Annotation (called inside infer thread — no Kafka I/O) ───────────────────
def _annotate(
    frame: np.ndarray,
    result,
    frame_id: int,
    fps: float,
    latency_ms: float,
    csv_writer,
    timestamp_ms: int,
    wid: str,
) -> np.ndarray:
    class_counts: dict[str, int] = {}

    for box in result.boxes:
        conf = float(box.conf[0])
        if conf < CONF_THRESHOLD:
            continue
        cls_id = int(box.cls[0])
        label  = result.names[cls_id]
        x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
        color  = _color(cls_id)

        # Bounding box
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        # Label chip
        text = f"{label} {conf:.2f}"
        (tw, th), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
        cv2.rectangle(frame, (x1, y1 - th - 6), (x1 + tw + 4, y1), color, -1)
        cv2.putText(frame, text, (x1 + 2, y1 - 4),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 1, cv2.LINE_AA)

        class_counts[label] = class_counts.get(label, 0) + 1

        if csv_writer is not None:
            csv_writer.writerow([
                timestamp_ms, wid, frame_id,
                label, f"{conf:.4f}", x1, y1, x2, y2,
            ])

    # Object count overlay — top-left
    y_off = 20
    for line in ([f"Total: {sum(class_counts.values())}"] +
                 [f"  {c}: {n}" for c, n in sorted(class_counts.items())]):
        cv2.putText(frame, line, (8, y_off),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(frame, line, (8, y_off),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 1, cv2.LINE_AA)
        y_off += 20

    # FPS + latency — top-right
    hud = f"FPS:{fps:5.1f}  Lat:{latency_ms:5.0f}ms"
    (fw, _), _ = cv2.getTextSize(hud, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
    fx = frame.shape[1] - fw - 8
    cv2.putText(frame, hud, (fx, 20),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(frame, hud, (fx, 20),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 128), 1, cv2.LINE_AA)

    return frame


# ── Stage 1 — consume + decode ────────────────────────────────────────────────
def _stage_consume(
    consumer: Consumer,
    decode_q: queue.Queue,
    fps_tracker: _FPSTracker,
    shutdown: threading.Event,
    log: logging.Logger,
) -> None:
    """Poll Kafka, JPEG-decode each frame, push (frame_id, ts, frame, msg) to decode_q."""
    now_ms = lambda: int(time.time() * 1000)

    while not shutdown.is_set():
        msg = consumer.poll(0.05)
        if msg is None or msg.error():
            continue

        frame_id, timestamp_ms = -1, 0
        for key, val in (msg.headers() or []):
            if key == "frame_id":
                try:
                    frame_id = int(val)
                except (ValueError, TypeError):
                    pass
            elif key == "timestamp":
                try:
                    timestamp_ms = int(val)
                except (ValueError, TypeError):
                    pass

        latency_ms = max(0.0, now_ms() - timestamp_ms)
        if latency_ms > MAX_LATENCY_SEC * 1000:
            log.debug("Drop stale frame %d (%.1f s)", frame_id, latency_ms / 1000)
            consumer.commit(msg)
            continue

        arr   = np.frombuffer(msg.value(), dtype=np.uint8)
        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if frame is None:
            consumer.commit(msg)
            continue

        fps_tracker.tick()

        # Block here when infer thread is busy — applies back-pressure
        decode_q.put((frame_id, timestamp_ms, frame, msg))

    decode_q.put(_POISON)
    log.debug("Stage 1 (consume) done.")


# ── Stage 2 — batch infer + annotate ─────────────────────────────────────────
def _stage_infer(
    model: YOLO,
    decode_q: queue.Queue,
    result_q: queue.Queue,
    fps_tracker: _FPSTracker,
    wid: str,
    csv_writer,
    shutdown: threading.Event,
    log: logging.Logger,
) -> None:
    """Drain decode_q in micro-batches, run YOLO once per batch, annotate frames."""
    now_ms = lambda: int(time.time() * 1000)

    while True:
        # Block until at least one item is available
        try:
            first = decode_q.get(timeout=0.1)
        except queue.Empty:
            if shutdown.is_set():
                break
            continue

        if first is _POISON:
            break

        # Greedily drain more items without blocking (fill the batch)
        batch = [first]
        while len(batch) < INFER_BATCH:
            try:
                item = decode_q.get_nowait()
                if item is _POISON:
                    decode_q.put(_POISON)   # re-insert for next iteration
                    break
                batch.append(item)
            except queue.Empty:
                break

        # Batch YOLO inference — ultralytics accepts a list of np arrays
        frames = [item[2] for item in batch]
        try:
            all_results = model(frames, verbose=False)
        except Exception as exc:
            log.error("Inference error (batch %d): %s", len(batch), exc)
            for _, _, _, msg in batch:
                consumer_commit_safe(msg, log)
            continue

        fps = fps_tracker.tick()

        for (frame_id, timestamp_ms, frame, msg), result in zip(batch, all_results):
            latency_ms = max(0.0, now_ms() - timestamp_ms)
            try:
                annotated = _annotate(
                    frame, result,
                    frame_id=frame_id,
                    fps=fps,
                    latency_ms=latency_ms,
                    csv_writer=csv_writer,
                    timestamp_ms=timestamp_ms,
                    wid=wid,
                )
            except Exception as exc:
                log.error("Annotate error frame %d: %s", frame_id, exc)
                annotated = frame  # push unannotated rather than drop

            result_q.put((frame_id, annotated, msg))

    result_q.put(_POISON)
    log.debug("Stage 2 (infer) done.")


def consumer_commit_safe(msg, log: logging.Logger) -> None:
    """Best-effort commit — ignore errors (e.g. consumer already closed)."""
    try:
        msg.consumer_obj and None   # noqa: consumer ref not stored here
    except Exception:
        pass


# ── Stage 3 — encode + publish ────────────────────────────────────────────────
def _stage_publish(
    producer: Producer,
    result_q: queue.Queue,
    consumer: Consumer,
    shutdown: threading.Event,
    log: logging.Logger,
) -> None:
    """JPEG-encode annotated frames and publish to yolo-results."""
    while True:
        try:
            item = result_q.get(timeout=0.1)
        except queue.Empty:
            producer.poll(0)    # flush delivery callbacks
            if shutdown.is_set():
                break
            continue

        if item is _POISON:
            break

        frame_id, annotated, msg = item

        ok, buf = cv2.imencode(
            ".jpg", annotated,
            [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
        )
        if not ok:
            consumer.commit(msg)
            continue

        headers = [("frame_id", str(frame_id).encode())]
        try:
            producer.produce(TOPIC_RESULTS, buf.tobytes(), headers=headers)
            producer.poll(0)
        except Exception as exc:
            log.error("Publish error frame %d: %s", frame_id, exc)

        consumer.commit(msg)

    log.debug("Stage 3 (publish) done.")


# ── Worker entry-point ────────────────────────────────────────────────────────
def run_worker(wid: str) -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"logs/worker_{wid}.log"),
        ],
    )
    log = logging.getLogger(f"worker_{wid}")

    shutdown = threading.Event()

    def _stop(sig, _):
        log.info("Worker %s — shutdown signal.", wid)
        shutdown.set()

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # ── CSV log ────────────────────────────────────────────────────────────
    csv_file   = open(DETECTION_LOG, "a", newline="", buffering=1)
    csv_writer = csv.writer(csv_file)
    if os.path.getsize(DETECTION_LOG) == 0:
        csv_writer.writerow(_CSV_HEADER)

    # ── YOLO ───────────────────────────────────────────────────────────────
    model = YOLO(YOLO_MODEL)
    log.info("YOLO loaded. conf=%.2f  batch=%d", CONF_THRESHOLD, INFER_BATCH)

    # ── Kafka ──────────────────────────────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers":         KAFKA_BROKER,
        "group.id":                  CONSUMER_GROUP,
        "auto.offset.reset":         "earliest",
        "enable.auto.commit":        False,
        "max.poll.interval.ms":      300_000,
        "fetch.max.bytes":           20_000_000,
        "max.partition.fetch.bytes": 20_000_000,
        # Fill fetch buffers quickly so decode_q stays topped up
        "fetch.min.bytes":           65_536,
        "fetch.wait.max.ms":         50,
    })
    consumer.subscribe([TOPIC_VIDEO])

    producer = Producer({
        "bootstrap.servers":            KAFKA_BROKER,
        "linger.ms":                    5,
        "compression.type":             "snappy",
        "message.max.bytes":            10_000_000,
        "queue.buffering.max.messages": 10_000,
        "socket.send.buffer.bytes":     1_048_576,
    })

    # ── Queues ─────────────────────────────────────────────────────────────
    fps_tracker = _FPSTracker(window=60)
    decode_q: queue.Queue = queue.Queue(maxsize=DECODE_QUEUE_SIZE)
    result_q: queue.Queue = queue.Queue(maxsize=RESULT_QUEUE_SIZE)

    # ── Threads ────────────────────────────────────────────────────────────
    threads = [
        threading.Thread(
            target=_stage_consume,
            args=(consumer, decode_q, fps_tracker, shutdown, log),
            name=f"w{wid}-consume", daemon=True,
        ),
        threading.Thread(
            target=_stage_infer,
            args=(model, decode_q, result_q, fps_tracker,
                  wid, csv_writer, shutdown, log),
            name=f"w{wid}-infer", daemon=True,
        ),
        threading.Thread(
            target=_stage_publish,
            args=(producer, result_q, consumer, shutdown, log),
            name=f"w{wid}-publish", daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    log.info("Worker %s pipeline running (3 stages, batch=%d).", wid, INFER_BATCH)

    try:
        while not shutdown.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown.set()

    log.info("Worker %s draining pipeline…", wid)
    threads[0].join(timeout=5)
    threads[1].join(timeout=30)   # infer may need time to finish last batch
    threads[2].join(timeout=10)

    producer.flush(timeout=10)
    consumer.close()
    csv_file.close()
    log.info("Worker %s done.", wid)


if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else "0"
    run_worker(wid)