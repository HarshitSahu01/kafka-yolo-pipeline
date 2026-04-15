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
  • JPEG encode + Kafka produce + CSV write happen in the publish thread (I/O only).
  • Bounded queues provide natural back-pressure — no unbounded RAM growth.
  • NO frame dropping — every frame that arrives is processed.

Tuning (all env-overridable):
  CONF_THRESHOLD      default 0.35   min detection confidence
  INFER_BATCH         default 8      frames per YOLO call (raise for GPU)
  DECODE_QUEUE_SIZE   default 32
  RESULT_QUEUE_SIZE   default 32
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
import torch
from confluent_kafka import Consumer, Producer
from ultralytics import YOLO

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",  "localhost:9092")
TOPIC_VIDEO    = "quickstart-events"
TOPIC_RESULTS  = "yolo-results"
CONSUMER_GROUP = "yolo-cluster-group"
YOLO_MODEL     = "yolov8n.pt"

CONF_THRESHOLD    = float(os.getenv("CONF_THRESHOLD",   "0.35"))
INFER_BATCH       = int(os.getenv("INFER_BATCH",        "16"))   # batch=16 gives peak GPU throughput
DECODE_QUEUE_SIZE = int(os.getenv("DECODE_QUEUE_SIZE",  "64"))   # large enough to never starve 16-frame batches
RESULT_QUEUE_SIZE = int(os.getenv("RESULT_QUEUE_SIZE",  "64"))
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
    def __init__(self, window: int = 90):
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
) -> tuple[np.ndarray, list]:
    """Annotate frame and return (annotated_frame, csv_rows).
    CSV rows are written in the publish thread to keep infer GPU-bound.
    """
    class_counts: dict[str, int] = {}
    csv_rows = []

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
        csv_rows.append([int(time.time() * 1000), None, frame_id,
                         label, f"{conf:.4f}", x1, y1, x2, y2])

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

    return frame, csv_rows


# ── Stage 1 — consume + decode ────────────────────────────────────────────────
def _stage_consume(
    consumer: Consumer,
    decode_q: queue.Queue,
    shutdown: threading.Event,
    log: logging.Logger,
) -> None:
    """Poll Kafka, JPEG-decode each frame, push (frame_id, ts_ms, frame) to decode_q.
    
    NO frame dropping — every valid frame is forwarded.
    """
    while not shutdown.is_set():
        msg = consumer.poll(0.01)   # 10ms timeout — keeps polling tight
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

        # Decode JPEG → numpy (CPU, fast)
        arr   = np.frombuffer(msg.value(), dtype=np.uint8)
        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if frame is None:
            continue

        # Block here when infer thread is busy — natural back-pressure
        decode_q.put((frame_id, timestamp_ms, frame))

    decode_q.put(_POISON)
    log.debug("Stage 1 (consume) done.")


# ── Stage 2 — batch infer + annotate ─────────────────────────────────────────
def _stage_infer(
    model: YOLO,
    infer_device: str,
    decode_q: queue.Queue,
    result_q: queue.Queue,
    fps_tracker: _FPSTracker,
    wid: str,
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
            all_results = model(frames, verbose=False, device=infer_device)
        except Exception as exc:
            log.error("Inference error (batch %d): %s", len(batch), exc)
            continue

        fps = fps_tracker.tick()

        for (frame_id, timestamp_ms, frame), result in zip(batch, all_results):
            latency_ms = max(0.0, now_ms() - timestamp_ms)
            try:
                annotated, csv_rows = _annotate(
                    frame, result,
                    frame_id=frame_id,
                    fps=fps,
                    latency_ms=latency_ms,
                )
                # Stamp worker_id into csv_rows
                for row in csv_rows:
                    row[1] = wid
            except Exception as exc:
                log.error("Annotate error frame %d: %s", frame_id, exc)
                annotated = frame
                csv_rows  = []

            result_q.put((frame_id, annotated, csv_rows))

    result_q.put(_POISON)
    log.debug("Stage 2 (infer) done.")


# ── Stage 3 — encode + publish + CSV write ───────────────────────────────────
def _stage_publish(
    producer: Producer,
    consumer: Consumer,
    result_q: queue.Queue,
    csv_writer,
    shutdown: threading.Event,
    log: logging.Logger,
) -> None:
    """JPEG-encode annotated frames, publish to yolo-results, write CSV rows."""
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

        frame_id, annotated, csv_rows = item

        # Write detections CSV (I/O — fine on this thread)
        if csv_writer is not None:
            for row in csv_rows:
                csv_writer.writerow(row)

        # JPEG encode
        ok, buf = cv2.imencode(
            ".jpg", annotated,
            [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
        )
        if not ok:
            continue

        headers = [("frame_id", str(frame_id).encode())]
        try:
            producer.produce(TOPIC_RESULTS, buf.tobytes(), headers=headers)
            producer.poll(0)
        except Exception as exc:
            log.error("Publish error frame %d: %s", frame_id, exc)

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

    # ── GPU / device selection ──────────────────────────────────────────────
    if torch.cuda.is_available():
        infer_device = "cuda"
        log.info("Worker %s — GPU: %s", wid, torch.cuda.get_device_name(0))
    else:
        infer_device = "cpu"
        log.warning("Worker %s — CUDA not available, falling back to CPU.", wid)

    # ── CSV log ────────────────────────────────────────────────────────────
    csv_path = DETECTION_LOG.replace(".csv", f"_{wid}.csv")
    csv_file   = open(csv_path, "a", newline="", buffering=1)
    csv_writer = csv.writer(csv_file)
    if os.path.getsize(csv_path) == 0:
        csv_writer.writerow(_CSV_HEADER)

    # ── YOLO ───────────────────────────────────────────────────────────────
    model = YOLO(YOLO_MODEL)
    # Warm-up: send one dummy frame to initialise CUDA context before real work
    dummy = np.zeros((640, 640, 3), dtype=np.uint8)
    model([dummy], verbose=False, device=infer_device)
    log.info("Worker %s — YOLO loaded on %s. conf=%.2f  batch=%d",
             wid, infer_device, CONF_THRESHOLD, INFER_BATCH)

    # ── Kafka consumer ─────────────────────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers":         KAFKA_BROKER,
        "group.id":                  CONSUMER_GROUP,
        "auto.offset.reset":         "earliest",
        # Auto-commit every 500 ms — avoids per-frame commit RTT overhead
        "enable.auto.commit":        True,
        "auto.commit.interval.ms":   500,
        "max.poll.interval.ms":      300_000,
        "fetch.max.bytes":           20_000_000,
        "max.partition.fetch.bytes": 20_000_000,
        # Aggressive fetch — wake up as soon as ANY data is available
        "fetch.min.bytes":           1,
        "fetch.wait.max.ms":         5,
    })
    consumer.subscribe([TOPIC_VIDEO])

    # ── Kafka producer ─────────────────────────────────────────────────────
    producer = Producer({
        "bootstrap.servers":            KAFKA_BROKER,
        "linger.ms":                    0,          # no added latency
        "compression.type":             "snappy",
        "message.max.bytes":            10_000_000,
        "queue.buffering.max.messages": 10_000,
        "socket.send.buffer.bytes":     1_048_576,
    })

    # ── Queues ─────────────────────────────────────────────────────────────
    fps_tracker = _FPSTracker(window=90)
    decode_q: queue.Queue = queue.Queue(maxsize=DECODE_QUEUE_SIZE)
    result_q: queue.Queue = queue.Queue(maxsize=RESULT_QUEUE_SIZE)

    # ── Threads ────────────────────────────────────────────────────────────
    threads = [
        threading.Thread(
            target=_stage_consume,
            args=(consumer, decode_q, shutdown, log),
            name=f"w{wid}-consume", daemon=True,
        ),
        threading.Thread(
            target=_stage_infer,
            args=(model, infer_device, decode_q, result_q,
                  fps_tracker, wid, shutdown, log),
            name=f"w{wid}-infer", daemon=True,
        ),
        threading.Thread(
            target=_stage_publish,
            args=(producer, consumer, result_q, csv_writer, shutdown, log),
            name=f"w{wid}-publish", daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    log.info("Worker %s pipeline running (3 stages, device=%s, batch=%d).",
             wid, infer_device, INFER_BATCH)

    try:
        last_log = time.monotonic()
        while not shutdown.is_set():
            time.sleep(0.5)
            now = time.monotonic()
            if now - last_log >= 5.0:
                log.info("Worker %s — FPS: %.1f", wid, fps_tracker.fps)
                last_log = now
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