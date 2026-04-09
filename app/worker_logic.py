#!/usr/bin/env python3
"""
worker_logic.py — Kafka consumer: decode JPEG frames, run YOLOv8, annotate,
                  then publish the annotated JPEG back to the results topic.
Started automatically by spawn_workers.py — one process per worker.
"""

import logging
from multiprocessing import shared_memory
import os
import signal
import struct
import sys
import time

import cv2
import numpy as np
from confluent_kafka import Consumer, Producer
from ultralytics import YOLO

import warnings
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="multiprocessing.resource_tracker"
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_VIDEO     = "quickstart-events"
TOPIC_RESULTS   = "yolo-results"
CONSUMER_GROUP  = "yolo-cluster-group"
YOLO_MODEL      = "yolov8n.pt"
# Disable latency-based dropping by default; keep variable for compatibility
MAX_LATENCY_SEC = 0.0
JPEG_QUALITY    = 80
FPS_FLUSH_INTERVAL = 0.25

# Colour palette — one colour per class id (mod 20)
_PALETTE = [
    (255, 56,  56 ), (255, 157, 151), (255, 112, 31 ), (255, 178, 29 ),
    (207, 210, 49 ), (72,  249, 10 ), (146, 204, 23 ), (61,  219, 134),
    (26,  147, 52 ), (0,   212, 187), (44,  153, 168), (0,   194, 255),
    (52,  69,  147), (100, 115, 255), (0,   24,  236), (132, 56,  255),
    (82,  0,   133), (203, 56,  255), (255, 149, 200), (255, 55,  199),
]


def _annotate(frame: np.ndarray, results) -> np.ndarray:
    """Draw boxes, labels, and confidence on the frame in-place."""
    for box in results[0].boxes:
        cls_id = int(box.cls[0])
        conf   = float(box.conf[0])
        label  = results[0].names[cls_id]
        x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
        color  = _PALETTE[cls_id % len(_PALETTE)]
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
        text   = f"{label} {conf:.2f}"
        (tw, th), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
        cv2.rectangle(frame, (x1, y1 - th - 6), (x1 + tw + 4, y1), color, -1)
        cv2.putText(frame, text, (x1 + 2, y1 - 4),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 1, cv2.LINE_AA)
    return frame


def run_worker(wid: str) -> None:
    # ── Per-worker logging ─────────────────────────────────────────────────
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

    # ── Signal handling ────────────────────────────────────────────────────
    _shutdown = False

    def _stop(sig, _):
        nonlocal _shutdown
        _shutdown = True

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # Optional shared-memory counter used by spawn_workers.py for combined FPS.
    shm_name = os.getenv("FPS_SHM_NAME", "")
    slot_size = int(os.getenv("FPS_SHM_SLOT_SIZE", "16"))
    wid_idx = int(wid)
    fps_shm = None
    shm_buf = None

    if shm_name:
        try:
            fps_shm = shared_memory.SharedMemory(name=shm_name)
            shm_buf = fps_shm.buf
            # Avoid resource_tracker warnings in child processes: the child
            # did not create this shared memory, so unregister it from the
            # resource tracker to prevent leak warnings at interpreter shutdown.
            try:
                from multiprocessing import resource_tracker

                try:
                    if fps_shm.name in resource_tracker._resource_tracker._cache.get("shared_memory", set()):
                        resource_tracker.unregister(fps_shm.name, "shared_memory")
                except Exception:
                    pass
            except Exception:
                pass
        except Exception as exc:
            log.warning("FPS shared memory unavailable: %s", exc)
            fps_shm = None
            shm_buf = None

    def _publish_count(count: int) -> None:
        if shm_buf is None:
            return
        base = wid_idx * slot_size
        seq = struct.unpack_from("Q", shm_buf, base)[0]
        struct.pack_into("Q", shm_buf, base, seq + 1)      # begin write (odd)
        struct.pack_into("Q", shm_buf, base + 8, count)
        struct.pack_into("Q", shm_buf, base, seq + 2)      # end write (even)

    # ── Setup ──────────────────────────────────────────────────────────────
    model = YOLO(YOLO_MODEL)

    consumer = Consumer({
        "bootstrap.servers":                  KAFKA_BROKER,
        "group.id":                           CONSUMER_GROUP,
        "auto.offset.reset":                  "latest",
        "max.poll.interval.ms":               300_000,
        "topic.metadata.refresh.interval.ms": 2000, 
    })
    consumer.subscribe([TOPIC_VIDEO])

    producer = Producer({
        "bootstrap.servers":            KAFKA_BROKER,
        "linger.ms":                    5,
        "compression.type":             "snappy",
        "message.max.bytes":            10_000_000,   # 10 MB — handles annotated frames
        "queue.buffering.max.messages": 10_000,
    })

    log.info("Worker %s ready (annotation mode).", wid)
    processed_count = 0
    last_fps_flush_ts = time.monotonic()

    # ── Main loop ──────────────────────────────────────────────────────────
    try:
        while not _shutdown:
            msg = consumer.poll(0.1)
            if msg is None or msg.error():
                continue

            # Extract headers
            frame_id, timestamp = -1, 0
            msg_fps = b""
            for key, val in (msg.headers() or []):
                if key == "frame_id":
                    frame_id = int(val)
                elif key == "timestamp":
                    timestamp = int(val)
                elif key == "fps":
                    msg_fps = val

            # Latency-based dropping disabled (MAX_LATENCY_SEC = 0)

            # Decode
            arr   = np.frombuffer(msg.value(), dtype=np.uint8)
            frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if frame is None:
                continue

            # Inference + annotation
            try:
                results     = model(frame, verbose=False)
                annotated   = _annotate(frame, results)
            except Exception as exc:
                log.error("Inference error on frame %d: %s", frame_id, exc)
                continue

            # Encode annotated frame back to JPEG
            ok, buf = cv2.imencode(".jpg", annotated,
                                   [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
            if not ok:
                continue

            headers = [("frame_id", str(frame_id).encode())]
            if msg_fps:
                headers.append(("fps", msg_fps))
            try:
                producer.produce(TOPIC_RESULTS, buf.tobytes(), headers=headers)
                producer.poll(0)
                processed_count += 1
            except Exception as exc:
                log.error("Publish error frame %d: %s", frame_id, exc)

            now = time.monotonic()
            if now - last_fps_flush_ts >= FPS_FLUSH_INTERVAL:
                _publish_count(processed_count)
                last_fps_flush_ts = now

    finally:
        log.info("Worker %s shutting down.", wid)
        _publish_count(processed_count)
        consumer.close()
        producer.flush()
        if fps_shm is not None:
            fps_shm.close()
        log.info("Worker %s done.", wid)


if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else "0"
    run_worker(wid)
