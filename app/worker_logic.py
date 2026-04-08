#!/usr/bin/env python3
"""
worker_logic.py — Kafka consumer: decode JPEG frames, run YOLOv8, annotate,
                  then publish the annotated JPEG back to the results topic.
Started automatically by spawn_workers.py — one process per worker.
"""

import logging
import os
import signal
import sys
import time

import cv2
import numpy as np
from confluent_kafka import Consumer, Producer
from ultralytics import YOLO

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_VIDEO     = "quickstart-events"
TOPIC_RESULTS   = "yolo-results"
CONSUMER_GROUP  = "yolo-cluster-group"
YOLO_MODEL      = "yolov8n.pt"
MAX_LATENCY_SEC = 0.5
JPEG_QUALITY    = 80

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

    # ── Setup ──────────────────────────────────────────────────────────────
    model = YOLO(YOLO_MODEL)

    consumer = Consumer({
        "bootstrap.servers":    KAFKA_BROKER,
        "group.id":             CONSUMER_GROUP,
        "auto.offset.reset":    "latest",
        "max.poll.interval.ms": 300_000,
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

    # ── Main loop ──────────────────────────────────────────────────────────
    try:
        while not _shutdown:
            msg = consumer.poll(0.1)
            if msg is None or msg.error():
                continue

            # Extract headers
            frame_id, timestamp = -1, 0
            for key, val in (msg.headers() or []):
                if key == "frame_id":
                    frame_id = int(val)
                elif key == "timestamp":
                    timestamp = int(val)

            # Drop stale frames
            latency = max(0.0, (int(time.time() * 1000) - timestamp) / 1000.0)
            if latency > MAX_LATENCY_SEC:
                continue

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
            try:
                producer.produce(TOPIC_RESULTS, buf.tobytes(), headers=headers)
                producer.poll(0)
            except Exception as exc:
                log.error("Publish error frame %d: %s", frame_id, exc)

    finally:
        log.info("Worker %s shutting down.", wid)
        consumer.close()
        producer.flush()
        log.info("Worker %s done.", wid)


if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else "0"
    run_worker(wid)
