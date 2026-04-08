#!/usr/bin/env python3
"""
ingestion.py — Push video frames into Kafka.

File mode (loops the video forever — default):
    python ingestion.py
    python ingestion.py --video path/to/other.mp4

RTSP mode:
    python ingestion.py --rtsp rtsp://127.0.0.1:8554/live
"""

import argparse
import logging
import os
import signal
import sys
import time

import cv2
from confluent_kafka import Producer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC          = "quickstart-events"
MAX_RESOLUTION = (1280, 720)
JPEG_QUALITY   = 80
TARGET_FPS     = 30

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingestion.log"),
    ],
)
log = logging.getLogger("ingestion")

# ── Signal handling ───────────────────────────────────────────────────────────
_SHUTDOWN = False

def _stop(sig, _):
    global _SHUTDOWN
    _SHUTDOWN = True
    log.info("Shutting down ingestion…")

signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":            KAFKA_BROKER,
        "linger.ms":                    5,
        "compression.type":             "snappy",
        "queue.buffering.max.messages": 100_000,
    })


def _publish(producer: Producer, frame, frame_id: int) -> None:
    h, w = frame.shape[:2]
    if w > MAX_RESOLUTION[0] or h > MAX_RESOLUTION[1]:
        frame = cv2.resize(frame, MAX_RESOLUTION, interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
    if not ok:
        return
    headers = [
        ("frame_id",  str(frame_id).encode()),
        ("timestamp", str(int(time.time() * 1000)).encode()),
    ]
    try:
        producer.produce(TOPIC, buf.tobytes(), headers=headers)
        producer.poll(0)
    except BufferError:
        log.warning("Producer queue full — dropping frame %d.", frame_id)
        producer.poll(0.1)


# ── Ingestion modes ───────────────────────────────────────────────────────────
def ingest_file(video_path: str) -> None:
    """Loop a local video file indefinitely at TARGET_FPS."""
    producer = _make_producer()
    frame_id = 0
    loop = 0
    log.info("Ingesting '%s' → Kafka (looping at %d fps).", video_path, TARGET_FPS)
    try:
        while not _SHUTDOWN:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                log.error("Cannot open: %s", video_path)
                break
            loop += 1
            log.info("Loop %d.", loop)
            while not _SHUTDOWN:
                ok, frame = cap.read()
                if not ok:
                    break
                _publish(producer, frame, frame_id)
                frame_id += 1
                time.sleep(1 / TARGET_FPS)
            cap.release()
    finally:
        producer.flush()
        log.info("Done — %d frames sent.", frame_id)


def ingest_rtsp(rtsp_url: str) -> None:
    """Attach to an RTSP stream and publish frames until stopped."""
    # Force OpenCV's FFmpeg backend to use TCP — MediaMTX serves over TCP,
    # and the default UDP transport causes cap.read() to block forever.
    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"

    producer = _make_producer()
    frame_id = 0
    log.info("Connecting to RTSP: %s", rtsp_url)
    try:
        while not _SHUTDOWN:
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                log.warning("Stream unavailable — retrying in 2 s.")
                time.sleep(2)
                continue
            log.info("Stream opened — reading frames.")
            while not _SHUTDOWN:
                ok, frame = cap.read()
                if not ok:
                    log.warning("Frame read failed — reconnecting.")
                    break
                _publish(producer, frame, frame_id)
                frame_id += 1
            cap.release()
    finally:
        producer.flush()
        log.info("Done — %d frames sent.", frame_id)


# ── Entry-point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka video ingestion.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--rtsp",  metavar="URL",  default=None,
                       help="RTSP stream URL.")
    group.add_argument("--video", metavar="PATH", default="race_car.mp4",
                       help="Local video file to loop (default: race_car.mp4).")
    args = parser.parse_args()

    if args.rtsp:
        ingest_rtsp(args.rtsp)
    else:
        if not os.path.isfile(args.video):
            log.error("Video file not found: %s", args.video)
            sys.exit(1)
        ingest_file(args.video)
