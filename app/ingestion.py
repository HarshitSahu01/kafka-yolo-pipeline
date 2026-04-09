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
from confluent_kafka.admin import AdminClient, NewTopic
import shutil
import subprocess

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC          = "quickstart-events"
MAX_RESOLUTION = (854, 480)
JPEG_QUALITY   = 80
TARGET_FPS     = 60

TOPIC_SPECS = {
    "quickstart-events": {"partitions": 3, "replication_factor": 1},
    "yolo-results":      {"partitions": 1, "replication_factor": 1},
}

# Consumer groups that should have offsets reset when topics are cleared
CONSUMER_GROUPS = [
    "yolo-cluster-group",   # workers consuming `quickstart-events`
    "yolo-results-group",   # collectors (base name; collectors append an id)
]

# Mute FFmpeg H264 decoding spam from OpenCV during RTSP stream dropouts
os.environ["OPENCV_FFMPEG_LOGLEVEL"] = "-8"

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
def _clear_topic(broker: str, topic: str, partitions: int, replication_factor: int = 1):
    log.info("Clearing Kafka topic '%s'...", topic)
    try:
        admin = AdminClient({'bootstrap.servers': broker})
        fs = admin.delete_topics([topic])
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                log.debug("Delete skipped for '%s' (topic may not exist yet).", t)
        log.info("Topic '%s' deleted. Waiting for Kafka to clear it...", topic)
        
        # Explicitly recreate the topic
        for _ in range(30):
            time.sleep(1)
            try:
                fs = admin.create_topics([
                    NewTopic(
                        topic,
                        num_partitions=partitions,
                        replication_factor=replication_factor,
                    )
                ])
                for t, f in fs.items():
                    f.result()
                log.info(
                    "Topic '%s' successfully recreated with %d partition(s).",
                    topic,
                    partitions,
                )
                # Wait for broker metadata to reflect the new topic/partitions.
                # Some clients may observe partition count 0 immediately after create;
                # poll the cluster metadata until the partition count matches expectation.
                for _m in range(10):
                    md = admin.list_topics(timeout=5)
                    tmd = md.topics.get(topic)
                    if tmd is not None and len(tmd.partitions) >= partitions:
                        break
                    time.sleep(1)
                else:
                    log.warning("Topic '%s' metadata did not settle to %d partitions yet.", topic, partitions)

                # Reset consumer group offsets so consumers don't request stale offsets
                try:
                    _reset_consumer_offsets(broker, topic, CONSUMER_GROUPS, to="earliest")
                except Exception:
                    log.debug("Offset reset helper failed for topic '%s' (non-fatal).", topic)
                break
            except Exception as exc:
                log.debug("Retrying topic creation for '%s': %s", topic, exc)
        else:
            log.error("Failed to recreate topic '%s' after retries.", topic)
    except Exception as e:
        log.error("Error clearing topic '%s': %s", topic, e)


def _reset_consumer_offsets(broker: str, topic: str, groups: list[str], to: str = "earliest") -> None:
    """Reset offsets for the given consumer groups on `topic` using the kafka-consumer-groups CLI.

    This helper is best-effort: if the CLI isn't available the operation is skipped.
    """
    exe = shutil.which("kafka-consumer-groups") or shutil.which("kafka-consumer-groups.sh")
    if not exe:
        log.warning("kafka-consumer-groups not on PATH — skipping offset reset for '%s'.", topic)
        return

    opt = "--to-earliest" if to == "earliest" else "--to-latest"
    for group in groups:
        cmd = [
            exe,
            "--bootstrap-server", broker,
            "--group", group,
            "--topic", topic,
            "--reset-offsets",
            opt,
            "--execute",
        ]
        try:
            proc = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            log.info("Reset offsets for group '%s' on topic '%s' -> %s", group, topic, to)
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode(errors="replace") if exc.stderr else str(exc)
            log.warning("Offset reset failed for group '%s' on topic '%s': %s", group, topic, stderr)

def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":            KAFKA_BROKER,
        "linger.ms":                    5,
        "compression.type":             "snappy",
        "queue.buffering.max.messages": 100_000,
    })


def _publish(producer: Producer, frame, frame_id: int, fps: float) -> None:
    h, w = frame.shape[:2]
    if w > MAX_RESOLUTION[0] or h > MAX_RESOLUTION[1]:
        frame = cv2.resize(frame, MAX_RESOLUTION, interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
    if not ok:
        return
    headers = [
        ("frame_id",  str(frame_id).encode()),
        ("timestamp", str(int(time.time() * 1000)).encode()),
        ("fps",       str(fps).encode()),
    ]
    while not _SHUTDOWN:
        try:
            producer.produce(TOPIC, buf.tobytes(), headers=headers)
            producer.poll(0)
            break
        except BufferError:
            log.warning("Producer queue full — waiting for space at frame %d...", frame_id)
            producer.poll(0.1)


# ── Ingestion modes ───────────────────────────────────────────────────────────
def ingest_file(video_path: str) -> None:
    """Loop a local video file indefinitely at TARGET_FPS."""
    producer = _make_producer()
    frame_id = 0
    loop = 0
    log.info("Ingesting '%s' → Kafka.", video_path)
    try:
        while not _SHUTDOWN:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                log.error("Cannot open: %s", video_path)
                break
            fps = cap.get(cv2.CAP_PROP_FPS)
            if not fps or fps != fps or fps <= 0:
                fps = 30.0
            loop += 1
            log.info("Loop %d (%.2f fps).", loop, fps)
            while not _SHUTDOWN:
                ok, frame = cap.read()
                if not ok:
                    break
                _publish(producer, frame, frame_id, fps)
                frame_id += 1
                time.sleep(1 / fps)
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
            fps = cap.get(cv2.CAP_PROP_FPS)
            if not fps or fps != fps or fps <= 0:
                fps = 30.0
            log.info("Stream opened (%.2f fps) — reading frames.", fps)
            while not _SHUTDOWN:
                ok, frame = cap.read()
                if not ok:
                    log.warning("Frame read failed — reconnecting.")
                    break
                _publish(producer, frame, frame_id, fps)
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

    _clear_topic(
        KAFKA_BROKER,
        TOPIC,
        partitions=TOPIC_SPECS[TOPIC]["partitions"],
        replication_factor=TOPIC_SPECS[TOPIC]["replication_factor"],
    )
    _clear_topic(
        KAFKA_BROKER,
        "yolo-results",
        partitions=TOPIC_SPECS["yolo-results"]["partitions"],
        replication_factor=TOPIC_SPECS["yolo-results"]["replication_factor"],
    )

    if args.rtsp:
        ingest_rtsp(args.rtsp)
    else:
        if not os.path.isfile(args.video):
            log.error("Video file not found: %s", args.video)
            sys.exit(1)
        ingest_file(args.video)
