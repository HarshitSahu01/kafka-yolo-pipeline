#!/usr/bin/env python3
"""
result_collector.py — Collect annotated JPEG frames from yolo-results, reorder
                      shuffled frames, and write output/output_{id}.mp4.
                      Ctrl+C finalises the video cleanly.

    python result_collector.py
    python result_collector.py --fps 30 --width 1280 --height 720
"""

import argparse
import heapq
import logging
import os
import signal
import time

import cv2
import numpy as np
from confluent_kafka import Consumer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_RESULTS  = "yolo-results"
CONSUMER_GROUP = "yolo-results-group"

# Out-of-order buffer: hold at most this many frames before forcing a flush
REORDER_WINDOW = 60   # ~2 s at 30 fps — absorbs burst shuffles
# After this many seconds of silence we assume a gap and flush whatever we have
FLUSH_TIMEOUT  = 2.0

OUTPUT_DIR     = "output"
OUTPUT_FPS     = 25
OUTPUT_W       = 1280
OUTPUT_H       = 720

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs",      exist_ok=True)
os.makedirs(OUTPUT_DIR,  exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/collector.log"),
    ],
)
log = logging.getLogger("collector")

# ── Signal handling ───────────────────────────────────────────────────────────
_SHUTDOWN = False

def _stop(sig, _):
    global _SHUTDOWN
    _SHUTDOWN = True
    log.info("Ctrl+C received — finalising video…")

signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


# ── Video writer helpers ──────────────────────────────────────────────────────
def _make_writer(output_id: str, w: int, h: int, fps: float) -> cv2.VideoWriter:
    path   = os.path.join(OUTPUT_DIR, f"output_{output_id}.mp4")
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(path, fourcc, fps, (w, h))
    log.info("Writing video → %s  (%dx%d @ %.0f fps)", path, w, h, fps)
    return writer


def _decode(data: bytes, w: int, h: int) -> np.ndarray | None:
    arr   = np.frombuffer(data, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        return None
    if (frame.shape[1], frame.shape[0]) != (w, h):
        frame = cv2.resize(frame, (w, h), interpolation=cv2.INTER_LINEAR)
    return frame


def _flush_heap(heap: list, writer: cv2.VideoWriter, w: int, h: int,
                next_expected: int, force: bool = False) -> int:
    """Write all consecutive frames starting from *next_expected*.

    If *force* is True, also skip any gap (write whatever is at the heap top)
    so we never block forever on a missing frame.
    """
    while heap:
        fid, data = heap[0]
        if fid == next_expected:
            heapq.heappop(heap)
            frame = _decode(data, w, h)
            if frame is not None:
                writer.write(frame)
            next_expected += 1
        elif force and fid > next_expected:
            # Skip the missing frames — just advance the counter
            log.debug("Skipping missing frames %d‥%d", next_expected, fid - 1)
            next_expected = fid
        else:
            break   # waiting for a frame that hasn't arrived yet
    return next_expected


# ── Main ──────────────────────────────────────────────────────────────────────
def main(fps: int, width: int, height: int) -> None:
    output_id = str(int(time.time()))
    consumer  = Consumer({
        "bootstrap.servers":    KAFKA_BROKER,
        "group.id":             CONSUMER_GROUP,
        "auto.offset.reset":    "latest",
        "max.poll.interval.ms": 300_000,
        "fetch.max.bytes":      20_000_000,   # 20 MB — large annotated frames
        "max.partition.fetch.bytes": 20_000_000,
    })
    consumer.subscribe([TOPIC_RESULTS])

    writer         = _make_writer(output_id, width, height, fps)
    heap: list     = []           # min-heap of (frame_id, jpeg_bytes)
    seen: set      = set()        # dedup guard against duplicates
    next_expected  = None         # first frame_id we ever see
    last_msg_time  = time.monotonic()
    total_written  = 0

    log.info("Collector started — Ctrl+C to stop and finalise.")

    try:
        while not _SHUTDOWN:
            msg = consumer.poll(0.2)

            now = time.monotonic()

            # Timeout flush — force-drain the heap when the stream stalls
            if now - last_msg_time > FLUSH_TIMEOUT and heap:
                next_expected = _flush_heap(heap, writer, width, height,
                                            next_expected, force=True)

            if msg is None or msg.error():
                continue

            last_msg_time = now

            # Parse frame_id from headers
            frame_id = None
            for key, val in (msg.headers() or []):
                if key == "frame_id":
                    try:
                        frame_id = int(val)
                    except (ValueError, TypeError):
                        pass

            if frame_id is None or frame_id in seen:
                continue
            seen.add(frame_id)

            # Bootstrap expected counter on first message
            if next_expected is None:
                next_expected = frame_id

            heapq.heappush(heap, (frame_id, msg.value()))

            # Flush in-order frames whenever the buffer is large enough
            if len(heap) >= REORDER_WINDOW:
                before         = next_expected
                next_expected  = _flush_heap(heap, writer, width, height,
                                             next_expected, force=False)
                total_written += next_expected - before

            # Also greedily flush any consecutive run that's ready
            next_expected  = _flush_heap(heap, writer, width, height,
                                         next_expected, force=False)

    finally:
        # Force-flush every remaining buffered frame before closing
        if next_expected is not None:
            next_expected = _flush_heap(heap, writer, width, height,
                                        next_expected, force=True)
        writer.release()
        consumer.close()
        log.info("Video saved — %d frames written, %d frames still in buffer flushed.",
                 total_written, len(heap))
        log.info("Output: %s/output_%s.mp4", OUTPUT_DIR, output_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLO annotated frame collector → MP4.")
    parser.add_argument("--fps",    type=int, default=OUTPUT_FPS,  help="Output video FPS.")
    parser.add_argument("--width",  type=int, default=OUTPUT_W,    help="Frame width.")
    parser.add_argument("--height", type=int, default=OUTPUT_H,    help="Frame height.")
    args = parser.parse_args()
    main(fps=args.fps, width=args.width, height=args.height)
