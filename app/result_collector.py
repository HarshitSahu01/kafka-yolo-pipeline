#!/usr/bin/env python3
"""
result_collector.py — Collect annotated JPEG frames from yolo-results, reorder
                      shuffled frames, and write output/output_{id}.mp4 (H.264).
                      Ctrl+C finalises the video cleanly.

    python result_collector.py
    python result_collector.py --fps 30 --width 1280 --height 720
"""

import argparse
import heapq
import logging
import os
import shutil
import signal
import subprocess
import time

import cv2
import numpy as np
from confluent_kafka import Consumer, TopicPartition

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_RESULTS  = "yolo-results"
CONSUMER_GROUP = "yolo-results-group"

# Out-of-order buffer: hold at most this many frames before forcing a flush
REORDER_WINDOW = 30    # ~1 s at 30 fps — absorbs burst shuffles
# After this many seconds of silence we assume a gap and flush whatever we have
FLUSH_TIMEOUT  = 2.0

OUTPUT_DIR = "output"
OUTPUT_FPS = 25
OUTPUT_W   = 854
OUTPUT_H   = 480

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs",     exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
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


# ── FFmpeg pipe writer ────────────────────────────────────────────────────────
def _make_ffmpeg_writer(path: str, w: int, h: int, fps: float) -> subprocess.Popen:
    """
    Open an ffmpeg subprocess that accepts raw BGR24 frames on stdin and
    encodes them to H.264 (libx264) MP4.  Always uses the software encoder —
    no V4L2 / hardware device required.
    """
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg not found — install it with: sudo apt-get install ffmpeg")

    cmd = [
        "ffmpeg", "-y",
        # Input: raw video piped on stdin
        "-f",       "rawvideo",
        "-vcodec",  "rawvideo",
        "-s",       f"{w}x{h}",
        "-pix_fmt", "bgr24",
        "-r",       str(fps),
        "-i",       "pipe:0",
        # Output: H.264 in MP4 container
        "-vcodec",  "libx264",
        "-preset",  "fast",
        "-crf",     "23",
        "-pix_fmt", "yuv420p",   # required for broad player compatibility
        "-movflags", "+faststart",  # place moov atom at front for streaming
        path,
    ]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,   # capture warnings (printed on close)
        preexec_fn=os.setpgrp     # protect ffmpeg from Ctrl+C signal
    )
    log.info("Writing video → %s  (%dx%d @ %.0f fps, codec=libx264/H.264)", path, w, h, fps)
    return proc


def _close_ffmpeg_writer(proc: subprocess.Popen) -> None:
    """Flush stdin, wait for ffmpeg to finish, log any warnings."""
    try:
        _, stderr = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        log.warning("ffmpeg processing timed out, killing.")
        proc.kill()
        _, stderr = proc.communicate()

    if proc.returncode != 0 and stderr:
        for line in stderr.decode(errors="replace").splitlines():
            if line.strip():
                log.warning("ffmpeg: %s", line)


def _write_frame(proc: subprocess.Popen, frame: np.ndarray) -> bool:
    """Write one BGR frame to ffmpeg stdin. Returns False if the pipe broke."""
    try:
        proc.stdin.write(frame.tobytes())
        return True
    except (BrokenPipeError, OSError):
        return False


# ── Decode helper ─────────────────────────────────────────────────────────────
def _decode(data: bytes, w: int, h: int) -> np.ndarray | None:
    arr   = np.frombuffer(data, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        return None
    if (frame.shape[1], frame.shape[0]) != (w, h):
        frame = cv2.resize(frame, (w, h), interpolation=cv2.INTER_LINEAR)
    return frame


# ── Reorder-buffer flush ──────────────────────────────────────────────────────
def _flush_heap(heap: list, proc: subprocess.Popen, w: int, h: int,
                next_expected: int, force: bool = False) -> tuple[int, int]:
    """Write all consecutive frames starting from *next_expected*.

    If *force* is True, also skip any gap so we never block forever on a
    missing frame.
    Returns (next_expected, num_written)
    """
    written = 0
    while heap:
        fid, _seq, data = heap[0]
        if fid < next_expected:
            heapq.heappop(heap)
            continue
        elif fid == next_expected:
            heapq.heappop(heap)
            frame = _decode(data, w, h)
            if frame is not None:
                if _write_frame(proc, frame):
                    written += 1
            next_expected += 1
        elif force and fid > next_expected:
            log.debug("Skipping missing frames %d‥%d", next_expected, fid - 1)
            next_expected = fid
        else:
            break   # waiting for a frame that hasn't arrived yet
    return next_expected, written


# ── Main ──────────────────────────────────────────────────────────────────────
def main(fps: float, width: int, height: int) -> None:
    # Use a unique group-id per run so we always start from offset 0 of the
    # results topic — no stale committed offsets from previous runs interfere.
    output_id = str(int(time.time()))
    group_id  = f"{CONSUMER_GROUP}-{output_id}"

    consumer = Consumer({
        "bootstrap.servers":                  KAFKA_BROKER,
        "group.id":                           group_id,
        "auto.offset.reset":                  "earliest",  # fresh group → read all available
        "max.poll.interval.ms":               300_000,
        "fetch.max.bytes":                    20_000_000,  # 20 MB — large annotated frames
        "max.partition.fetch.bytes":          20_000_000,
        "topic.metadata.refresh.interval.ms": 2000,
    })
    consumer.subscribe([TOPIC_RESULTS])

    output_path = os.path.join(OUTPUT_DIR, f"output_{output_id}.mp4")
    proc        = None
    actual_fps  = fps

    heap: list    = []   # min-heap of (frame_id, seq, jpeg_bytes)
    _heap_seq     = 0    # monotonic tie-breaker — avoids comparing raw bytes
    seen: set     = set()
    next_expected = None
    last_msg_time = time.monotonic()
    total_written = 0

    log.info("Collector started (group=%s) — Ctrl+C to stop and finalise.", group_id)

    try:
        while not _SHUTDOWN:
            msg = consumer.poll(0.2)
            now = time.monotonic()

            # Timeout flush — force-drain the heap when the stream stalls
            if now - last_msg_time > FLUSH_TIMEOUT and heap:
                if proc is not None:
                    next_expected, wr  = _flush_heap(heap, proc, width, height,
                                                     next_expected, force=True)
                    total_written += wr

            if msg is None or msg.error():
                continue

            last_msg_time = now

            # Parse frame_id from headers
            frame_id = None
            msg_fps  = None
            for key, val in (msg.headers() or []):
                if key == "frame_id":
                    try:
                        frame_id = int(val)
                    except (ValueError, TypeError):
                        pass
                elif key == "fps":
                    try:
                        msg_fps = float(val)
                    except (ValueError, TypeError):
                        pass

            if frame_id is None or frame_id in seen:
                continue
            seen.add(frame_id)

            # Lazy init proc with exact stream fps
            if proc is None:
                if msg_fps is not None and msg_fps > 0:
                    actual_fps = msg_fps
                proc = _make_ffmpeg_writer(output_path, width, height, actual_fps)

            # Bootstrap expected counter on first message
            if next_expected is None:
                next_expected = frame_id

            heapq.heappush(heap, (frame_id, _heap_seq, msg.value()))
            _heap_seq += 1

            # Flush frames with force=True if we exceed reorder window bounds
            if len(heap) >= REORDER_WINDOW:
                next_expected, wr  = _flush_heap(heap, proc, width, height,
                                                 next_expected, force=True)
                total_written += wr

            # Greedily flush any consecutive run that's ready right now
            next_expected, wr  = _flush_heap(heap, proc, width, height,
                                         next_expected, force=False)
            total_written += wr

    finally:
        # Force-flush every remaining buffered frame before closing
        if next_expected is not None:
            if proc is not None:
                next_expected, wr  = _flush_heap(heap, proc, width, height,
                                             next_expected, force=True)
                total_written += wr

        log.info("Closing video encoder…")
        if proc is not None:
            _close_ffmpeg_writer(proc)
        consumer.close()
        duration_sec = total_written / actual_fps if actual_fps > 0 else 0
        log.info("Video saved — %d frames received, %d frames written (%.2fs) to %s", 
                 len(seen), total_written, duration_sec, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLO annotated frame collector → MP4.")
    parser.add_argument("--fps",    type=float, default=OUTPUT_FPS,  help="Output video FPS.")
    parser.add_argument("--width",  type=int, default=OUTPUT_W,    help="Frame width.")
    parser.add_argument("--height", type=int, default=OUTPUT_H,    help="Frame height.")
    args = parser.parse_args()
    main(fps=args.fps, width=args.width, height=args.height)
