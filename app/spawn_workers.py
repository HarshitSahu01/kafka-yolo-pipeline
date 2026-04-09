#!/usr/bin/env python3
"""
spawn_workers.py — Pre-download YOLO weights, then launch N worker subprocesses.

    python spawn_workers.py              # 4 workers (default)
    python spawn_workers.py --workers 8
"""

import argparse
import logging
from multiprocessing import shared_memory
import os
import signal
import struct
import subprocess
import sys
import threading
import time

import warnings
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="multiprocessing.resource_tracker"
)

YOLO_MODEL = "yolov8n.pt"
INTERVAL = 2.0
_SLOT_SIZE = 16  # 8 bytes sequence + 8 bytes frame counter

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/spawn_workers.log"),
    ],
)
log = logging.getLogger("spawn_workers")


# ── Helpers ───────────────────────────────────────────────────────────────────
def prefetch_model() -> None:
    """Download weights in the parent process before any subprocess reads them."""
    from ultralytics import YOLO
    log.info("Pre-fetching model '%s'…", YOLO_MODEL)
    try:
        YOLO(YOLO_MODEL)
        log.info("Model ready.")
    except Exception as exc:
        log.error("Failed to load model: %s", exc)
        sys.exit(1)


def _read_counter(shm_buf, worker_idx: int) -> int:
    """Read a per-worker counter using a seqlock pattern."""
    base = worker_idx * _SLOT_SIZE
    while True:
        seq1 = struct.unpack_from("Q", shm_buf, base)[0]
        count = struct.unpack_from("Q", shm_buf, base + 8)[0]
        seq2 = struct.unpack_from("Q", shm_buf, base)[0]
        if seq1 == seq2 and (seq1 % 2 == 0):
            return count


def _monitor_fps(processes, workers: int, interval: float, shm_buf, stop_event: threading.Event) -> None:
    """Log effective FPS across all workers at fixed intervals."""
    last_total = sum(_read_counter(shm_buf, i) for i in range(workers))
    last_ts = time.monotonic()

    while not stop_event.wait(interval):
        if all(p.poll() is not None for p in processes):
            break

        now = time.monotonic()
        current_total = sum(_read_counter(shm_buf, i) for i in range(workers))
        dt = now - last_ts
        if dt > 0:
            fps = (current_total - last_total) / dt
            log.info("Effective FPS (all workers): %.2f", fps)
        last_total = current_total
        last_ts = now


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Spawn YOLO inference workers.")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="Number of parallel worker processes (default: 4).")
    parser.add_argument("--interval", type=float, default=INTERVAL,
                        help="Seconds between combined FPS logs (0 disables, default: 2).")
    args = parser.parse_args()

    prefetch_model()
    log.info("Spawning %d workers…", args.workers)

    fps_shm = shared_memory.SharedMemory(create=True, size=args.workers * _SLOT_SIZE)
    fps_shm.buf[:] = b"\x00" * (args.workers * _SLOT_SIZE)

    env = {
        **os.environ,
        "PYTHONPATH": os.getcwd(),
        "FPS_SHM_NAME": fps_shm.name,
        "FPS_SHM_SLOT_SIZE": str(_SLOT_SIZE),
    }
    processes = [
        subprocess.Popen([sys.executable, "worker_logic.py", str(i)], env=env)
        for i in range(args.workers)
    ]

    stop_event = threading.Event()
    monitor_thread = None
    if args.interval > 0:
        monitor_thread = threading.Thread(
            target=_monitor_fps,
            args=(processes, args.workers, args.interval, fps_shm.buf, stop_event),
            daemon=True,
        )
        monitor_thread.start()

    def _relay(sig, _):
        log.info("Signal %s — terminating workers.", sig)
        for p in processes:
            p.terminate()

    signal.signal(signal.SIGTERM, _relay)

    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        log.info("Stopping all workers…")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        log.info("All workers stopped.")
    finally:
        stop_event.set()
        if monitor_thread is not None:
            monitor_thread.join(timeout=1.0)
        fps_shm.close()
        fps_shm.unlink()


if __name__ == "__main__":
    main()
