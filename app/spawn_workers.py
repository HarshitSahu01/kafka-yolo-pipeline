#!/usr/bin/env python3
"""
spawn_workers.py — Pre-download YOLO weights, then launch N worker subprocesses.

    python spawn_workers.py              # 4 workers (default)
    python spawn_workers.py --workers 8
"""

import argparse
import logging
import os
import signal
import subprocess
import sys

YOLO_MODEL = "yolov8n.pt"

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


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Spawn YOLO inference workers.")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="Number of parallel worker processes (default: 4).")
    args = parser.parse_args()

    prefetch_model()
    log.info("Spawning %d workers…", args.workers)

    env = {**os.environ, "PYTHONPATH": os.getcwd()}
    processes = [
        subprocess.Popen([sys.executable, "worker_logic.py", str(i)], env=env)
        for i in range(args.workers)
    ]

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


if __name__ == "__main__":
    main()
