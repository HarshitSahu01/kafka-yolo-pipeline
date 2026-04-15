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

# ── Resolve the correct Python interpreter ────────────────────────────────────
# Always use the venv Python if this script lives inside a venv-managed project.
# This ensures worker subprocesses inherit the same GPU-capable torch install.
_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)  # .venv lives one level up (kafka-yolo-pipeline/)
_VENV_PYTHON  = os.path.join(_PROJECT_ROOT, ".venv", "bin", "python")
PYTHON_EXE    = _VENV_PYTHON if os.path.isfile(_VENV_PYTHON) else sys.executable

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
    """Verify weights are present and CUDA is available before spawning workers."""
    import torch
    from ultralytics import YOLO

    log.info("Pre-fetching model '%s'…", YOLO_MODEL)
    try:
        YOLO(YOLO_MODEL)
        log.info("Model ready.")
    except Exception as exc:
        log.error("Failed to load model: %s", exc)
        sys.exit(1)

    if torch.cuda.is_available():
        log.info("GPU detected: %s  (%.1f GB VRAM)",
                 torch.cuda.get_device_name(0),
                 torch.cuda.get_device_properties(0).total_memory / 1024**3)
    else:
        log.warning("CUDA NOT available — workers will run on CPU. "
                    "Check PyTorch CUDA version vs driver version.")

    log.info("Using Python: %s", PYTHON_EXE)


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
        subprocess.Popen([PYTHON_EXE, "worker_logic.py", str(i)], env=env)
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
