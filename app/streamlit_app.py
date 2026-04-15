#!/usr/bin/env python3
"""
streamlit_app.py — Premium pipeline control dashboard.

Run from anywhere in the project:
    uv run streamlit run app/streamlit_app.py
    .venv/bin/streamlit run app/streamlit_app.py
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil
import streamlit as st

# ── Absolute paths (relative to this file) ────────────────────────────────────
_APP_DIR      = Path(__file__).resolve().parent          # kafka-yolo-pipeline/app/
PROJECT_ROOT  = _APP_DIR.parent                          # kafka-yolo-pipeline/
OUTPUT_DIR    = PROJECT_ROOT / "output"                  # canonical output dir
LOGS_DIR      = _APP_DIR / "logs"
STATE_FILE    = PROJECT_ROOT / ".pipeline_state.json"

VENV_PYTHON   = PROJECT_ROOT / ".venv" / "bin" / "python"
PYTHON        = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="YOLO Kafka Pipeline",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Premium CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #0d1528 50%, #0a0e1a 100%);
}
#MainMenu, footer, header { visibility: hidden; }

.hero {
    background: linear-gradient(120deg, rgba(99,102,241,0.15) 0%, rgba(16,185,129,0.10) 100%);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 20px;
    padding: 2rem 2.5rem;
    margin-bottom: 1.5rem;
    backdrop-filter: blur(16px);
}
.hero h1 {
    font-size: 2rem; font-weight: 700;
    background: linear-gradient(90deg, #a5b4fc, #34d399);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin: 0 0 0.3rem 0;
}
.hero p { color: #94a3b8; font-size: 0.9rem; margin: 0; }

.section-title {
    color: #64748b; font-size: 0.7rem; font-weight: 600;
    letter-spacing: 0.13em; text-transform: uppercase; margin-bottom: 0.75rem;
}

.badge { display: inline-block; padding: 0.22rem 0.7rem; border-radius: 999px;
         font-size: 0.75rem; font-weight: 600; }
.badge-running { background: rgba(16,185,129,0.15); color: #34d399;
                 border: 1px solid rgba(16,185,129,0.35); }
.badge-stopped { background: rgba(100,116,139,0.12); color: #475569;
                 border: 1px solid rgba(100,116,139,0.25); }

.metric-box { text-align: center; padding: 0.75rem 0; }
.metric-value {
    font-size: 2.2rem; font-weight: 700;
    background: linear-gradient(90deg, #a5b4fc, #34d399);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    line-height: 1;
}
.metric-label {
    color: #475569; font-size: 0.7rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.1em; margin-top: 0.3rem;
}

.worker-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 0.45rem 0.1rem;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    font-size: 0.84rem;
}
.fps-bar-wrap {
    flex: 1; margin: 0 0.8rem; height: 5px;
    background: rgba(255,255,255,0.07); border-radius: 999px; overflow: hidden;
}
.fps-bar {
    height: 100%; border-radius: 999px;
    background: linear-gradient(90deg, #6366f1, #34d399);
    transition: width 0.6s ease;
}

/* Output table */
.out-header {
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.5rem 0.8rem;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px 10px 0 0;
    font-size: 0.7rem; font-weight: 600; color: #475569;
    text-transform: uppercase; letter-spacing: 0.1em;
}
.out-row {
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.75rem 0.8rem;
    border-left: 1px solid rgba(255,255,255,0.06);
    border-right: 1px solid rgba(255,255,255,0.06);
    border-bottom: 1px solid rgba(255,255,255,0.05);
    transition: background 0.2s;
}
.out-row:hover { background: rgba(99,102,241,0.07); }
.out-row:last-child { border-radius: 0 0 10px 10px; border-bottom: 1px solid rgba(255,255,255,0.06); }
.out-name  { color: #e2e8f0; font-size: 0.88rem; font-weight: 600; flex: 2.5; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.out-date  { color: #64748b; font-size: 0.78rem; flex: 2; }
.out-size  { color: #64748b; font-size: 0.78rem; flex: 0.8; text-align: right; }
.badge-new {
    display: inline-block; background: rgba(99,102,241,0.2);
    color: #a5b4fc; border: 1px solid rgba(99,102,241,0.4);
    border-radius: 999px; padding: 0.05rem 0.5rem;
    font-size: 0.65rem; font-weight: 700; margin-left: 0.4rem;
    vertical-align: middle;
}

/* Streamlit overrides */
div[data-testid="stTextInput"] input {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    color: #e2e8f0 !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: rgba(99,102,241,0.6) !important;
    box-shadow: 0 0 0 2px rgba(99,102,241,0.12) !important;
}
div[data-testid="stButton"] button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    transition: all 0.2s !important;
}
div[data-testid="stDownloadButton"] button {
    border-radius: 8px !important; font-size: 0.78rem !important;
    padding: 0.2rem 0.75rem !important;
    background: transparent !important;
    border: 1px solid rgba(99,102,241,0.35) !important;
    color: #a5b4fc !important;
}
div[data-testid="stDownloadButton"] button:hover {
    background: rgba(99,102,241,0.15) !important;
    border-color: rgba(99,102,241,0.6) !important;
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# State helpers
# ══════════════════════════════════════════════════════════════════════════════

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"pids": {}, "rtsp_url": "", "n_workers": 4, "start_time": None}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _is_alive(pid) -> bool:
    if not pid:
        return False
    try:
        return psutil.pid_exists(int(pid)) and \
               psutil.Process(int(pid)).status() != psutil.STATUS_ZOMBIE
    except Exception:
        return False


def _kill_pid(pid) -> None:
    if not pid:
        return
    try:
        proc = psutil.Process(int(pid))
        for c in proc.children(recursive=True):
            try:
                c.terminate()
            except Exception:
                pass
        proc.terminate()
        try:
            proc.wait(timeout=4)
        except psutil.TimeoutExpired:
            proc.kill()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Pipeline control
# ══════════════════════════════════════════════════════════════════════════════

def start_pipeline(rtsp_url: str, n_workers: int) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONPATH": str(_APP_DIR)}

    # Ingestion
    if rtsp_url.strip():
        ingest_cmd = [PYTHON, str(_APP_DIR / "ingestion.py"), "--rtsp", rtsp_url.strip()]
    else:
        default_video = PROJECT_ROOT / "streaming" / "race_car.mp4"
        ingest_cmd = [PYTHON, str(_APP_DIR / "ingestion.py"), "--video", str(default_video)]

    p_ingest = subprocess.Popen(
        ingest_cmd, cwd=str(_APP_DIR), env=env,
        stdout=open(LOGS_DIR / "ingestion.log", "a"), stderr=subprocess.STDOUT,
    )

    # Workers — pipe stdout to log via thread
    p_workers = subprocess.Popen(
        [PYTHON, str(_APP_DIR / "spawn_workers.py"), "--workers", str(n_workers)],
        cwd=str(_APP_DIR), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    def _tee_workers():
        with open(LOGS_DIR / "spawn_workers.log", "a") as f:
            for line in p_workers.stdout:
                f.write(line)
                f.flush()
    threading.Thread(target=_tee_workers, daemon=True).start()

    # Collector — runs with __file__-based absolute paths now
    p_collector = subprocess.Popen(
        [PYTHON, str(_APP_DIR / "result_collector.py")],
        cwd=str(_APP_DIR), env=env,
        stdout=open(LOGS_DIR / "collector.log", "a"), stderr=subprocess.STDOUT,
    )

    state = _load_state()
    state.update({
        "pids": {
            "ingestion": p_ingest.pid,
            "workers":   p_workers.pid,
            "collector": p_collector.pid,
        },
        "rtsp_url":   rtsp_url,
        "n_workers":  n_workers,
        "start_time": time.time(),
    })
    _save_state(state)


def stop_pipeline() -> None:
    state = _load_state()
    for pid in state.get("pids", {}).values():
        _kill_pid(pid)
    # Sweep any orphaned processes not tracked in state
    keywords = ["ingestion.py", "spawn_workers.py", "worker_logic.py", "result_collector.py"]
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmd = " ".join(proc.cmdline())
            if any(k in cmd for k in keywords):
                proc.terminate()
        except Exception:
            pass
    state["pids"] = {}
    state["start_time"] = None
    _save_state(state)


# ══════════════════════════════════════════════════════════════════════════════
# Metrics helpers
# ══════════════════════════════════════════════════════════════════════════════

def _worker_fps() -> dict[int, float]:
    fps: dict[int, float] = {}
    for f in sorted(LOGS_DIR.glob("worker_*.log")):
        m = re.search(r"worker_(\d+)\.log$", f.name)
        if not m:
            continue
        try:
            matches = re.findall(r"FPS:\s*([\d.]+)", f.read_text())
            if matches:
                fps[int(m.group(1))] = float(matches[-1])
        except Exception:
            pass
    return fps


def _gpu() -> dict:
    try:
        raw = subprocess.check_output(
            ["nvidia-smi",
             "--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
             "--format=csv,noheader,nounits"],
            timeout=2, text=True,
        ).strip().split(",")
        return {
            "util": int(raw[0].strip()),
            "mem_used": int(raw[1].strip()),
            "mem_total": int(raw[2].strip()),
            "temp": int(raw[3].strip()),
            "power": float(raw[4].strip()),
        }
    except Exception:
        return {}


def _fmt_size(b: int) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} GB"


def _fmt_elapsed(t: float | None) -> str:
    if not t:
        return ""
    s = int(time.time() - t)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s" if m else f"{s}s"


def _list_outputs() -> list[dict]:
    """Scan OUTPUT_DIR for mp4 files, newest first."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = []
    for p in OUTPUT_DIR.glob("*.mp4"):
        try:
            st_info = p.stat()
            files.append({
                "path":  p,
                "name":  p.name,
                "size":  st_info.st_size,
                "mtime": st_info.st_mtime,
                "dt":    datetime.fromtimestamp(st_info.st_mtime),
            })
        except Exception:
            pass
    # Sort newest first
    files.sort(key=lambda f: f["mtime"], reverse=True)
    return files


# ══════════════════════════════════════════════════════════════════════════════
# Load state
# ══════════════════════════════════════════════════════════════════════════════

state  = _load_state()
pids   = state.get("pids", {})
status = {k: _is_alive(v) for k, v in pids.items()}
any_running = any(status.values())

# ══════════════════════════════════════════════════════════════════════════════
# Hero
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="hero">
  <h1>🎯 YOLO Kafka Pipeline</h1>
  <p>Real-time distributed YOLOv8 inference &nbsp;·&nbsp; RTX 3050 A Laptop GPU &nbsp;·&nbsp; batch=16</p>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Control  +  Live Metrics
# ══════════════════════════════════════════════════════════════════════════════

left, right = st.columns([1.1, 1], gap="large")

# ── Control panel ─────────────────────────────────────────────────────────────
with left:
    st.markdown('<div class="section-title">Pipeline Control</div>', unsafe_allow_html=True)

    rtsp_url = st.text_input(
        "RTSP Stream URL",
        value=state.get("rtsp_url", ""),
        placeholder="rtsp://localhost:8554/live  (blank → loop race_car.mp4)",
    )
    n_workers = st.slider(
        "Worker count", 1, 8, value=int(state.get("n_workers", 4)),
        help="Each worker runs a YOLO session on the GPU.",
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("▶  Start Pipeline", use_container_width=True,
                     disabled=any_running, type="primary"):
            start_pipeline(rtsp_url, n_workers)
            st.rerun()
    with c2:
        if st.button("⏹  Stop Pipeline", use_container_width=True,
                     disabled=not any_running):
            stop_pipeline()
            st.rerun()

    # Component status rows
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Component Status</div>', unsafe_allow_html=True)

    components = {
        "ingestion": "📡 Ingestion",
        "workers":   "⚙️  Workers",
        "collector": "📼 Collector",
    }
    for key, label in components.items():
        alive = status.get(key, False)
        badge = ('<span class="badge badge-running">● RUNNING</span>'
                 if alive else '<span class="badge badge-stopped">○ STOPPED</span>')
        elapsed_html = ""
        if alive and state.get("start_time"):
            elapsed_html = (f'<span style="color:#475569;font-size:0.73rem;margin-left:0.5rem;">'
                            f'{_fmt_elapsed(state["start_time"])}</span>')
        st.markdown(
            f'<div style="display:flex;align-items:center;justify-content:space-between;'
            f'padding:0.5rem 0;border-bottom:1px solid rgba(255,255,255,0.05);">'
            f'<span style="color:#cbd5e1;font-size:0.86rem;">{label}</span>'
            f'<span>{badge}{elapsed_html}</span></div>',
            unsafe_allow_html=True,
        )

# ── Live Metrics ───────────────────────────────────────────────────────────────
with right:
    st.markdown('<div class="section-title">Live Metrics</div>', unsafe_allow_html=True)

    fps_map = _worker_fps()
    agg_fps = sum(fps_map.values())
    gpu     = _gpu()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            f'<div class="metric-box"><div class="metric-value">{agg_fps:.0f}</div>'
            f'<div class="metric-label">Agg. FPS</div></div>', unsafe_allow_html=True)
    with c2:
        u = gpu.get("util", "—")
        st.markdown(
            f'<div class="metric-box"><div class="metric-value">{u}{"%" if isinstance(u,int) else ""}</div>'
            f'<div class="metric-label">GPU Util</div></div>', unsafe_allow_html=True)
    with c3:
        mu = gpu.get("mem_used", "—")
        ms = f"{mu}M" if isinstance(mu, int) else "—"
        st.markdown(
            f'<div class="metric-box"><div class="metric-value">{ms}</div>'
            f'<div class="metric-label">VRAM Used</div></div>', unsafe_allow_html=True)

    if gpu:
        st.markdown(
            f'<div style="display:flex;gap:1.5rem;justify-content:center;'
            f'color:#475569;font-size:0.78rem;margin:0.2rem 0 0.9rem;">'
            f'<span>🌡️ {gpu.get("temp","—")}°C</span>'
            f'<span>⚡ {gpu.get("power",0):.0f} W</span>'
            f'<span>📦 batch=16</span></div>',
            unsafe_allow_html=True)

    # Per-worker FPS bars
    st.markdown('<div class="section-title" style="margin-top:0.3rem;">Per-Worker FPS</div>',
                unsafe_allow_html=True)

    if fps_map:
        max_fps = max(fps_map.values(), default=1) or 1
        for wid in sorted(fps_map):
            fps = fps_map[wid]
            pct = min(100, fps / max(max_fps, 30) * 100)
            st.markdown(
                f'<div class="worker-row">'
                f'<span style="color:#94a3b8;min-width:56px;">W{wid}</span>'
                f'<div class="fps-bar-wrap"><div class="fps-bar" style="width:{pct:.0f}%"></div></div>'
                f'<span style="color:#e2e8f0;min-width:52px;text-align:right;">{fps:.1f} fps</span>'
                f'</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            '<p style="color:#475569;font-size:0.82rem;text-align:center;padding:1.2rem 0;">'
            'Start the pipeline to see live FPS.</p>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Output recordings — newest first
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="section-title">Output Recordings</div>', unsafe_allow_html=True)

outputs = _list_outputs()
now     = time.time()

if not outputs:
    st.markdown(
        '<div style="background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.07);'
        'border-radius:12px;padding:2.5rem;text-align:center;color:#475569;">'
        '📂 No recordings yet. Run the pipeline to generate annotated MP4 files.'
        '</div>', unsafe_allow_html=True)
else:
    st.markdown(
        f'<p style="color:#475569;font-size:0.78rem;margin-bottom:0.5rem;">'
        f'{len(outputs)} recording{"s" if len(outputs)!=1 else ""} &nbsp;·&nbsp; '
        f'output → <code style="color:#64748b">{OUTPUT_DIR}</code></p>',
        unsafe_allow_html=True)

    # Table header
    st.markdown(
        '<div class="out-header">'
        '<span style="flex:0.3">#</span>'
        '<span style="flex:2.5">Filename</span>'
        '<span style="flex:2">Created</span>'
        '<span style="flex:0.8;text-align:right">Size</span>'
        '<span style="flex:0.8;text-align:right">Download</span>'
        '</div>', unsafe_allow_html=True)

    for i, f in enumerate(outputs):
        is_new   = (now - f["mtime"]) < 300      # new badge if < 5 min
        new_badge = '<span class="badge-new">NEW</span>' if is_new else ""
        size_str = _fmt_size(f["size"])

        secs_ago = int(now - f["mtime"])
        if secs_ago < 60:
            ago = f"{secs_ago}s ago"
        elif secs_ago < 3600:
            ago = f"{secs_ago//60}m ago"
        elif secs_ago < 86400:
            ago = f"{secs_ago//3600}h ago"
        else:
            ago = f["dt"].strftime("%b %d")

        dt_str = f["dt"].strftime("%b %d, %Y  %H:%M:%S")

        # Row: use columns so the download button renders as a real Streamlit widget
        row_cols = st.columns([0.3, 2.5, 2, 0.8, 0.9])
        with row_cols[0]:
            st.markdown(f'<div style="color:#475569;font-size:0.82rem;padding-top:0.55rem;">{i+1}</div>',
                        unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(
                f'<div style="padding-top:0.45rem;">'
                f'<span class="out-name" style="color:#e2e8f0;font-size:0.87rem;font-weight:600;">'
                f'{f["name"]}</span>{new_badge}'
                f'<div style="color:#475569;font-size:0.73rem;margin-top:0.1rem;">{ago}</div>'
                f'</div>', unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(
                f'<div style="color:#64748b;font-size:0.8rem;padding-top:0.5rem;">{dt_str}</div>',
                unsafe_allow_html=True)
        with row_cols[3]:
            st.markdown(
                f'<div style="color:#64748b;font-size:0.8rem;padding-top:0.5rem;text-align:right;">{size_str}</div>',
                unsafe_allow_html=True)
        with row_cols[4]:
            try:
                with open(f["path"], "rb") as fh:
                    data = fh.read()
                st.download_button(
                    "⬇ Download", data=data,
                    file_name=f["name"], mime="video/mp4",
                    key=f"dl_{i}_{f['name']}",
                    use_container_width=True,
                )
            except Exception:
                st.markdown('<span style="color:#ef4444;font-size:0.75rem;">unavailable</span>',
                            unsafe_allow_html=True)

        # Thin divider between rows
        if i < len(outputs) - 1:
            st.markdown('<hr style="border:none;border-top:1px solid rgba(255,255,255,0.04);margin:0;">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Auto-refresh while pipeline is running
# ══════════════════════════════════════════════════════════════════════════════
if any_running:
    time.sleep(2)
    st.rerun()
