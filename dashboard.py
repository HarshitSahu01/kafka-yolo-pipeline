import streamlit as st
import subprocess
import signal
import os
import glob
import time

st.set_page_config(page_title="YOLO Kafka Pipeline", layout="wide")
st.title("YOLO Kafka Pipeline Dashboard")

# Ensure required directories exist
os.makedirs("output", exist_ok=True)
os.makedirs("uploads", exist_ok=True)

if "ingest_pid" not in st.session_state:
    st.session_state.ingest_pid = None
if "collect_pid" not in st.session_state:
    st.session_state.collect_pid = None

def start_processes(cmd_args):
    # Stop any existing processes
    stop_processes()
    
    # Start collector first
    st.session_state.collect_pid = subprocess.Popen(["uv", "run", "app/result_collector.py"]).pid
    time.sleep(1) # wait for collector to bind to group
    
    # Start ingestion
    st.session_state.ingest_pid = subprocess.Popen(["uv", "run", "app/ingestion.py"] + cmd_args).pid

def stop_processes():
    if st.session_state.ingest_pid:
        try:
            os.kill(st.session_state.ingest_pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        st.session_state.ingest_pid = None
        
    if st.session_state.collect_pid:
        try:
            os.kill(st.session_state.collect_pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        st.session_state.collect_pid = None
    
    # Allow time for graceful shutdown and final mp4 creation
    if st.session_state.ingest_pid or st.session_state.collect_pid:
        time.sleep(2) 

col1, col2 = st.columns(2)

with col1:
    st.header("Input Source")
    st.markdown("Ensure the worker cluster (`uv run spawn_workers.py`) is already running!")
    
    rtsp_url = st.text_input("RTSP Link", placeholder="rtsp://localhost:8554/live")
    st.markdown("**OR**")
    uploaded_file = st.file_uploader("Upload Video", type=["mp4", "avi", "mov"])
    
    col_btn_1, col_btn_2 = st.columns(2)
    with col_btn_1:
        if st.button("Start Pipeline", use_container_width=True):
            if uploaded_file:
                video_path = os.path.join("uploads", uploaded_file.name)
                with open(video_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                start_processes(["--video", video_path])
                st.success(f"Started pipeline with {uploaded_file.name}")
            elif rtsp_url:
                start_processes(["--rtsp", rtsp_url])
                st.success(f"Started pipeline with RTSP stream.")
            else:
                st.error("Please provide an RTSP link or upload a video.")
                
    with col_btn_2:
        if st.button("Stop Pipeline", use_container_width=True):
            stop_processes()
            st.success("Pipeline stopped. Finalising video encoding...")
            time.sleep(3)
            st.rerun()

with col2:
    st.header("Actions & Statistics")
    
    # Download MP4
    videos = sorted(glob.glob("output/*.mp4"), key=os.path.getmtime, reverse=True)
    if videos:
        latest = videos[0]
        st.write(f"**Latest Video:** {os.path.basename(latest)}")
        with open(latest, "rb") as f:
            st.download_button("Download Latest Output MP4", f, file_name=os.path.basename(latest), mime="video/mp4")
    else:
        st.write("No output videos yet.")
        
    st.subheader("Latest Pipeline Statistics")
    
    # Read the latest Video saved statistics from collector log
    if os.path.exists("logs/collector.log"):
        with open("logs/collector.log", "r") as f:
            log_lines = f.readlines()
            saved_stats = [line for line in log_lines if "Video saved" in line]
            if saved_stats:
                st.success(saved_stats[-1].strip())
            else:
                st.info("Video currently encoding or not finalised yet...")
    
    st.subheader("Realtime Ingestion Logs")
    if os.path.exists("logs/ingestion.log"):
        with open("logs/ingestion.log", "r") as f:
            log_lines = f.readlines()
            if log_lines:
                # show last 5 lines
                st.code("".join(log_lines[-7:]))
