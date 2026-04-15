import streamlit as st
import pandas as pd
import subprocess
import time
import os

# --- Configuration ---
st.set_page_config(page_title="YOLO Pipeline Controls", layout="wide")
st.title("YOLO Live Stream Pipeline")

DETECTIONS_CSV = "logs/detections.csv"

# --- State Management ---
if 'processes' not in st.session_state:
    st.session_state.processes = []
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False

# --- Process Handlers ---
def start_pipeline(rtsp_link):
    if not st.session_state.pipeline_running:
        st.session_state.processes = []
        
        # 1. Spawn Workers
        p1 = subprocess.Popen(["python", "app/spawn_workers.py"])
        st.session_state.processes.append(p1)
        
        # 2. Start Result Collector
        p2 = subprocess.Popen(["python", "app/result_collector.py"])
        st.session_state.processes.append(p2)
        
        # 3. Start Ingestion
        if rtsp_link and rtsp_link.strip():
            cmd = ["python", "app/ingestion.py", "--rtsp", rtsp_link.strip()]
        else:
            cmd = ["python", "app/ingestion.py"]
        p3 = subprocess.Popen(cmd)
        st.session_state.processes.append(p3)
        
        st.session_state.pipeline_running = True

def stop_pipeline():
    if st.session_state.pipeline_running:
        for p in st.session_state.processes:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        st.session_state.processes = []
        st.session_state.pipeline_running = False

# --- UI Controls ---
rtsp_link = st.text_input("RTSP Input Link", placeholder="rtsp://... (leave blank for default video)")

col1, col2, col3, col4 = st.columns([1, 1, 1.5, 1.5])

with col1:
    if st.button("Start Pipeline", use_container_width=True, disabled=st.session_state.pipeline_running):
        start_pipeline(rtsp_link)
        st.rerun()

with col2:
    if st.button("Stop Pipeline", use_container_width=True, disabled=not st.session_state.pipeline_running):
        stop_pipeline()
        st.rerun()

with col3:
    # Handle Download button
    if os.path.exists(DETECTIONS_CSV) and os.path.getsize(DETECTIONS_CSV) > 0:
        with open(DETECTIONS_CSV, "rb") as f:
            csv_data = f.read()
        st.download_button(
            label="Download Results (CSV)",
            data=csv_data,
            file_name='detections.csv',
            mime='text/csv',
            use_container_width=True
        )
    else:
        st.download_button("Download Results (CSV)", data="", disabled=True, use_container_width=True)

with col4:
    auto_refresh = st.checkbox("Auto-Refresh Dashboard (2s)", value=True)

if st.session_state.pipeline_running:
    st.success("Pipeline is currently running (managed via UI)...")

st.divider()

# --- Live Results Dashboard ---
st.subheader("Live Pipeline Dashboard")

if os.path.exists(DETECTIONS_CSV) and os.path.getsize(DETECTIONS_CSV) > 0:
    try:
        # Load the CSV data
        df = pd.read_csv(DETECTIONS_CSV)
        
        if not df.empty:
            total_frames = df['frame_id'].nunique() if 'frame_id' in df.columns else 0
            total_detections = len(df)
            
            avg_latency_str = "N/A"
            if 'timestamp_ms' in df.columns:
                latest_ts = df['timestamp_ms'].max()
                current_time_ms = time.time() * 1000
                latency = max(0, current_time_ms - latest_ts)
                avg_latency_str = f"{latency:.0f} ms"
                
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Frames Processed", total_frames)
            m2.metric("Total Object Detections", total_detections)
            m3.metric("Latest Est. Latency", avg_latency_str)
            
            st.divider()
            
            col_chart, col_table = st.columns(2)
            
            with col_chart:
                st.subheader("Detections by Class")
                if 'class_name' in df.columns:
                    class_counts = df['class_name'].value_counts().reset_index()
                    class_counts.columns = ['class_name', 'count']
                    st.bar_chart(class_counts, x='class_name', y='count')
                else:
                    st.info("Class names not available in data.")
                    
            with col_table:
                st.subheader("Recent Detections Table")
                if 'timestamp_ms' in df.columns:
                    df_sorted = df.sort_values(by='timestamp_ms', ascending=False)
                    st.dataframe(df_sorted.head(100), use_container_width=True)
                else:
                    st.dataframe(df.head(100), use_container_width=True)
        else:
            st.caption("No detections yet.")
    except Exception as e:
        st.error(f"Error reading results: {e}")
else:
    st.info("No results to display yet. Wait for pipeline data or start the pipeline.")

# Auto refresh every 2 seconds
if auto_refresh:
    time.sleep(2)
    st.rerun()
