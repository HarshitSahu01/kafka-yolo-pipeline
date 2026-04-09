# Kafka + YOLOv8 Real-Time Video Pipeline

Distributed video inference pipeline where frames are ingested into Kafka, processed by parallel YOLO workers, then reassembled into an annotated MP4.

```
Input (file or RTSP)
    |
    v
app/ingestion.py --(JPEG frames + metadata)--> Kafka topic: quickstart-events
    |
    v
app/worker_logic.py x N --(annotated JPEG + frame_id)--> Kafka topic: yolo-results
    |
    v
app/result_collector.py --(reorder + ffmpeg encode)--> output/output_<timestamp>.mp4
```

## Architecture Notes

- Ingestion clears and recreates both Kafka topics at startup for a clean run.
- quickstart-events is recreated with 8 partitions for worker parallelism.
- yolo-results is recreated with 1 partition for stable output ordering.
- Collector uses a per-run consumer group and frame-id reorder buffering before encoding.

## Repository Layout

```
kafka-yolo-pipeline/
├── app/
│   ├── ingestion.py
│   ├── spawn_workers.py
│   ├── worker_logic.py
│   ├── result_collector.py
│   ├── logs/
│   └── output/
├── dashboard.py
├── docker-compose.yml
├── requirements.txt
├── streaming/
│   └── streamscript.sh
├── logs/
├── output/
└── yolov8n.pt
```

## Prerequisites

- Python 3.10+
- Docker + Docker Compose
- ffmpeg (required by collector and RTSP helper script)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Start Infrastructure

```bash
docker compose up -d
```

This starts:

- Zookeeper
- Kafka
- MediaMTX (RTSP server on rtsp://localhost:8554/live)

Stop infrastructure:

```bash
docker compose down
```

## Run Pipeline (CLI)

Run the app scripts from the app directory:

```bash
cd app
```

1. Start workers

```bash
python spawn_workers.py --workers 4
```

2. Start collector in another terminal

```bash
cd app
python result_collector.py
```

3. Start ingestion in another terminal

File input:

```bash
cd app
python ingestion.py --video ../path/to/video.mp4
```

RTSP input:

```bash
cd app
python ingestion.py --rtsp rtsp://127.0.0.1:8554/live
```

Output video is written to:

```text
app/output/output_<timestamp>.mp4
```

## RTSP Helper Script

Use streaming/streamscript.sh to loop a local file into MediaMTX:

```bash
bash streaming/streamscript.sh /absolute/or/relative/video.mp4
```

The script publishes to:

```text
rtsp://localhost:8554/live
```

## Streamlit Dashboard

The dashboard can start and stop ingestion + collector and lets you download the latest MP4.
Start worker processes first (for example: cd app && python spawn_workers.py --workers 4), then launch the dashboard.

```bash
streamlit run dashboard.py
```

Open the URL shown by Streamlit in your browser.

## Logs

Main runtime logs (when running from app/):

- app/logs/ingestion.log
- app/logs/spawn_workers.log
- app/logs/worker_<id>.log
- app/logs/collector.log

## Useful Configuration

Environment variables:

- KAFKA_BROKER (default: localhost:9092)
- MAX_LATENCY_SEC (worker stale-frame drop threshold, default disabled)

Common constants are defined near the top of:

- app/ingestion.py
- app/worker_logic.py
- app/result_collector.py

## Graceful Shutdown

Use Ctrl+C for each running process. Components flush outstanding work and close Kafka/ffmpeg resources before exit.
