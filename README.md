# Distributed Real-Time YOLOv8 Inference Pipeline

Multi-process computer vision pipeline — video frames go into Kafka, a pool of YOLOv8 workers pulls them in parallel, and detections are written to SQLite in real time.

```
ingestion.py  ──►  Kafka (quickstart-events)  ──►  worker_logic.py × N
                                                           │
                                                           ▼
                                               result_collector.py → detections.db
```

---

## Project Structure

```
kafka-yolo-pipeline/
├── ingestion.py          # Producer: file loop or RTSP → Kafka
├── spawn_workers.py      # Orchestrator: pre-fetch model, spawn N workers
├── worker_logic.py       # Consumer: YOLOv8 inference → yolo-results
├── result_collector.py   # Aggregator: SQLite writer + live FPS display
├── docker-compose.yml    # Kafka + Zookeeper + MediaMTX
├── requirements.txt
├── race_car.mp4          # Sample video for testing
├── yolov8n.pt            # YOLOv8 nano weights (auto-downloaded if absent)
└── detections.db         # SQLite output (auto-created at runtime)
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 1 — Start Kafka (Docker Compose)

Make sure Docker is running, then:

```bash
docker compose up -d
```

Expected output:
```
✔ Container ... zookeeper  Started
✔ Container ... kafka      Started
✔ Container ... mediamtx   Started
```

To stop and tear down:
```bash
docker compose down
```

---

## Step 2 — Start Ingestion

### Option A — Local video file (loops forever)

```bash
# Default (loops race_car.mp4 at 30 fps)
python ingestion.py

# Custom video file
python ingestion.py --video path/to/video.mp4
```

### Option B — RTSP stream

First start the RTSP relay (requires FFmpeg):
```bash
ffmpeg -re -stream_loop -1 -i race_car.mp4 \
    -c copy -f rtsp -rtsp_transport tcp \
    rtsp://127.0.0.1:8554/live
```

Then attach the ingestion node:
```bash
python ingestion.py --rtsp rtsp://127.0.0.1:8554/live
```

Expected output (either mode):
```
[INFO] Ingesting 'race_car.mp4' → Kafka (looping at 30 fps).
[INFO] Loop 1.
```

---

## Step 3 — Spawn YOLOv8 Workers

```bash
# 4 workers (default)
python spawn_workers.py

# Custom count
python spawn_workers.py --workers 8
```

Expected output:
```
[INFO] Pre-fetching model 'yolov8n.pt'…
[INFO] Model ready.
[INFO] Spawning 4 workers…
[INFO] Worker 0 ready.
[INFO] Worker 1 ready.
...
```

---

## Step 4 — Start Result Collector

```bash
python result_collector.py
```

Expected output (updates every second):
```
[INFO] Collector started — writing to 'detections.db'.
[INFO] [Summary] FPS: 41.3 | Detections: {'car': 124, 'person': 3}
[INFO] [Summary] FPS: 38.9 | Detections: {'car': 117}
```

- **FPS** — total inference frames processed across all workers in the last second.
- **Detections** — object classes seen, aggregated over the 1-second window.

---

## Query Detections

```bash
sqlite3 detections.db "SELECT frame_id, worker_id, latency_sec, objects FROM detections LIMIT 10;"
```

Example:
```
12|0|0.043|["car", "car"]
13|2|0.051|["car"]
14|1|0.038|["car", "person"]
```

Schema:

| Column | Type | Description |
|---|---|---|
| `frame_id` | INTEGER | Frame sequence number from producer |
| `worker_id` | TEXT | Worker that ran inference |
| `latency_sec` | REAL | Frame age at time of inference |
| `objects` | TEXT | JSON array of detected class names |

---

## Graceful Shutdown

Press `Ctrl-C` in any terminal. Each node will:
1. Finish its current message.
2. Flush buffered rows to SQLite / flush Kafka producer.
3. Commit consumer offsets and exit cleanly.

---

## Logs

Each node writes to `logs/`:

| File | Node |
|---|---|
| `logs/ingestion.log` | `ingestion.py` |
| `logs/spawn_workers.log` | `spawn_workers.py` |
| `logs/worker_0.log` … | `worker_logic.py` instances |
| `logs/collector.log` | `result_collector.py` |

---

## Configuration

All constants are at the top of each file. Key values:

| Constant | Default | File |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` (env: `KAFKA_BROKER`) | all |
| `MAX_RESOLUTION` | `(1280, 720)` | `ingestion.py` |
| `JPEG_QUALITY` | `80` | `ingestion.py` |
| `TARGET_FPS` | `30` | `ingestion.py` |
| `MAX_LATENCY_SEC` | `0.5` | `worker_logic.py` |
| `YOLO_MODEL` | `yolov8n.pt` | `spawn_workers.py`, `worker_logic.py` |
| `DB_FILE` | `detections.db` | `result_collector.py` |
| `BATCH_SIZE` | `10` | `result_collector.py` |
