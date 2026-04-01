import sys
import cv2
import numpy as np
from ultralytics import YOLO
from confluent_kafka import Consumer

def run_worker(wid, bootstrap, topic):
    model = YOLO('yolov8n.pt') # Lightweight model for speed
    
    c = Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': 'yolo-cluster-group',
        'auto.offset.reset': 'latest'
    })
    c.subscribe([topic])

    frames_processed = 0
    print(f"[Worker {wid}] Ready.")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            
            # Convert bytes back to Image
            data = np.frombuffer(msg.value(), dtype=np.uint8)
            frame = cv2.imdecode(data, cv2.IMREAD_COLOR)

            if frame is not None:
                # Run Inference
                results = model(frame, verbose=False)
                frames_processed += 1
                
                # Useful result summary
                detections = results[0].boxes.cls.tolist()
                objects = [model.names[int(cls)] for cls in detections]
                
                # Output statistics
                print(f"📊 Worker:{wid} | Frames:{frames_processed} | Found:{objects}")

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    run_worker(sys.argv[1], sys.argv[2], sys.argv[3])