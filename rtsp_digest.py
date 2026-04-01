import cv2
import sys
from confluent_kafka import Producer

def stream_to_kafka(rtsp_url):
    # Optimized for throughput
    p = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 100000,
        'linger.ms': 0 # Send immediately
    })

    cap = cv2.VideoCapture(rtsp_url)
    if not cap.isOpened():
        print(f"Error: Could not open stream {rtsp_url}")
        return

    print(f"Streaming frames to Kafka...")
    try:
        while True:
            success, frame = cap.read()
            if not success:
                break
            
            # Encode to JPEG (reduces network load significantly)
            _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
            
            # Produce to Kafka
            p.produce('quickstart-events', buffer.tobytes())
            p.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        cap.release()
        p.flush()

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "rtsp://127.0.0.1:8554/live"
    stream_to_kafka(url)