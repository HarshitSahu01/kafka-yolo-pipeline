import json
from confluent_kafka import Consumer
c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'test2', 'auto.offset.reset': 'earliest'})
c.subscribe(['yolo-results'])
msg = c.poll(5.0)
if msg:
    for k, v in msg.headers() or []:
        if k == "detections":
            print(v.decode())
c.close()
