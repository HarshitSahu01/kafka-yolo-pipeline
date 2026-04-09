from confluent_kafka import Consumer, TopicPartition
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'monitor',
    'enable.auto.commit': False
})

topic = "your_topic"

while True:
    md = consumer.list_topics(topic, timeout=5)
    partitions = md.topics[topic].partitions.keys()

    for p in partitions:
        tp = TopicPartition(topic, p)
        low, high = consumer.get_watermark_offsets(tp)

        first_msg = low
        last_msg = high - 1 if high > 0 else None

        print(f"Partition {p}: first={first_msg}, last={last_msg}")

    time.sleep(2)  # your T seconds