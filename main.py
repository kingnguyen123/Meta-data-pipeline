import logging
import threading
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import uuid
import random
import time
import json

KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3  # Ensure you have at least 3 brokers running!
logging.basicConfig(level=logging.INFO)

TOPIC_NAME = "Transactions"
logger = logging.getLogger(__name__)

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'linger.ms': 10,
    'acks': 1,
    'compression.type': 'gzip'
}

producer = Producer(producer_conf)


# Create Kafka Topic
def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in cluster_metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f" Topic '{topic_name}' created successfully")
                except Exception as e:
                    logger.error(f" Failed to create topic '{topic_name}': {e}")
        else:
            logger.info(f"🔹 Topic '{topic_name}' already exists")
    except Exception as e:
        logger.error(f"⚠️ Error creating topic '{topic_name}': {e}")


# Generate transaction data
def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time()),
        merchantId=random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        transactionType=random.choice(['purchase', 'refund']),
        location=f"location_{random.randint(1, 50)}",
        paymentMethod=random.choice(['credit_card', 'paypal', 'bank_transfer']),
        isInternational=random.choice(['True', 'False']),
        currency=random.choice(['USD', 'EUR', 'GBP'])
    )


# Kafka Message Delivery Callback
def delivery(err, msg):
    if err is not None:
        logger.error(f" Failed to deliver message: {msg.key().decode()} | Error: {err}")
    else:
        logger.info(f" Message produced: {msg.key().decode()} successfully")


# Producing Transactions in Multiple Threads
def produce_transaction(thread_id, stop_event):
    while not stop_event.is_set():
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['userId'].encode('utf-8'),
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery
            )
            logger.info(f"Thread-{thread_id} 🔹 Producing transaction: {transaction}")
            producer.poll(1)  # ✅ Allow callback to execute
            #time.sleep(random.uniform(0.5, 2))  # ✅ Simulate real transaction intervals
        except Exception as e:
            logger.error(f"❌ Failed to produce transaction: {e}")


# Start Multiple Producer Threads
def producer_data_in_parallel(num_threads):
    threads = []
    stop_event = threading.Event()

    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i, stop_event))
            thread.daemon = False  # ✅ Ensure threads do not terminate abruptly
            thread.start()
            threads.append(thread)

        logger.info(f"✅ Started {num_threads} producer threads.")

        # Keep running until interrupted
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("⚠️ Received termination signal. Stopping producers gracefully...")
        stop_event.set()  # Signal threads to stop
        for thread in threads:
            thread.join()  # Wait for all threads to finish
        producer.flush()  # ✅ Ensure all messages are sent before exiting
        logger.info("✅ Producer shut down successfully.")


if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    producer_data_in_parallel(3)  # ✅ Start 3 producer threads

