import os
import json
from datetime import datetime
from google.cloud import pubsub_v1
import logging

# === Logging Setup ===
logging.basicConfig(
    filename='/home/srilakp/subscriber.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"
output_dir = "received_data"

os.makedirs(output_dir, exist_ok=True)

# === Callback to process each message ===
def callback(message):
    print(f"Received raw message: {message.data}")
    logger.info(f"Received raw message: {message.data}")
    try:
        record = json.loads(message.data.decode("utf-8"))
        today_opd = datetime.now().strftime("%d%b%Y").upper() + ":00:00:00"  # e.g., 13APR2025:00:00:00
        opd_date_str = record.get("OPD_DATE", today_opd)  # Default to today
        date_obj = datetime.strptime(opd_date_str, "%d%b%Y:%H:%M:%S")
        date_str = date_obj.strftime("%Y-%m-%d")  # e.g., 2025-04-13
        output_file = os.path.join(output_dir, f"{date_str}.jsonl")
        print(f"Attempting to save to {output_file}")
        logger.info(f"Attempting to save to {output_file}")
        with open(output_file, "a") as f:
            f.write(json.dumps(record) + "\n")
        print(f"Saved message to {output_file}")
        logger.info(f"Saved message to {output_file}")
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        logger.error(f"Error processing message: {e}")
        message.nack()

# === Subscriber setup ===
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening to {subscription_path}")
logger.info(f"Listening to {subscription_path}")
streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

# === Keep the subscriber running ===
try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("Subscriber stopped.")
    logger.info("Subscriber stopped")
