import os
import json
import shutil
from datetime import datetime
from google.cloud import pubsub_v1
import logging

# === Disable all logging output ===
logging.getLogger().disabled = True
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"
output_dir = "received_data"

os.makedirs(output_dir, exist_ok=True)

# === Callback to process each message ===
def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        
        opd_date_str = record.get("OPD_DATE")
        if not opd_date_str:
            message.ack()  # or nack(), depending on your policy
            return
        date_obj = datetime.strptime(opd_date_str, "%d%b%Y:%H:%M:%S")
        date_str = date_obj.strftime("%Y-%m-%d")  # e.g., 2025-04-28

        output_file = os.path.join(output_dir, f"{date_str}.jsonl")

        with open(output_file, "a") as f:
            f.write(json.dumps(record) + "\n")
        
        logger.info(f"Saved message to {output_file}")

        message.ack()

    except Exception as e:
       # print(f"Error processing message: {e}")
        logger.error(f"Error processing message: {e}")
        message.nack()

# === (Optional) Function to compress the received data folder ===
def compress_received_data():
    today_str = datetime.now().date().isoformat()
    zip_base = f"received_data_{today_str}"
    shutil.make_archive(zip_base, 'zip', output_dir)
    # print(f"Compressed received_data/ into {zip_base}.zip")
    logger.info(f"Compressed received_data/ into {zip_base}.zip")

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
