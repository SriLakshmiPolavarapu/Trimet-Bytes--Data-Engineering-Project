import os
import json
import zipfile
from google.cloud import pubsub_v1
import logging
from datetime import datetime

# === Logging Setup ===
logging.basicConfig(
    filename='/home/srilakp/publisher.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
project_id = "dataengineeringproject-456307"
topic_id = "MyTopic1"
today_str = datetime.now().strftime("%Y-%m-%d")  # e.g., 2025-04-13
today_opd = datetime.now().strftime("%d%b%Y").upper() + ":00:00:00"  # e.g., 13APR2025:00:00:00
zip_filename = f"bus_data_{today_str}.zip"
zip_path = os.path.join(os.getcwd(), zip_filename)
extract_folder = os.path.join(os.getcwd(), f"extracted_json/{today_str}")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def unzip_file(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {zip_path} to {extract_to}")
        logger.info(f"Extracted {zip_path} to {extract_to}")
    except Exception as e:
        print(f"Failed to unzip file: {e}")
        logger.error(f"Failed to unzip file: {e}")

def publish_json_from_folder(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".json"):
            file_path = os.path.join(folder, filename)
            print(f"Publishing records from {filename}")
            logger.info(f"Publishing records from {filename}")
            try:
                with open(file_path, 'r') as f:
                    records = json.load(f)
                    for record in records:
                        # Ensure OPD_DATE is set to today's date
                        record["OPD_DATE"] = today_opd
                        data = json.dumps(record).encode("utf-8")
                        try:
                            future = publisher.publish(topic_path, data)
                            print(f"Published msg ID: {future.result()}")
                            logger.info(f"Published msg ID: {future.result()}")
                        except Exception as e:
                            print(f"Failed to publish: {e}")
                            logger.error(f"Failed to publish: {e}")
            except Exception as e:
                print(f"Error reading JSON from {filename}: {e}")
                logger.error(f"Error reading JSON from {filename}: {e}")

# === Run ===
if os.path.exists(zip_path):
    os.makedirs(extract_folder, exist_ok=True)
    unzip_file(zip_path, extract_folder)
    publish_json_from_folder(extract_folder)
else:
    print(f"ZIP file not found: {zip_path}")
    logger.error(f"ZIP file not found: {zip_path}")
