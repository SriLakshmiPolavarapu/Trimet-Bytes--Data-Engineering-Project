import os
import json
import requests
import zipfile
import shutil
import pandas as pd
from datetime import date
from google.cloud import pubsub_v1

# === CONFIGURATION ===
today_str = date.today().isoformat()
output_folder = "bus_data"
processed_data_folder = "processed_data"
extract_folder = os.path.join("extracted_json", today_str)

project_id = "dataengineeringproject-456307"
topic_id = "MyTopic1"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# === Helper: Folder Size in Bytes ===
def get_folder_size(folder):
    total_size = 0
    for dirpath, _, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    return total_size

# === Step 1: Gather and Save Bus Data ===
def gather_bus_data():
    os.makedirs(output_folder, exist_ok=True)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    vehicle_ids_path = os.path.join(script_dir, "vehicle_ids.csv")

    try:
        df = pd.read_csv(vehicle_ids_path, header=None)
        vehicle_ids = df[0].astype(str).str.strip().tolist()
    except Exception:
        return 0

    total_records = 0

    for vid in vehicle_ids:
        try:
            url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                total_records += len(data)
                file_path = os.path.join(output_folder, f"bus_{vid}_{today_str}.json")
                with open(file_path, "w") as out:
                    out.write(response.text)
        except Exception:
            pass

    os.makedirs(processed_data_folder, exist_ok=True)
    zip_base = os.path.join(processed_data_folder, f"bus_data_{today_str}")
    shutil.make_archive(zip_base, 'zip', output_folder)

    try:
        shutil.rmtree(output_folder)
    except Exception:
        pass

    return total_records

# === Step 2: Unzip the Zipped Data ===
def unzip_data(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    except Exception:
        pass

# === Step 3: Publish Extracted Data to Pub/Sub ===
def publish_data(folder):
    total_published = 0
    for filename in os.listdir(folder):
        if filename.endswith(".json"):
            file_path = os.path.join(folder, filename)
            try:
                with open(file_path, "r") as f:
                    records = json.load(f)
                    futures = []
                    for record in records:
                        data = json.dumps(record).encode("utf-8")
                        try:
                            future = publisher.publish(topic_path, data)
                            futures.append(future)
                        except Exception:
                            pass

                    for future in futures:
                        try:
                            future.result()
                            total_published += 1
                        except Exception:
                            pass

                os.remove(file_path)
            except Exception:
                pass
    return total_published

# === MAIN ===
def main():
    gathered = gather_bus_data()
    print(f"Total breadcrumbs saved: {gathered}")

    zip_path = os.path.join(processed_data_folder, f"bus_data_{today_str}.zip")
    if os.path.exists(zip_path):
        os.makedirs(extract_folder, exist_ok=True)
        unzip_data(zip_path, extract_folder)
        published = publish_data(extract_folder)
        print(f"Total records published: {published}")

        try:
            shutil.rmtree(extract_folder)
        except Exception:
            pass

if __name__ == "__main__":
    main()
