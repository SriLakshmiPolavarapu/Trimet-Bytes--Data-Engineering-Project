import os
import json
import requests
import shutil
import logging
from datetime import date
from google.cloud import pubsub_v1
import pandas as pd
import concurrent.futures
from bs4 import BeautifulSoup


class StopEventPublisher:
    def __init__(self, project_id, topic_id, key_path, vehicle_file):
        self.project_id = project_id
        self.topic_id = topic_id
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        self.vehicle_file = vehicle_file
        self.output_folder = "stop_events_data"
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        self.today_str = date.today().isoformat()

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
        self.logger = logging.getLogger(__name__)

    def gather_data(self):
        os.makedirs(self.output_folder, exist_ok=True)
        try:
            df = pd.read_csv(self.vehicle_file, header=None)
            vehicle_ids = df[0].astype(str).str.strip().tolist()
        except Exception as e:
            self.logger.error(f"Failed to read vehicle_ids.csv: {e}")
            return 0

        total_records = 0
        for vid in vehicle_ids:
            url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vid}"
            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                if response.status_code == 200 and "<table>" in response.text:
                    records = self.parse_html(response.text)
                    total_records += len(records)
                    file_path = os.path.join(self.output_folder, f"stop_{vid}_{self.today_str}.json")
                    with open(file_path, "w") as out:
                        json.dump(records, out)
                else:
                    self.logger.warning(f"No stop data for vehicle {vid} - Status: {response.status_code}")
            except Exception as e:
                self.logger.error(f"Error gathering stop data for vehicle {vid}: {e}")

        return total_records

    def parse_html(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find("table")
        if not table:
            return []

        rows = table.find_all("tr")
        header = [th.text.strip() for th in rows[0].find_all("th")]
        records = []

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) != len(header):
                continue
            record = {header[i]: cells[i].text.strip() for i in range(len(cells))}
            records.append(record)

        return records

    def publish_data(self):
        count = 0
        futures_list = []
        for filename in os.listdir(self.output_folder):
            if not filename.endswith(".json"):
                continue
            file_path = os.path.join(self.output_folder, filename)
            try:
                with open(file_path, "r") as f:
                    records = json.load(f)
            except Exception as e:
                self.logger.error(f"Error reading JSON {file_path}: {e}")
                continue

            for record in records:
                count += 1
                data = json.dumps(record).encode("utf-8")
                try:
                    future = self.publisher.publish(self.topic_path, data)
                    future.add_done_callback(lambda f: f.result())
                    futures_list.append(future)
                except Exception as e:
                    self.logger.error(f"Error publishing: {e}")

            os.remove(file_path)

        for future in concurrent.futures.as_completed(futures_list):
            continue

        return count

    def run(self):
        gathered = self.gather_data()
        print(f"Total stop events gathered: {gathered}")
        published = self.publish_data()
        print(f"Total stop events published: {published}")
        shutil.rmtree(self.output_folder, ignore_errors=True)


if __name__ == "__main__":
    publisher = StopEventPublisher(
        project_id="dataengineeringproject-456307",
        topic_id="stop-events-topic",
        key_path="dataengineeringproject-456307-2dca2bb9e633.json",
        vehicle_file="vehicle_ids.csv"
    )
    publisher.run()
