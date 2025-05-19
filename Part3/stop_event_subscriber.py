import json
import pandas as pd
import psycopg2
from io import StringIO
from google.cloud import pubsub_v1


class StopEventSubscriber:
    def __init__(self, project_id, subscription_id, db_config):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.db_config = db_config
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.json_list = []

    def callback(self, message: pubsub_v1.subscriber.message.Message):
        try:
            json_message = json.loads(message.data.decode('utf-8'))
            self.json_list.append(json_message)
        except Exception as e:
            print(f"[callback] error decoding message: {e}")
        finally:
            message.ack()

    def listen(self, timeout=400.0):
        streaming_pull = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}...\n")
        with self.subscriber:
            try:
                streaming_pull.result(timeout=timeout)
            except Exception as e:
                print(f"[Pub/Sub] streaming pull terminated: {e}")
                streaming_pull.cancel()

    def load_to_postgres(self, table_name):
        df = pd.DataFrame(self.json_list)
        if df.empty:
            print("No messages received.")
            return

        print(f"Received {len(df)} stop events")

        # Reorder and validate columns to match table schema
        expected_columns = [
            'vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
            'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell',
            'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
            'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance',
            'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'
        ]

        try:
            df = df[expected_columns]
        except KeyError as e:
            print(f" Missing columns in incoming data: {e}")
            return

        # Connect and insert into PostgreSQL
        conn = psycopg2.connect(**self.db_config)
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table_name, sep=",")
            conn.commit()
            print(f" Loaded {table_name} with {len(df)} rows")
        except Exception as e:
            print(f" Error loading {table_name}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def run(self):
        self.listen()
        self.load_to_postgres("stop_events")


if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'database': 'trimet_data',
        'user': 'srilakshmi',
        'password': 'srilu2001'
    }

    subscriber = StopEventSubscriber(
        project_id="dataengineeringproject-456307",
        subscription_id="stop-events-topic-sub",
        db_config=db_config
    )
    subscriber.run()
