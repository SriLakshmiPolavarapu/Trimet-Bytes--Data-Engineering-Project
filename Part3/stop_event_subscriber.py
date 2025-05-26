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

    def listen(self):
        streaming_pull = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}...\n")
        with self.subscriber:
            try:
                streaming_pull.result()
            except Exception as e:
                print(f"[Pub/Sub] streaming pull terminated: {e}")
                streaming_pull.cancel()

    def validate_row(self, row):
        try:
            self.validate_vehicle_number(row)
            self.validate_stop_time(row)
            self.validate_maximum_speed(row)
            self.validate_direction(row)
            self.validate_trip_number(row)
            self.validate_service_key(row)
            self.validate_arrive_before_leave(row)
            self.validate_estimated_load(row)
            self.validate_dwell(row)
            self.validate_location_id(row)
            return True
        except:
            return False

    def validate_vehicle_number(self, row):
        try:
            assert row["vehicle_number"].isdigit()
        except:
            raise

    def validate_stop_time(self, row):
        try:
            assert row["stop_time"] not in (None, "", " ")
        except:
            raise

    def validate_maximum_speed(self, row):
        try:
            speed = float(row.get("maximum_speed", 0))
            assert 0 <= speed <= 70
        except:
            raise

    def validate_direction(self, row):
        try:
            assert row["direction"] in ["0", "1"]
        except:
            raise

    def validate_trip_number(self, row):
        try:
            assert row["trip_number"].isdigit()
        except:
            raise

    def validate_service_key(self, row):
        try:
            assert row["service_key"] in ["W", "S", "U"]
        except:
            raise

    def validate_arrive_before_leave(self, row):
        try:
            assert row["arrive_time"] <= row["leave_time"]
        except:
            raise

    def validate_estimated_load(self, row):
        try:
            assert row["estimated_load"] in ["", "low", "medium", "high"]
        except:
            raise

    def validate_dwell(self, row):
        try:
            assert int(row["dwell"]) >= 0
        except:
            raise

    def validate_location_id(self, row):
        try:
            assert row["location_id"].isdigit()
        except:
            raise

    def load_to_postgres(self, table_name):
        df = pd.DataFrame(self.json_list)
        if df.empty:
            print("No messages received.")
            return

        print(f"Received {len(df)} stop events")

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
            print(f"Missing columns in incoming data: {e}")
            return

        valid_rows = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            if self.validate_row(row_dict):
                valid_rows.append(row_dict)

        if not valid_rows:
            print("No valid records after validation.")
            return

        valid_df = pd.DataFrame(valid_rows)
        conn = psycopg2.connect(**self.db_config)
        buffer = StringIO()
        valid_df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table_name, sep=",")
            conn.commit()
            print(f"Loaded {table_name} with {len(valid_df)} validated rows")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")
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
        'password': '####'
    }

    subscriber = StopEventSubscriber(
        project_id="dataengineeringproject-456307",
        subscription_id="stop-events-topic-sub",
        db_config=db_config
    )
    subscriber.run()
