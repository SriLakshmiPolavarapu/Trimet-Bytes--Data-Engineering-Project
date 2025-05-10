from io import StringIO
import psycopg2
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import os
import json
import pandas as pd

# === Your Config ===
project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"
DBname = "trimet_data"
DBuser = "srilakshmi"
DBpwd = "#####"

# === Pub/Sub Setup ===
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        json_message = json.loads(message.data.decode('utf-8'))
        json_list.append(json_message)
    except Exception as e:
        print(f"[callback] error decoding message: {e}")
    finally:
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        # Will listen until an exception occurs
        streaming_pull_future.result(timeout=400.0)
    except Exception as e:
        print(f"[Pub/Sub] streaming pull terminated: {e}")
        streaming_pull_future.cancel()

# === Load into DataFrame ===
df = pd.DataFrame(json_list)

if not df.empty:
    print(f"Received {len(df)} messages")

    # === Transformations ===
    df['NEW_OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
    df['DAY_OF_WEEK'] = df['NEW_OPD_DATE'].dt.dayofweek
    df['DAY_NAME'] = df['DAY_OF_WEEK'].map({
        0: 'Weekday', 1: 'Weekday', 2: 'Weekday',
        3: 'Weekday', 4: 'Weekday', 5: 'Saturday', 6: 'Sunday'
    })

    def create_timestamp(row):
        try:
            opd_date = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
            act_time = timedelta(seconds=min(row.get('ACT_TIME', 0), 86399))
            return pd.Timestamp(opd_date + act_time)
        except Exception as e:
            print(f"[create_timestamp] error on row {row.name}: {e}")
            return pd.NaT

    df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)
    df.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP', 'VEHICLE_ID'], inplace=True)

    # Calculate speed (meters per second)
    df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff() / df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()
    df['SPEED'] = df['SPEED'].bfill().clip(lower=0)

    # Fill missing GPS coords
    df['GPS_LATITUDE'] = df['GPS_LATITUDE'].fillna(0.0)
    df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].fillna(0.0)

    # Dedupe for trip table
    result_df = df.drop_duplicates(subset=['EVENT_NO_TRIP'], keep='first').copy()
    result_df.loc[:, 'ROUTE_ID'] = 0
    result_df.loc[:, 'DIRECTION'] = 'Out'  # ENUM-safe value

    # Prepare trip DataFrame
    df_trip = result_df[[
        'EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'DAY_NAME', 'DIRECTION'
    ]].rename(columns={
        'EVENT_NO_TRIP': 'trip_id',
        'ROUTE_ID': 'route_id',
        'VEHICLE_ID': 'vehicle_id',
        'DAY_NAME': 'service_key',
        'DIRECTION': 'direction'
    })

    # Prepare breadcrumb DataFrame
    df_breadcrumb = df[[
        'TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP'
    ]].rename(columns={
        'TIMESTAMP': 'tstamp',
        'GPS_LATITUDE': 'latitude',
        'GPS_LONGITUDE': 'longitude',
        'SPEED': 'speed',
        'EVENT_NO_TRIP': 'trip_id'
    })

    # === PostgreSQL Insert ===
    conn = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd
    )

    def copy_from_df(conn, df, table):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",")
            conn.commit()
            print(f"[copy_from_df] Loaded {table} with {len(df)} rows")
        except Exception as e:
            print(f"[copy_from_df] Error loading {table}: {e}")
            conn.rollback()
        finally:
            cursor.close()

    copy_from_df(conn, df_trip, "trip")
    copy_from_df(conn, df_breadcrumb, "breadcrumb")

    conn.close()
else:
    print("No messages received.")
