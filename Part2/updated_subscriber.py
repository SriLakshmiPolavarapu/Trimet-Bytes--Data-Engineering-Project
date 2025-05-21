from io import StringIO
import psycopg2
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import os
import json
import pandas as pd

# === Config ===
project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"
DBname = "trimet_data"
DBuser = "srilakshmi"
DBpwd = "####"

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
        streaming_pull_future.result()  # infinite loop
    except KeyboardInterrupt:
        print("Stopping subscriber...")
        streaming_pull_future.cancel()

# === Load into DataFrame ===
df = pd.DataFrame(json_list)

if not df.empty:
    print(f"Received {len(df)} messages")

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

    df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff() / df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()
    df['SPEED'] = df['SPEED'].bfill().clip(lower=0)

    df['GPS_LATITUDE'] = df['GPS_LATITUDE'].fillna(0.0)
    df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].fillna(0.0)

    # === Assertions (Validation) ===
    def assert_opd_date(row):
        try:
            assert isinstance(row['OPD_DATE'], str) and len(row['OPD_DATE']) > 0
        except AssertionError:
            raise ValueError("Invalid OPD_DATE")

    def assert_vehicle_id(row):
        try:
            assert row['VEHICLE_ID'] > 0
        except AssertionError:
            raise ValueError("Invalid VEHICLE_ID")

    def assert_act_time(row):
        try:
            assert 0 <= row.get('ACT_TIME', -1) <= 86399
        except AssertionError:
            raise ValueError("Invalid ACT_TIME")

    def assert_gps_lat(row):
        try:
            assert -90.0 <= row['GPS_LATITUDE'] <= 90.0
        except AssertionError:
            raise ValueError("Invalid GPS_LATITUDE")

    def assert_gps_long(row):
        try:
            assert -180.0 <= row['GPS_LONGITUDE'] <= 180.0
        except AssertionError:
            raise ValueError("Invalid GPS_LONGITUDE")

    def assert_event_trip(row):
        try:
            assert row['EVENT_NO_TRIP'] > 0
        except AssertionError:
            raise ValueError("Invalid EVENT_NO_TRIP")

    def assert_meters(row):
        try:
            assert row['METERS'] >= 0
        except AssertionError:
            raise ValueError("Invalid METERS value")

    def assert_speed(row):
        try:
            assert row['SPEED'] >= 0
        except AssertionError:
            raise ValueError("Invalid SPEED")

    def assert_timestamp(row):
        try:
            assert not pd.isna(row['TIMESTAMP'])
        except AssertionError:
            raise ValueError("Missing TIMESTAMP")

    def assert_day_of_week(row):
        try:
            assert row['DAY_OF_WEEK'] in range(7)
        except AssertionError:
            raise ValueError("Invalid DAY_OF_WEEK")

    # === Filter out invalid rows ===
    valid_rows = []
    for i, row in df.iterrows():
        try:
            assert_opd_date(row)
            assert_vehicle_id(row)
            assert_act_time(row)
            assert_gps_lat(row)
            assert_gps_long(row)
            assert_event_trip(row)
            assert_meters(row)
            assert_speed(row)
            assert_timestamp(row)
            assert_day_of_week(row)
            valid_rows.append(row)
        except Exception as e:
            print(f"[Validation] Row {i} failed validation: {e}")

    df = pd.DataFrame(valid_rows)

    # === Transformation for DB ===
    result_df = df.drop_duplicates(subset=['EVENT_NO_TRIP'], keep='first').copy()
    result_df.loc[:, 'ROUTE_ID'] = 0
    result_df.loc[:, 'DIRECTION'] = 'Out'

    df_trip = result_df[[
        'EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'DAY_NAME', 'DIRECTION'
    ]].rename(columns={
        'EVENT_NO_TRIP': 'trip_id',
        'ROUTE_ID': 'route_id',
        'VEHICLE_ID': 'vehicle_id',
        'DAY_NAME': 'service_key',
        'DIRECTION': 'direction'
    })

    df_breadcrumb = df[[
        'TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP'
    ]].rename(columns={
        'TIMESTAMP': 'tstamp',
        'GPS_LATITUDE': 'latitude',
        'GPS_LONGITUDE': 'longitude',
        'SPEED': 'speed',
        'EVENT_NO_TRIP': 'trip_id'
    })

    # === Insert into PostgreSQL ===
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
    
    print(f"Total messages received: {len(json_list)}")
    print(f"Valid trips inserted: {len(df_trip)}")
    print(f"Valid breadcrumbs inserted: {len(df_breadcrumb)}")

    try:
        conn = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trip;")
        total_trips = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM breadcrumb;")
        total_breadcrumbs = cursor.fetchone()[0]
        print(f"Total rows in DB - trip: {total_trips}, breadcrumb: {total_breadcrumbs}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[Summary] Failed to fetch DB row counts: {e}")
else:
    print("No messages received.")
