import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import psycopg2
from io import StringIO

VEHICLE_IDS_CSV = "vehicle_ids.csv"
DB_CONFIG = {
    "host": "localhost",
    "database": "trimet_data",
    "user": "srilakshmi",
    "password": "####",
    "port": 5432
}
URL_TEMPLATE = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={}"

def fetch_breadcrumb_data(vehicle_id):
    url = URL_TEMPLATE.format(vehicle_id)
    try:
        response = requests.get(url)
        if response.status_code == 200 and response.text.strip():
            return response.json()
    except:
        pass
    return []

def transform_breadcrumbs(records):
    df = pd.DataFrame(records)
    if df.empty or 'OPD_DATE' not in df.columns:
        return pd.DataFrame()

    df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format="%d%b%Y:%H:%M:%S", errors='coerce')
    df = df.dropna(subset=['OPD_DATE'])

    def to_timestamp(row):
        try:
            return row['OPD_DATE'] + timedelta(seconds=min(int(row.get('ACT_TIME', 0)), 86399))
        except:
            return pd.NaT

    df['tstamp'] = df.apply(to_timestamp, axis=1)
    df['latitude'] = pd.to_numeric(df.get('GPS_LATITUDE', None), errors='coerce')
    df['longitude'] = pd.to_numeric(df.get('GPS_LONGITUDE', None), errors='coerce')
    df['speed'] = pd.to_numeric(df.get('SPEED', None), errors='coerce')
    df['trip_id'] = pd.to_numeric(df.get('EVENT_NO_TRIP', None), errors='coerce')

    breadcrumb_df = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']]
    breadcrumb_df = breadcrumb_df.dropna(subset=['tstamp'])

    return breadcrumb_df

def copy_from_df(conn, df, table_name):
    buffer = StringIO()
    df = df.where(pd.notnull(df), None)
    df.to_csv(buffer, index=False, header=False, sep=",", na_rep='\\N')
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table_name, sep=",", null='\\N')
        conn.commit()
    except:
        conn.rollback()
    finally:
        cursor.close()

def main():
    if not os.path.exists(VEHICLE_IDS_CSV):
        return

    vehicle_ids = pd.read_csv(VEHICLE_IDS_CSV, header=None)[0].astype(str).tolist()
    all_records = []
    for vid in vehicle_ids:
        data = fetch_breadcrumb_data(vid)
        if data:
            all_records.extend(data)

    df = transform_breadcrumbs(all_records)
    if df.empty:
        return

    print(f"Final breadcrumb rows ready to insert: {len(df)}")

    conn = psycopg2.connect(**DB_CONFIG)
    copy_from_df(conn, df, "breadcrumb")
    conn.close()

if __name__ == "__main__":
    main()
