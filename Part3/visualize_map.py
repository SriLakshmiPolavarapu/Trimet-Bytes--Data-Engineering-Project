import psycopg2
import pandas as pd
import folium
from folium.plugins import MarkerCluster

conn = psycopg2.connect(
    host="localhost",
    database="trimet_data",
    user="srilakshmi",
    password="####"
)

query = """
SELECT 
    t.trip_id,
    t.vehicle_id,
    t.route_id,
    t.service_key,
    b.tstamp,
    b.latitude,
    b.longitude,
    b.speed
FROM trip t
JOIN breadcrumb b ON t.trip_id = b.trip_id
WHERE b.latitude IS NOT NULL AND b.longitude IS NOT NULL
ORDER BY t.trip_id, b.tstamp;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No breadcrumb data found.")
    exit()

center_lat = df['latitude'].astype(float).mean()
center_lon = df['longitude'].astype(float).mean()
m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

for trip_id, trip_data in df.groupby("trip_id"):
    trip_data = trip_data.sort_values("tstamp")
    coordinates = trip_data[['latitude', 'longitude']].astype(float).values.tolist()
    
    folium.PolyLine(
        locations=coordinates,
        color="blue",
        weight=3,
        opacity=0.7,
        tooltip=f"Trip: {trip_id}"
    ).add_to(m)

    if len(coordinates) > 1:
        folium.Marker(coordinates[0], popup="Start", icon=folium.Icon(color="green")).add_to(m)
        folium.Marker(coordinates[-1], popup="End", icon=folium.Icon(color="red")).add_to(m)

m.save("breadcrumb_visualization.html")
print(" Map saved as breadcrumb_visualization.html")
