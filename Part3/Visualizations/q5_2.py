import folium
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    dbname='trimet_data',
    user='srilakshmi',
    password='####',
    host='localhost',
    port='5432'
)

query = """
SELECT latitude, longitude, speed
FROM breadcrumb
WHERE EXTRACT(HOUR FROM tstamp) BETWEEN 22 AND 23
  AND DATE(tstamp) = '2023-01-15'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No data found for Visualization 7.")
else:
    m = folium.Map(location=[df.latitude.mean(), df.longitude.mean()], zoom_start=13)
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            popup=f"Speed: {row['speed']}",
            color='blue',
            fill=True,
            fill_opacity=0.6
        ).add_to(m)

    m.save("q7_late_night_trips.html")
    print("Saved q7_late_night_trips.html")
