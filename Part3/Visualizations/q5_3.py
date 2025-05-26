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
WHERE speed > 25
  AND DATE(tstamp) = '2023-01-16'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No high-speed data found for the given date.")
else:
    m = folium.Map(location=[df.latitude.mean(), df.longitude.mean()], zoom_start=13)
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=3,
            popup=f"Speed: {row['speed']}",
            color='blue',
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

    m.save("q5c_high_speed_segments.html")
    print("Saved q5c_high_speed_segments.html")
