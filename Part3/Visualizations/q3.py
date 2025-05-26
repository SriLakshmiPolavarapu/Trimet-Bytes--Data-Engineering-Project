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
SELECT b.latitude, b.longitude, b.speed
FROM breadcrumb b
JOIN trip t ON t.trip_id = b.trip_id
WHERE b.trip_id IN (238332615, 238332716)
  AND EXTRACT(DOW FROM b.tstamp) = 0
  AND EXTRACT(HOUR FROM b.tstamp) BETWEEN 9 AND 11
  AND DATE(b.tstamp) = '2023-01-15'
  AND b.latitude IS NOT NULL
  AND b.longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No data found for Q3 visualization.")
else:
    m = folium.Map(location=[df.latitude.mean(), df.longitude.mean()], zoom_start=14)

    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            popup=f"Speed: {row['speed']}",
            color='blue',
            fill=True,
            fill_opacity=0.6
        ).add_to(m)

    m.save("q3_visualization.html")
    print("Saved Q3 visualization as q3_visualization.html")
