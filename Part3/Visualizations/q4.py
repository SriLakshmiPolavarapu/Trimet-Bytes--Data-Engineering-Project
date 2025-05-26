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
WHERE DATE(tstamp) = '2023-01-15'
  AND EXTRACT(HOUR FROM tstamp) < 11
  AND latitude BETWEEN 45.503 AND 45.514
  AND longitude BETWEEN -122.655 AND -122.643
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No data found for Visualization 4.")
else:
    m = folium.Map(location=[45.508537, -122.649434], zoom_start=14)
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            popup=f"Speed: {row['speed']}",
            color='blue',
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

    m.save("q4_visualize_laddcircle.html")
    print("Saved map as q4_visualize_laddcircle.html")
