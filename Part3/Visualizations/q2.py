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

trip_id = '238327769'

query = f"""
SELECT longitude, latitude, speed 
FROM breadcrumb 
WHERE trip_id = (
  SELECT DISTINCT b.trip_id
  FROM breadcrumb b 
  JOIN trip t ON b.trip_id = t.trip_id
  WHERE t.route_id = 20
    AND EXTRACT(HOUR FROM b.tstamp) BETWEEN 16 AND 18
    AND DATE(b.tstamp) = '2023-01-26'
  LIMIT 1
);

"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No data found for Visualization 2")
else:
    m = folium.Map(location=[df.latitude.mean(), df.longitude.mean()], zoom_start=13)
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            color='blue',
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

    m.save("q2_visualize.html")
    print("Saved q2_visualize.html")
