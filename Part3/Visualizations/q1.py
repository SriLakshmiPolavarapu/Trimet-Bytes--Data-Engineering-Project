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
SELECT longitude, latitude, speed 
FROM breadcrumb 
WHERE trip_id = (
    SELECT b.trip_id
    FROM breadcrumb b
    INNER JOIN trip t ON t.trip_id = b.trip_id
    WHERE b.latitude BETWEEN 45.506022 AND 45.516636
      AND b.longitude BETWEEN -122.711662 AND -122.700316
      AND t.route_id > 0
    ORDER BY DATE(b.tstamp) DESC
    LIMIT 1
)
AND latitude IS NOT NULL AND longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No data found for Q1 visualization.")
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

    m.save("q1_visualize.html")
    print("Saved q1_visualize.html")
