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

ROUTE_ID = 35
DATE = '2023-01-15'

query = f"""
SELECT b.latitude, b.longitude, b.speed
FROM breadcrumb b
JOIN trip t ON b.trip_id = t.trip_id
WHERE t.route_id = {ROUTE_ID}
  AND DATE(b.tstamp) = '{DATE}'
  AND b.latitude IS NOT NULL
  AND b.longitude IS NOT NULL;
"""

df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print(" No data found for Visualization 5a.")
else:
    
    m = folium.Map(location=[df.latitude.mean(), df.longitude.mean()], zoom_start=13)

    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            popup=f"Speed: {row['speed']}",
            color='blue',
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

    
    output_file = "q5a_route20_20230126.html"
    m.save(output_file)
    print(f"Saved {output_file}")
