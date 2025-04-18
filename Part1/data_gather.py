import requests
import datetime
import os
import shutil

# Create a directory to store JSON files if it doesn't exist
output_folder = "bus_data"
os.makedirs(output_folder, exist_ok=True)

# Load vehicle IDs from a file
script_dir = os.path.dirname(os.path.abspath(__file__))
vehicle_ids_path = os.path.join(script_dir, "vehicle_ids.txt")
with open(vehicle_ids_path) as f:
    vehicle_ids = [line.strip() for line in f]


today = datetime.date.today().isoformat()  # e.g., '2025-04-09'

# Fetch data for each vehicle and save to JSON files
for vid in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Define the output file path within the output folder
        file_path = os.path.join(output_folder, f"bus_{vid}_{today}.json")

        # Save to file
        with open(file_path, "w") as out:
            out.write(response.text)

        print(f"Saved: {file_path} ({len(data)} records)")

    except Exception as e:
        print(f"Error fetching {vid}: {e}")

# Check if the folder exists
if os.path.exists(output_folder):
    print("Folder found:", output_folder)
else:
    print("Folder does not exist. Check your earlier script or re-run the download.")

# Create a ZIP archive of the folder
shutil.make_archive(f"bus_data_{today}", 'zip', output_folder)

# Optional: If you want to move the zip file to a specific directory, you can do so here
# shutil.move(f"bus_data_{today}.zip", '/path/to/your/desired/directory')

# You can download or access the archive manually, as the code will run locally or on a VM
print(f"ZIP archive created: bus_data_{today}.zip")
