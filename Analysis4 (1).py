import mysql.connector
import pandas as pd
import geopandas as gpd
import folium

# Database Configuration
db_config = {
    "host": "localhost306",
    "user": "root",
    "password": "sahidfaisal@03",
    "database": "employee_data"
}

try:
    print("⏳ Step 1: Connecting to database...")
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)  # Fetch data as dictionary

    print("✅ Database connected!")

    # Fetch GIS data (based on `location`)
    print("📊 Fetching GIS data...")
    query = """
        SELECT meter_id, timestamp, consumption, location
        FROM ami_data
        WHERE location IS NOT NULL;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        print("⚠️ No GIS data found. Please update `location` values in `ami_data`.")
    else:
        print(f"✅ Retrieved {len(rows)} records!")

        # Convert to DataFrame
        df = pd.DataFrame(rows)

        # Check unique locations
        unique_locations = df["location"].unique()
        print(f"📍 Found {len(unique_locations)} unique locations.")

        # Create a base map
        print("🗺️ Generating consumption map...")
        map_center = [8.5, -13.2]  # Default center (adjust based on your data)
        m = folium.Map(location=map_center, zoom_start=10)

        # Group by location and sum consumption
        location_data = df.groupby("location")["consumption"].sum().reset_index()

        # Add locations to the map
        for _, row in location_data.iterrows():
            folium.Marker(
                location=[8.5, -13.2],  # Placeholder coordinates (update manually)
                popup=f"Location: {row['location']}<br>Total Consumption: {row['consumption']} kWh",
                icon=folium.Icon(color="blue"),
            ).add_to(m)

        # Save Map
        map_filename = "consumption_map.html"
        m.save(map_filename)
        print(f"✅ GIS Analysis Completed! Map saved as `{map_filename}`")

except mysql.connector.Error as e:
    print(f"❌ Database Error: {e}")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("🔒 Database connection closed.")
