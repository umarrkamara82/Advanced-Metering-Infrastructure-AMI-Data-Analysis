import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Database Configuration
db_config = {
    "host": "localhost306",
    "user": "root",
    "password": "sahidfaisal@03",
    "database": "employee_data"
}

try:
    print("🔄 Connecting to database...")
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    print("✅ Connection successful!")

    # Fetch Data
    print("📊 Fetching consumption data...")
    query = """
        SELECT meter_id, SUM(consumption) as total_consumption
        FROM ami_data
        GROUP BY meter_id;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        print("⚠️ No Data Found!")
    else:
        print(f"✅ Retrieved {len(rows)} meter records!")

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['meter_id', 'total_consumption'])

        # Normalize data for clustering
        scaler = StandardScaler()
        df['scaled_consumption'] = scaler.fit_transform(df[['total_consumption']])

        # Determine the number of clusters using the Elbow Method
        print("📌 Finding optimal clusters...")
        distortions = []
        K = range(1, 10)
        for k in K:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(df[['scaled_consumption']])
            distortions.append(kmeans.inertia_)

        # Plot Elbow Method
        plt.figure(figsize=(8, 5))
        plt.plot(K, distortions, marker='o')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Distortion')
        plt.title('Elbow Method for Optimal K')
        plt.show()

        # Apply K-Means Clustering (choose K=3 for simplicity)
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df['cluster'] = kmeans.fit_predict(df[['scaled_consumption']])

        # Visualize Clusters
        print("📊 Plotting cluster results...")
        plt.figure(figsize=(8, 5))
        sns.scatterplot(x=df['meter_id'], y=df['total_consumption'], hue=df['cluster'], palette='viridis')
        plt.xlabel('Meter ID')
        plt.ylabel('Total Consumption')
        plt.title('Consumption Clustering')
        plt.show()

        # Save Results
        df.to_csv("consumption_clusters.csv", index=False)
        print("✅ Clustering analysis completed and saved as consumption_clusters.csv!")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("🔒 Connection closed.")
