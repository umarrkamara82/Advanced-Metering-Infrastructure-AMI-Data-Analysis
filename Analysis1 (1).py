import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import zscore

# Database Connection Configuration
db_config = {
    "host": "localhost306",
    "user": "root",
    "password": "sahidfaisal@03",
    "database": "employee_data"  # Ensure this matches your actual database name
}

def create_database_if_not_exists():
    """
    Create the employee-data database if it does not exist.
    """
    conn = mysql.connector.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"]
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS `employee-data`;")
    conn.commit()
    cursor.close()
    conn.close()

def create_table_if_not_exists():
    """
    Create the ami_data table if it does not exist.
    """
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ami_data (
            meter_id BIGINT NOT NULL,
            timestamp DATETIME NOT NULL,
            location VARCHAR(50) NOT NULL,
            season VARCHAR(10) NOT NULL,
            meter_type VARCHAR(50) NOT NULL,
            consumption DECIMAL(10, 2) NOT NULL,
            billing DECIMAL(10, 2) NOT NULL,
            is_downtime TINYINT(1) NOT NULL,
            PRIMARY KEY (meter_id, timestamp)
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def read_data_from_database(limit=1000):
    """
    Read data from the ami_data table in the database.
    """
    try:
        # Establish the connection
        conn = mysql.connector.connect(**db_config)
        query = f"SELECT meter_id, timestamp, location, season, meter_type, consumption, billing, is_downtime FROM ami_data LIMIT {limit}"
        df = pd.read_sql(query, conn)  # Use the MySQL connection
        return df
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()  # Return an empty DataFrame on error
    finally:
        if conn.is_connected():
            conn.close()  # Ensure the connection is closed

# Call the functions to create the database and table
create_database_if_not_exists()
create_table_if_not_exists()

# Read data directly from the database
df = read_data_from_database()

# Convert timestamp to datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Perform analysis and visualization
# Example: Display the first few rows of the dataframe
print(df.head())

# Step 1: Flag database anomalies (downtime cases)
df['db_anomaly'] = df['is_downtime'] == 1  # Boolean flag

# Step 2: Compute statistical anomalies
df['z_score'] = df.groupby('meter_type')['consumption'].transform(lambda x: zscore(x, nan_policy='omit'))
df['iqr'] = df.groupby('meter_type')['consumption'].transform(lambda x: x.quantile(0.75) - x.quantile(0.25))

# Set thresholds for anomalies
z_threshold = 3  # Flag readings with a Z-score > 3
iqr_threshold = 1.5  # Flag readings beyond IQR*1.5

df['stat_anomaly'] = (
    (df['z_score'].abs() > z_threshold) |
    (df['consumption'] < df.groupby('meter_type')['consumption'].transform(lambda x: x.quantile(0.25) - iqr_threshold * df['iqr'])) |
    (df['consumption'] > df.groupby('meter_type')['consumption'].transform(lambda x: x.quantile(0.75) + iqr_threshold * df['iqr']))
)

# Combine both anomaly detections
df['final_anomaly'] = df['db_anomaly'] | df['stat_anomaly']

# Step 3: Visualization

# Plot consumption over time with anomalies highlighted
plt.figure(figsize=(12, 6))
plt.plot(df['timestamp'], df['consumption'], color='blue', alpha=0.5, label='Normal Data')
plt.scatter(df[df['final_anomaly']]['timestamp'], df[df['final_anomaly']]['consumption'], color='red', label='Anomalies')
plt.xlabel("Timestamp")
plt.ylabel("Consumption (kWh)")
plt.title("Anomaly Detection in AMI Data")
plt.legend()
plt.show()

# Boxplot to visualize outliers per meter type
plt.figure(figsize=(10, 5))
sns.boxplot(x='meter_type', y='consumption', data=df)
plt.title("Consumption Distribution by Meter Type")
plt.show()

# Histogram to check consumption spread
plt.figure(figsize=(10, 5))
sns.histplot(df['consumption'], bins=50, kde=True)
plt.title("Consumption Distribution")
plt.show()

# Print summary
print("Total records:", len(df))
print("Anomalies detected:", df['final_anomaly'].sum())
print("Percentage of anomalies:", round(df['final_anomaly'].mean() * 100, 2), "%")


