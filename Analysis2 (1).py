import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Database Configuration
db_config = {
    "host": "localhost306",
    "user": "root",
    "password": "sahidfaisal@03",
    "database": "employee_data"
}

def read_billing_data_from_database():
    """
    Read billing summary data from the ami_data table in the database.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        query = """
            SELECT location, season, meter_type, SUM(billing) as bill_amount
            FROM ami_data
            GROUP BY location, season, meter_type;
        """
        df = pd.read_sql(query, conn)
        return df
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()
    finally:
        if conn.is_connected():
            conn.close()

# Load the billing summary data from the database
billing_summary = read_billing_data_from_database()

# Check if billing_summary is empty
if billing_summary.empty:
    print("⚠️ No data available in the billing summary.")
else:
    # Print the DataFrame to check the data
    print("Billing Summary DataFrame:")
    print(billing_summary)

    # 1. Total Revenue Calculation
    total_revenue = billing_summary['bill_amount'].sum()
    print(f"✅ Total Revenue: Le {total_revenue:.2f}")

    # 2. Billing Amount Distribution by Location (Histogram)
    plt.figure(figsize=(10, 5))
    sns.histplot(data=billing_summary, x='bill_amount', hue='location', kde=True, multiple="stack", palette='Set2')
    plt.title('Billing Amount Distribution by Location')
    plt.xlabel('Billing Amount (Le)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # 3. Billing Amount Distribution by Season (Histogram)
    plt.figure(figsize=(10, 5))
    sns.histplot(data=billing_summary, x='bill_amount', hue='season', kde=True, multiple="stack", palette='Set1')
    plt.title('Billing Amount Distribution by Season')
    plt.xlabel('Billing Amount (Le)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # 4. Billing Amount Distribution by Meter Type (Histogram)
    plt.figure(figsize=(10, 5))
    sns.histplot(data=billing_summary, x='bill_amount', hue='meter_type', kde=True, multiple="stack", palette='Set3')
    plt.title('Billing Amount Distribution by Meter Type')
    plt.xlabel('Billing Amount (Le)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # 5. Top 10 Customers by Billing Amount (Bar Chart)
    top_customers = billing_summary.groupby('meter_type')['bill_amount'].sum().reset_index()
    top_customers = top_customers.sort_values(by='bill_amount', ascending=False).head(10)

    # Plotting the top 10 customers by billing amount
    plt.figure(figsize=(12, 6))
    sns.barplot(x='meter_type', y='bill_amount', data=top_customers, palette='viridis')
    plt.title('Top 10 Customers by Billing Amount')
    plt.xlabel('Meter Type')
    plt.ylabel('Total Billing Amount (Le)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()