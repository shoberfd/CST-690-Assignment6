# visualize.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set a nice style for the plots
sns.set_theme(style="whitegrid")

try:
    # Load the data from your Parquet file
    df = pd.read_parquet('data/final_enriched_data.parquet')

    # --- 1. Inspect the Data ---
    print("Successfully loaded data from Parquet file:")
    print(df.to_string()) # .to_string() ensures all columns are displayed

    # --- 2. Visualize the Data (will be more interesting with more rows) ---
    if not df.empty and len(df) > 1:
        print("\nGenerating a plot...")

        # Create a bar plot
        plt.figure(figsize=(10, 6))
        sns.barplot(
            data=df,
            x='measurement_date',
            y='health_risk_index',
            hue='city'
        )

        plt.title('Health Risk Index by Date and City')
        plt.xlabel('Measurement Date')
        plt.ylabel('Health Risk Index')
        plt.xticks(rotation=45)
        plt.tight_layout() # Adjust layout to make room for rotated labels

        # Save the plot as an image file
        plt.savefig('diagrams/health_risk_chart.png')
        print("Plot saved to diagrams/health_risk_chart.png")

    elif not df.empty:
        print("\nPlotting is skipped because there is only one row of data.")

    else:
        print("The Parquet file is empty. No data to show or plot.")

except FileNotFoundError:
    print("Error: The file 'data/final_enriched_data.parquet' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")