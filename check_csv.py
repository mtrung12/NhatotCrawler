import pandas as pd

def check_csv(csv_file):
    try:
        df = pd.read_csv(csv_file)
        print(f"CSV file: {csv_file}")
        print(f"Shape: {df.shape}")
        print("Columns:", df.columns.tolist())
        print("First few rows:")
        print(df.head())
    except Exception as e:
        print(f"Error checking CSV: {e}")

if __name__ == "__main__":
    check_csv('bdsonline_export.csv')
    # Or check 'test.csv' if needed