import pandas as pd
from logger import log

def check_csv(csv_file):
    try:
        df = pd.read_csv(csv_file)
        log.info(f"CSV file: {csv_file}")
        log.info(f"Shape: {df.shape}")
        log.info("Columns:", df.columns.tolist())
        log.info("First few rows:")
        log.info(df.head())
    except Exception as e:
        log.error(f"Error checking CSV: {e}")

if __name__ == "__main__":
    check_csv('bdsonline_export.csv')
    # Or check 'test.csv' if needed