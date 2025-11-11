import duckdb
import pandas as pd

def export_to_csv(db_file, table_name, csv_file):
    con = duckdb.connect(db_file, read_only=True)
    
    tables = con.execute("SHOW TABLES").fetchall()
    if ('ads',) not in tables:
        print(f"Table '{table_name}' doesn's exist!")
        con.close()
        return
    
    df = con.execute(f"SELECT * FROM {table_name} ORDER BY \"ID bài đăng\"").fetchdf()
    con.close()
    
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"Exported {len(df)} ads → {csv_file}")
    print(f"Columns ({len(df.columns)}): {list(df.columns)}")