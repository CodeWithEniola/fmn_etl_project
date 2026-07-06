import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def create_schema_if_not_exists(schema_name):
    """
    Create a schema in PostgreSQL if it doesn't already exist.
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        conn.commit()
    print(f"Schema '{schema_name}' ensured to exist.")

def ingest_data_to_raw(file_path, sheet_name, table_name=None, schema='raw'):
    """
    Read a sheet from an Excel file and load it into a PostgreSQL raw table.
    If table_name is not provided, it prefixes 'raw_' to the sheet_name.
    """
    # ✅ Ensure the schema exists before loading
    create_schema_if_not_exists(schema)
    
    if table_name is None:
        table_name = f"raw_{sheet_name}"
    
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '', regex=False).str.replace(')', '', regex=False)
    
    # ✅ Full refresh: replace the table if it exists
    df.to_sql(table_name, engine, if_exists='replace', index=False, schema=schema)
    print(f"Data from sheet '{sheet_name}' in {file_path} ingested into table '{schema}.{table_name}'.")
    return len(df)

def ingest_all_sheets(file_path, sheets=None):
    """
    Ingest all sheets from an Excel file.
    """
    if sheets is None:
        sheets = ['Transactions', 'Products', 'Distributors', 
                  'Salespersons', 'Monthly_Targets', 'Date_Table']
    
    results = {}
    for sheet in sheets:
        row_count = ingest_data_to_raw(file_path, sheet)
        results[sheet] = row_count
    
    print(f"✅ Ingested {len(sheets)} sheets: {results}")
    return results

# if __name__ == "__main__":
#     # ✅ Use the correct path for your folder structure
#     #data_dir = os.getenv('DATA_PATH', './dags/dataset')
#     #excel_file = os.path.join(data_dir, 'FMN Data Engineer Assesment Dataset.xlsx')
    
#     excel_file = "./FMN Data Engineer Assesment Dataset.xlsx"  #Testing path, adjust as needed for your environment

#     # Check if file exists
#     if not os.path.exists(excel_file):
#         print(f"❌ ERROR: Excel file not found at: {excel_file}")
#         print("Please check DATA_PATH in .env file")
#         exit(1)
    
#     sheets = ['Transactions', 'Products', 'Distributors', 
#               'Salespersons', 'Monthly_Targets', 'Date_Table']
    
#     ingest_all_sheets(excel_file, sheets)
#     print("✅ All data ingestion to raw tables completed successfully.")