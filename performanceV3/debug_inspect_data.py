
import logging
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configure logging
logging.basicConfig(level=logging.INFO)

# --- Configuration (Copied from __init__.py) ---
KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
SECRET_NAME = "azure-storage-account-access-key2"
CONTAINER_NAME = "document-intelligence"
PARQUET_BLOB_PATH = "logs/all_data.parquet"

def inspect_data():
    print("--- Initializing Azure Services ---")
    try:
        credential = DefaultAzureCredential()
        kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        connection_string = kv_client.get_secret(SECRET_NAME).value
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        print("Successfully connected to KeyVault and Blob Service.")
    except Exception as e:
        print(f"CRITICAL: Failed to initialize Azure services: {e}")
        return

    print(f"--- Downloading {PARQUET_BLOB_PATH} ---")
    try:
        blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, PARQUET_BLOB_PATH)
        if not blob_client.exists():
             print("Parquet file not found at specified path.")
             return
        
        # Download and read into DataFrame
        stream = io.BytesIO(blob_client.download_blob().readall())
        df = pd.read_parquet(stream)
        
        print("\n--- Data Loaded Successfully ---")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\n--- Column Types ---")
        print(df.dtypes)
        
        print("\n--- First 5 Rows ---")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df.head(5))
        
        print("\n--- Sample of 'HISTORY_STATUS' values ---")
        if 'HISTORY_STATUS' in df.columns:
            print(df['HISTORY_STATUS'].unique()[:20])
            
        print("\n--- Sample of 'USERCODE' values ---")
        if 'USERCODE' in df.columns:
            print(df['USERCODE'].dropna().unique()[:20])
            
    except Exception as e:
        print(f"Error reading or processing parquet file: {e}")

if __name__ == "__main__":
    inspect_data()
