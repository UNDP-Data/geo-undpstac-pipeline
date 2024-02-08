import os
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def blob_exists_in_azure(blob_name):
    container_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING')).get_container_client('geo-nightlights')
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()


# if __name__ == "__main__":
#     print(blob_exists_in_azure('data/raw/SVDNB_npp_d20240201.rade9d_sunfiltered.tif'))