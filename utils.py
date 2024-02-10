import os
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()


def blob_exists_in_azure(blob_name):
    """
    Check if a blob exists in Azure
    :param blob_name: str
    :return: bool
    """
    container_client = BlobServiceClient.from_connection_string(
        os.getenv('AZURE_STORAGE_CONNECTION_STRING')).get_container_client('geo-nightlights')
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()


def download_blob_from_azure(blob_name: str, local_path: str):
    """
    Download a blob from Azure
    :param blob_name: str
    :param local_path: str
    :return: None
    """
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container='geo-nightlights', blob=blob_name)
        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
    except Exception as e:
        raise e

# if __name__ == "__main__":
#     print(blob_exists_in_azure('data/raw/SVDNB_npp_d20240201.rade9d_sunfiltered.tif'))
