import os
from azure.storage.blob import BlobServiceClient
from nighttimelights_pipeline.const import AZURE_CONTAINER_NAME
AZURE_CONN_STR = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')


def get_blob_service_client(conn_str=AZURE_CONN_STR, container_name=AZURE_CONTAINER_NAME):
    return BlobServiceClient.from_connection_string(conn_str).get_container_client(container_name)


def blob_exists_in_azure(blob_name:str=None):
    """
    Check if a blob exists in Azure
    :param blob_name: str
    :return: bool
    """

    blob_service_client=get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(blob_name)
    return blob_client.exists()


def download_blob_from_azure(blob_name: str, local_path: str):
    """
    Download a blob from Azure
    :param blob_name: str
    :param local_path: str
    :return: None
    """
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(blob=blob_name)
        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
    except Exception as e:
        raise e


def set_metadata_in_azure(blob_name: str, metadata: dict):
    """
    Set metadata in an Azure blob
    :param blob_name: str
    :param metadata: dict
    :return: None
    """
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(blob=blob_name)
        blob_client.set_blob_metadata(metadata)
    except Exception as e:
        raise e


def get_blob_metadata_from_azure(blob_name: str):
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client( blob=blob_name)
        return blob_client.get_blob_properties()
    except Exception as e:
        raise e