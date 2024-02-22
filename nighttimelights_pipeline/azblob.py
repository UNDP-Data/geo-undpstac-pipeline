import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from nighttimelights_pipeline.const import AZURE_CONTAINER_NAME, AZURE_STORAGE_CONNECTION_STRING
import logging
import math
from tqdm import tqdm
logger = logging.getLogger(__name__)



def get_blob_service_client(conn_str=AZURE_STORAGE_CONNECTION_STRING, container_name=AZURE_CONTAINER_NAME):
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




def upload(
                dst_path: str = None,
                src_path: str = None,
                data: bytes = None,
                content_type: str = None,
                overwrite: bool = True,
                max_concurrency: int = 1
                ):

    try:
        logger.info(f'Uploading {src_path} to {dst_path}')
        _, blob_name = os.path.split(dst_path)

        def _progress_(current, total) -> None:
            logger.info(f'Current {current} vs total {total}')
            progress = current / total * 100
            rounded_progress = int(math.floor(progress))
            logger.info(f'{blob_name} was uploaded - {rounded_progress}%')

        def callback(response):
            current = response.context['upload_stream_current']  # There's also a 'download_stream_current'
            total = response.context['data_stream_total']
            logger.info(f'Current {current} vs total {total}')
            if current is not None:
                progress = current / total * 100
                rounded_progress = int(math.floor(progress))
                logger.info(f'{blob_name} was uploaded - {rounded_progress}%')

        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(blob=dst_path)
        if src_path:
            size = os.path.getsize(src_path)
            #with tqdm.wrapattr(open(src_path, 'rb'), "read", total=size) as dataf:
            with open(src_path, 'rb') as dataf:
                logger.debug(f'Uploading {src_path} to {dst_path}')
                blob_client.upload_blob(
                    data=dataf,
                    overwrite=overwrite,
                    content_settings=ContentSettings(content_type=content_type) if content_type else None,
                    #progress_hook=_progress_,
                    max_concurrency=max_concurrency,
                    raw_response_hook=callback
                )
        elif data:
            logger.debug(f'Uploading bytes/data to {dst_path}')
            blob_client.upload_blob(
                data=data,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=content_type),
                progress_hook=_progress_,
                max_concurrency=max_concurrency

            )
        else:
            raise ValueError("Either 'src_path' or 'data' must be provided.")
    except Exception as e:
        raise e


def upload_to_azure(local_path: str, blob_name: str):
    """
    Upload a file to Azure Blob Storage
    :param local_path: str - As we are using a temporary directory, the file will be deleted after the function is done
    :param blob_name:
    :return:
    """
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(blob=blob_name)
        with open(local_path, "rb") as file:
            total_size = os.path.getsize(local_path)
            with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=blob_name) as pbar:
                # Upload the file in chunks
                logger.info(f"Uploading {local_path} to {blob_name}")
                chunk_size = 4 * 1024 * 1024  # 4MB chunks
                while True:
                    data = file.read(chunk_size)
                    if not data:
                        break
                    blob_client.upload_blob(data, overwrite=True, max_concurrency=8)
                    pbar.update(len(data))
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to {blob_name}: {e}")
        raise e
