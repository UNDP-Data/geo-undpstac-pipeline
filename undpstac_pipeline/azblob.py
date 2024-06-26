import os
from azure.storage.blob import BlobServiceClient, ContentSettings, ContainerClient
from undpstac_pipeline.const import AZURE_CONTAINER_NAME, AZURE_STORAGE_CONNECTION_STRING
import logging
from tqdm import tqdm
logger = logging.getLogger(__name__)



def get_container_client(conn_str=AZURE_STORAGE_CONNECTION_STRING, container_name=AZURE_CONTAINER_NAME):
    return BlobServiceClient.from_connection_string(conn_str).get_container_client(container_name)


def blob_exists_in_azure(blob_path:str=None, container_client=None):
    """
    Check if a blob exists in Azure
    :param blob_name: str
    :return: bool
    """
    try:
        local_container_client = container_client if container_client else get_container_client()
        blob_client = local_container_client.get_blob_client(blob_path)
        exists = blob_client.exists()
        if exists:
            return True, blob_client.url
        else:
            return False, None
    finally:
        if not container_client:
            local_container_client.close()



def upload_file_to_blob(
                dst_path: str = None,
                src_path: str = None,
                content_type: str = None,
                overwrite: bool = True,
                max_concurrency: int = 8,
                container_client:ContainerClient = None,
                ):

    _, blob_name = os.path.split(dst_path)
    local_container_client = container_client if container_client else get_container_client()
    blob_client = local_container_client.get_blob_client(blob=dst_path)

    size = os.path.getsize(src_path)
    with tqdm.wrapattr(open(src_path, 'rb'), "read", total=size, desc=f'Uploading {blob_name}') as dataf:
        #with open(src_path, 'rb') as dataf:
            logger.debug(f'Uploading {src_path} to {dst_path}')
            blob_client.upload_blob(
                data=dataf,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=content_type) if content_type else None,
                max_concurrency=max_concurrency,
                connection_timeout=300

            )


def upload(
                dst_path: str = None,
                src_path: str = None,
                data: bytes = None,
                content_type: str = None,
                overwrite: bool = True,
                max_concurrency: int = 1,
                container_client:ContainerClient = None,
                no_attempts=3
                ):

    try:
        for attempt in range(no_attempts):
            try:
                _, blob_name = os.path.split(dst_path)
                local_container_client = container_client if container_client else get_container_client()
                blob_client = local_container_client.get_blob_client(blob=dst_path)
                if src_path:
                    size = os.path.getsize(src_path)
                    with tqdm.wrapattr(open(src_path, 'rb'), "read", total=size, desc=f'Uploading {blob_name}') as dataf:
                        logger.debug(f'Uploading {src_path} to {dst_path}')
                        blob_client.upload_blob(
                            data=dataf,
                            overwrite=overwrite,
                            content_settings=ContentSettings(content_type=content_type) if content_type else None,
                            max_concurrency=max_concurrency,

                        )

                elif data:
                    logger.debug(f'Uploading bytes/data to {dst_path}')
                    blob_client.upload_blob(
                        data=data,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        #progress_hook=_progress_,
                        max_concurrency=max_concurrency

                    )
                else:
                    raise ValueError("Either 'src_path' or 'data' must be provided.")
                return
            except Exception as e:
                if attempt == no_attempts - 1:
                    raise e
                logger.info(f'Failed to upload {src_path or "data"} in attempt no {attempt + 1}. Trying again ...')
                continue
    finally:
        if not container_client:
            local_container_client.close()

def download(blob_path: str = None, dst_path: str = None, container_client: ContainerClient =None) -> str:
    """
    Downloads a file from Azure Blob Storage and returns its data or saves it to a local file.

    Args:
        blob_path (str, optional): The name of the blob to download. Defaults to None.
        dst_path (str, optional): The local path to save the downloaded file. If not provided, the file data is returned instead of being saved to a file. Defaults to None.

    Returns:
        bytes or None: The data of the downloaded file, or None if a dst_path argument is provided.
    """
    try:
        logger.debug(f'Downloading {blob_path}')
        local_container_client = container_client if container_client else get_container_client()

        blob_client = local_container_client.get_blob_client(blob=blob_path)
        chunk_list = []
        stream = blob_client.download_blob()
        for chunk in stream.chunks():
            chunk_list.append(chunk)

        data = b"".join(chunk_list)
        logger.debug(f'Finished downloading {blob_path}')
        if dst_path:
            with open(dst_path, "wb") as f:
                f.write(data)
            return None
        else:
            return data.decode('utf-8')
    finally:
        if not container_client:local_container_client.close()