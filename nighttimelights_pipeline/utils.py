import os
import math
import uuid as _uu
import requests
from nighttimelights_pipeline.azblob import blob_exists_in_azure
from tqdm import tqdm
import logging



logger = logging.getLogger(__name__)
def download_http_resurce(url: str=None, save_path: str=None, timeout=(25, 250)):
    """
    Download nighttime data
    :param url: str
    :param save_path: str
    :return: None
    """
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()  # raise an exception if the request fails.
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True)

    with open(save_path, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size != 0 and progress_bar.n != total_size:
        raise ValueError("Download failed")
    logger.info(f"Downloaded {url} to {save_path}")



def fetch_resource_size(url=None):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # raise an exception if the request fails.
    return int(response.headers.get('content-length', 0))


def should_download(blob_name: str=None, remote_file_url: str=None) -> bool:
    if not blob_exists_in_azure(blob_name):
        return True
    else:
        # remote file size
        remote_size = fetch_resource_size(url=remote_file_url)
        # azure file size
        #azure_size = get_blob_metadata_from_azure(blob_name)['raw_file_downloaded_size']
        azure_size = 0
        if remote_size != azure_size:
            return True
        else:
            return False


def get_cog_metadata(blob_path=None):
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    assert conn_str not in []
    pass


def generate_id(name=None, pad_length=None):
    """
    Generate and return a UUID.

    If the name parameter is provided, set the namespace to the provided
    name and generate a UUID.
    """
    __alphabet__ = list("23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    __alphabetlen__ = int(math.ceil(math.log(2 ** 128, len(__alphabet__))))
    if pad_length is None:
        pad_length = __alphabetlen__

    # If no name is given, generate a random UUID.
    if name is None:
        uuid = _uu.uuid4()
    elif "http" not in name.lower():
        uuid = _uu.uuid5(_uu.NAMESPACE_DNS, name)
    else:
        uuid = _uu.uuid5(_uu.NAMESPACE_URL, name)

    return str(uuid)