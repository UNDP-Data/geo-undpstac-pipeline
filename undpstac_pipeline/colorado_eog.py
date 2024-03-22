import datetime
import os
import asyncio
import logging
import aiohttp
import aiofiles
import tqdm
import urllib
from undpstac_pipeline import const
from typing import Dict, Literal




logger = logging.getLogger(__name__)




def compute_dnb_filename(date=None, file_type='DNB' ):
    assert file_type in const.FILE_TYPE_NAMES, f'invalid file_type={file_type}. Valid values are {",".join(const.FILE_TYPE_NAMES)}'
    folder = const.FILE_TYPE_FOLDERS[file_type]
    file_name = const.FILE_TYPE_TEMPLATES[file_type].format(date=date.strftime('%Y%m%d'))
    return os.path.join(const.ROOT_EOG_URL,folder,file_name)

def get_dnb_files(date=None, file_type=const.DNB_FILE_TYPES.DNB) -> Dict[Literal[str], tuple]:
    """
    COmpute and return the name of files to download
    :param date: the date for whcig to compute the names
    :param file_type: the VIIRS DNB file type (regular or sunfiltered)
    :return: dict where key is file type and value is a tuple holding file url and descr
    """
    remote_dnb_file = compute_dnb_filename(date=date, file_type=file_type)
    file_type_desc = const.DNB_FILE_TYPES_DESC[file_type.value]
    dnb_file_desc = f'{file_type_desc.value} for {date}'
    remote_dnb_cloudmask_file = compute_dnb_filename(date=date, file_type=const.DNB_FILE_TYPES.CLOUD_COVER)
    dnb_cloudmask_desc = f'{const.DNB_FILE_TYPES_DESC.CLOUD_COVER.value} for {date}'

    return {
            file_type.value :  (remote_dnb_file,dnb_file_desc),
             const.DNB_FILE_TYPES.CLOUD_COVER.value  : (remote_dnb_cloudmask_file, dnb_cloudmask_desc)
    }

async def download_file(file_url=None, no_attempts=3, connect_timeout=250, data_read_timeout=9000,
                        read_chunk_size=1024 * 10):
    """
    https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/SVDNB_npp_d20240206.rade9d_sunfiltered.tif
    :param file_url:
    :param no_attempts:
    :return:
    """
    try:
        parse_res = urllib.parse.urlparse(file_url)
        pth, __ = os.path.split(file_url)
        _, orig_file_name = os.path.split(parse_res.path)
        file_name = f'{orig_file_name}.local'
        dst_file_path = f'/tmp/{file_name}'

        timeout = aiohttp.ClientTimeout(connect=connect_timeout, sock_read=data_read_timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for attempt in range(no_attempts):
                logger.info(f'Attempt no {attempt+1} to download {orig_file_name} from {pth}')
                try:
                    async with session.get(file_url, timeout=data_read_timeout) as response:
                        if response.status == 200:
                            remote_size = int(response.headers['Content-Length'])
                            if os.path.exists(dst_file_path):
                                return dst_file_path
                                if os.path.getsize(dst_file_path) == remote_size:
                                    logger.debug(f'Returning local file {dst_file_path}')
                                    return dst_file_path
                                else:
                                    os.remove(dst_file_path)

                            progressbar = tqdm.tqdm(total=remote_size, desc=f'Downloading {dst_file_path}', unit='iB',
                                                    unit_scale=True)
                            async with aiofiles.open(dst_file_path, 'wb') as local_file:
                                while True:
                                    chunk = await response.content.read(read_chunk_size)
                                    if not chunk:
                                        break
                                    await local_file.write(chunk)
                                    progressbar.update(len(chunk))

                            size = os.path.getsize(dst_file_path)
                            if size != remote_size:
                                raise Exception(f'{file_url} is was not downloaded correctly!')

                            logger.debug(f'File {dst_file_path} was successfully downloaded')
                            return dst_file_path
                        elif response.status == 404:
                            # 404 means that the connection and request were fine, but that the file requested
                            # was not available.  In this case there is no point in trying again.
                            # This is included for downloaders that created their url list procedurally, so it
                            # can contain files that are not yet available on the server.
                            msg = f"GET request failed for url {file_url}, with status code 404 in" \
                                         f" attempt {attempt}. "
                            if os.path.exists(dst_file_path):
                                os.remove(dst_file_path)
                            raise Exception(msg)
                        else:
                            msg = f'GET request failed for url {file_url} with status code {response.status}.'
                            raise Exception(msg)
                except asyncio.CancelledError as ce:
                    logger.error(
                        f'Download action for {file_url} was cancelled by the user')
                    if os.path.exists(dst_file_path):
                        os.remove(dst_file_path)
                    raise ce
                except Exception as e:
                    logger.error(f'Exception "{e}" was encountered in while downloading {file_url}')
                    if os.path.exists(dst_file_path):
                        os.remove(dst_file_path)
                    if attempt == no_attempts - 1:
                        raise e
                    continue

    except Exception as fe:
        if os.path.exists(dst_file_path):
            os.remove(dst_file_path)
        raise fe



if __name__ == '__main__':
    import asyncio
    today = datetime.datetime.now().date()
    a_date = today-datetime.timedelta(days=5)

    file_url = 'https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/SVDNB_npp_d20240206.rade9d_sunfiltered.tif'
    #asyncio.run(download_file(file_url=file_url))
