import os
import urllib
import logging
import aiohttp
import aiofiles
import traceback
import tqdm

logger = logging.getLogger(__name__)


async def download_file(file_url=None, no_attempts=3, connect_timeout=5, data_read_timeout=900,
                        read_chunk_size=1024 * 10):
    """
    https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/SVDNB_npp_d20240206.rade9d_sunfiltered.tif
    :param file_url:
    :param no_attempts:
    :return:
    """
    try:
        parse_res = urllib.parse.urlparse(str(file_url))
        _, file_name = os.path.split(parse_res.path)
        local_file_path = f'/tmp/{file_name}'
        for attempt in range(no_attempts):
            if attempt > 0:
                logger.info(f'Attempt no {attempt} to download {file_url}')

                try:
                    timeout = aiohttp.ClientTimeout(connect=connect_timeout,
                                                    sock_read=data_read_timeout)

                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(file_url, timeout=data_read_timeout) as response:
                            if response.status == 200:
                                # For large files use response.content.read(chunk_size) instead.

                                remote_size = int(response.headers['Content-Length'])
                                progressbar = tqdm.tqdm(total=remote_size, desc=local_file_path, unit='iB',
                                                        unit_scale=True)

                                async with aiofiles.open(local_file_path, 'wb') as local_file:
                                    while True:
                                        chunk = await response.content.read(read_chunk_size)
                                        if not chunk:
                                            break
                                        await local_file.write(chunk)
                                        progressbar.update(len(chunk))

                                size = os.path.getsize(local_file_path)
                                if size == 0:
                                    raise Exception(f'The downloaded file is empty!')

                                logger.debug(f'File {local_file_path} was successfully downloaded')
                                return local_file_path
                            elif response.status == 404:
                                # 404 means that the connection and request were fine, but that the file requested
                                # was not available.  In this case there is no point in trying again.
                                # This is included for downloaders that created their url list procedurally, so it
                                # can contain files that are not yet available on the server.
                                logger.error(f"GET request failed for url {file_url}, with status code 404 in"
                                             f" attempt {attempt}. No new attempts will be made to download "
                                             f"this file. \n Response: {response.text}")
                                break
                            else:
                                logger.error(f'GET request failed for url {file_url} with status code '
                                             f'{response.status} in attempt {attempt}.')
                                continue

                except Exception as e:
                    traceback.print_exc()
                    logger.debug(f'Exception {e.__class__} in attempt {attempt}')
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)
                    if attempt == no_attempts - 1:
                        logger.exception(f'Failed to download {file_url}.')
                        return file_url, e
                    continue

    except Exception as fe:
        logger.exception(f'Failed to download {file_url}.')
        return file_url, fe


if __name__ == '__main__':
    import asyncio

    file_url = 'https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/SVDNB_npp_d20240206.rade9d_sunfiltered.tif'
    asyncio.run(download_file(file_url=file_url))
