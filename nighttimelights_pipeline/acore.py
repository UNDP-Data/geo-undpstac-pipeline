import multiprocessing
import asyncio
from nighttimelights_pipeline.utils import should_download
from nighttimelights_pipeline.colorado_eog import compute_ntl_filename, download_file
from nighttimelights_pipeline.const import DNB_FILE_TYPES, AZURE_DNB_FOLDER, COG_CONVERT_TIMEOUT,AIOHTTP_READ_CHUNKSIZE
import datetime
import logging
import os
import tqdm
from osgeo import gdal
from nighttimelights_pipeline.azblob import upload, set_metadata_in_azure, upload_to_azure


logger = logging.getLogger(__name__)


def gdal_callback(complete, message, data):
    timeout_event, progressbar = data
    progressbar.update(complete * 100)
    if timeout_event and timeout_event.is_set():
        logger.info(f'GDAL was signalled to stop...')
        return 0


def tiff2cog(src_path=None, dst_path=None, timeout_event=None, use_translate=True):
    """
    Concvert a GeoTIFF to COG

    :param src_path: str, input GeoTIDF path
    :param dst_path: str, output COG path
    :param timeout_event:
    :param use_translate:
    :return:
    """

    logger.info(f'Converting {src_path} to COG')
    progressbar = tqdm.tqdm(range(0,100), desc=f'Creating COG {dst_path}', unit_scale=True)
    if use_translate:
        cog_ds = gdal.Translate(
            destName=dst_path,
            srcDS=src_path,
            format='COG',
            creationOptions=[
                "BLOCKSIZE=256",
                "OVERVIEWS=IGNORE_EXISTING",
                "COMPRESS=ZSTD",
                "PREDICTOR = YES",
                "OVERVIEW_RESAMPLING=NEAREST",
                "BIGTIFF=YES",
                "TARGET_SRS=EPSG:3857",
                "RESAMPLING=NEAREST",
            ],

            #projWin=(0, 0, 25, -20),

            callback=gdal_callback,
            callback_data=(timeout_event, progressbar)
        )
    else:
        cog_ds = gdal.Warp(srcDSOrSrcDSTab=src_path, destNameOrDestDS=dst_path,
                        format='COG',
                        creationOptions=[
                            "BLOCKSIZE=256",
                            "OVERVIEWS=IGNORE_EXISTING",
                            "COMPRESS=ZSTD",
                            "PREDICTOR = YES",
                            "OVERVIEW_RESAMPLING=NEAREST",
                            "BIGTIFF=YES",
                            "TARGET_SRS=EPSG:3857",
                            "RESAMPLING=NEAREST",
                        ],
                        #options=gdal.WarpOptions(warpOptions=['NUM_THREADS=ALL_CPUS']), #if this is enabled async mode does not work with CTRL^C
                        callback=gdal_callback,
                        ccallback_data=(timeout_event, progressbar)


                        )

    ctime = datetime.datetime.fromtimestamp(os.path.getctime(src_path)).strftime('%Y%m%d%H%M%S')
    cog_ds.SetMetadata({f"DNB_FILE_SIZE_{ctime}": f"{os.path.getsize(src_path)}" })
    #logger.info(f'Setting COG metadata {cog_ds.GetMetadata()} ')
    del cog_ds




async def process_nighttime_data(date: datetime.datetime = None,
                                 file_type=DNB_FILE_TYPES.DNB,
                                 CONVERT_TIMEOUT=COG_CONVERT_TIMEOUT):
    """

    :param date:
    :param file_type:
    :param CONVERT_TIMEOUT:
    :return:
    """


    year = date.year
    month = date.month
    timeout_event = multiprocessing.Event()

    try:
        remote_dnb_file = compute_ntl_filename(date=date,file_type=file_type)
        _, dnb_file_name = os.path.split(remote_dnb_file)
        cog_blob_path = os.path.join(AZURE_DNB_FOLDER,str(year),f'{month:02d}', dnb_file_name)
        will_download=should_download(blob_name=cog_blob_path,remote_file_url=remote_dnb_file)
        if will_download:
            logger.info(f'Processing nighttime lights from Colorado EOG for {date}')
            local_dnb_file = await download_file(file_url=remote_dnb_file, read_chunk_size=AIOHTTP_READ_CHUNKSIZE)
            logger.info(f'Downloaded {remote_dnb_file} to {local_dnb_file}')
            local_cog_file = os.path.splitext(local_dnb_file)[0]
            cog_task = asyncio.ensure_future(
                asyncio.to_thread(tiff2cog, src_path=local_dnb_file, dst_path=local_cog_file, timeout_event=timeout_event )
            )
            cog_task.set_name('cog')
            done, pending = await asyncio.wait(
                [cog_task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=CONVERT_TIMEOUT,
            )
            if len(done) == 0:
                error_message = f'Failed to convert {local_dnb_file} to COG in {CONVERT_TIMEOUT} seconds.'
                logger.error(error_message)
                timeout_event.set()

            for pending_future in pending:
                try:
                    pending_future.cancel()
                    await pending_future
                except asyncio.exceptions.CancelledError as e:
                    future_name = pending_future.get_name()
                    logger.error()

            for done_future in done:
                try:
                    await done_future
                    logger.info(f'Successfully created DNB COG {local_cog_file}')

                except Exception as e:
                    logger.error(f'done future error {e}')
                    raise

            #upload to azure
            #cog_blob_path = os.path.join(AZURE_DNB_FOLDER, str(year), f'{month:02d}', local_dnb_file)
            start = datetime.datetime.now()

            #upload(dst_path=cog_blob_path,src_path=local_cog_file, content_type='image/tiff', overwrite=True)
            upload_to_azure(local_path=local_cog_file, blob_name=cog_blob_path)
            end = datetime.datetime.now()
            logger.info(end-start)


        else:
            logger.info(f'No nighttime lights data will be processed for {date} from Colorado EOG ')

    except asyncio.CancelledError as ce:
        logger.info(f'Cancelling...')
        timeout_event.set()

    except Exception as e:
        logger.error(f"Failed to process data for {date.strftime('%Y-%m-%d')}: {e}")


def process_historical_nighttime_data(start_date: datetime.datetime, end_date: datetime.datetime):
    """
    Process historical nighttime data
    :param start_date: datetime.datetime
    :param end_date: datetime.datetime
    :return: None
    """
    for date in range((end_date - start_date).days):
        process_nighttime_data(start_date + datetime.timedelta(days=date))

