import multiprocessing
import asyncio
from nighttimelights_pipeline.utils import should_download
from nighttimelights_pipeline.colorado_eog import get_dnb_files, download_file
from nighttimelights_pipeline.const import DNB_FILE_TYPES, AZURE_DNB_COLLECTION_FOLDER, COG_CONVERT_TIMEOUT,AIOHTTP_READ_CHUNKSIZE, COG_DOWNLOAD_TIMEOUT
import datetime
import logging
import os
import tqdm
from osgeo import gdal
from nighttimelights_pipeline.validate import validate
from nighttimelights_pipeline.azblob import upload
from nighttimelights_pipeline.stac import update_undp_stac


gdal.UseExceptions()

logger = logging.getLogger(__name__)


def gdal_callback(complete, message, data):
    timeout_event, progressbar = data
    progressbar.update(complete * 100)
    if timeout_event and timeout_event.is_set():
        logger.info(f'GDAL was signalled to stop...')
        return 0


def tiff2cog(src_path=None, dst_path=None, timeout_event=None, use_translate=True, description=None,
             lonmin=-180, latmin=-65, lonmax=180, latmax=75):
    """
    Convert a GeoTIFF to COG

    :param src_path: str, input GeoTIDF path
    :param dst_path: str, output COG path
    :param timeout_event:
    :param use_translate:
    :return:
    """

    logger.info(f'Converting {src_path} to COG')
    if os.path.exists(dst_path):os.remove(dst_path)
    logger.info(f'Setting custom metadata ')
    ctime = datetime.datetime.fromtimestamp(os.path.getctime(src_path)).strftime('%Y%m%d%H%M%S')
    src_cog_ds = gdal.OpenEx(src_path, gdal.OF_UPDATE, )
    band = src_cog_ds.GetRasterBand(1)
    # COGS do not like to be edited. So adding metadata will BREAK them
    src_cog_ds.SetMetadata({f'DNB_FILE_SIZE_{ctime}': f'{os.path.getsize(src_path)}'})
    band.SetMetadata({'DESCRIPTION': description})
    del src_cog_ds
    if use_translate:
        logger.info(f'running gdal_translate on {src_path}')
        progressbar = tqdm.tqdm(range(0, 100), desc=f'Creating COG {dst_path}', unit_scale=True)
        gdal.TranslateOptions()
        cog_ds = gdal.Translate(
            destName=dst_path,
            srcDS=src_path,
            format='COG',
            creationOptions=[

                "BLOCKSIZE=256",
                "OVERVIEWS=IGNORE_EXISTING",
                "COMPRESS=ZSTD",
                "LEVEL=9",
                "PREDICTOR=YES",
                "OVERVIEW_RESAMPLING=NEAREST",
                "BIGTIFF=IF_SAFER",
                "TARGET_SRS=EPSG:3857",
                # "RES=500",
                # f"EXTENT={lonmin},{latmin},{lonmax},{latmax}"
                "RESAMPLING=NEAREST",
                #"STATISTICS=YES",
                "ADD_ALPHA=NO",
                "COPY_SRC_MDD=YES"

            ],
            stats=True,
            # xRes=500,
            # yRes=500,
            projWin=(lonmin, latmax, lonmax, latmin),
            projWinSRS='EPSG:4326',
            callback=gdal_callback,
            callback_data=(timeout_event, progressbar)
        )
    else:



        logger.info(f'running gdalwarp on {src_path}')
        progressbar = tqdm.tqdm(range(0, 100), desc=f'Creating COG {dst_path}', unit_scale=True)

        wo = gdal.WarpOptions(
            format='COG',
            srcSRS='EPSG:4326',
            dstSRS='EPSG:3857',
            overviewLevel=None,
            multithread=True,
            outputBounds=[lonmin, latmin, lonmax, latmax],  # <xmin> <ymin> <xmax> <ymax>
            outputBoundsSRS='EPSG:4326',
            resampleAlg='NEAREST',
            targetAlignedPixels=True,
            xRes=500,
            yRes=500,
            creationOptions=[
                "BLOCKSIZE=256",
                "OVERVIEWS=IGNORE_EXISTING",
                "COMPRESS=ZSTD",
                "LEVEL=9",
                "PREDICTOR=YES",
                "OVERVIEW_RESAMPLING=NEAREST",
                "BIGTIFF=IF_SAFER",
                "NUM_THREADS=ALL_CPUS",
                "ADD_ALPHA=NO",


            ],
            copyMetadata=True,
            callback=gdal_callback,
            callback_data=(timeout_event, progressbar)
        )
        cog_ds = gdal.Warp(
            srcDSOrSrcDSTab=src_path,
            destNameOrDestDS=dst_path,
            options=wo,
        )

    #TODO ADD LandSea mask

    del cog_ds
    warnings, errors, details = validate(dst_path, full_check=True)
    if warnings:
        for wm in warnings:
            logger.warning(wm)
    if errors:
        for em in errors:
            logger.error(em)
        raise Exception('\n'.join(errors))
    return dst_path

async def process_nighttime_data(date: datetime.date = None,
                                 file_type=DNB_FILE_TYPES.DNB,
                                 DOWNLOAD_TIMEOUT=COG_DOWNLOAD_TIMEOUT,
                                 CONVERT_TIMEOUT=COG_CONVERT_TIMEOUT,
                                 force_processing=False
                                 ):
    """

    :param date: the date for which the mosaics are produced
    :param file_type:
    :param DOWNLOAD_TIMEOUT:
    :param CONVERT_TIMEOUT:
    :return:
    """


    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    timeout_event = multiprocessing.Event()

    try:

        remote_dnb_files = get_dnb_files(date=date,file_type=file_type)


        dnb_file_names = dict()
        azure_dnb_cogs =  dict()
        downloaded_dnb_files = dict()
        local_cog_files = dict()
        for k, v in remote_dnb_files.items():
            furl, fdesc = v
            _, fname = os.path.split(furl)
            dnb_file_names[k] = fname
            azure_dnb_cogs[k] = os.path.join(AZURE_DNB_COLLECTION_FOLDER,year,month, day, fname)

        cog_dnb_blob_path = azure_dnb_cogs[file_type.value]
        daily_dnb_cloudmask_blob_path = None
        remote_dnb_file = remote_dnb_files[file_type.value][0]
        if force_processing:
            will_download = force_processing
        else:
            will_download=should_download(blob_name=cog_dnb_blob_path,remote_file_url=remote_dnb_file)
        if will_download:
            logger.info(f'Processing nighttime lights from Colorado EOG for {date}')
            ################### downlaod from remote  ########################
            download_futures = list()
            for dnb_file_type, remote in remote_dnb_files.items():
                dnb_file_url, fdesc = remote
                download_task = asyncio.ensure_future(
                    download_file(file_url=dnb_file_url, read_chunk_size=AIOHTTP_READ_CHUNKSIZE)
                )
                download_task.set_name(dnb_file_type)
                download_futures.append(download_task)
            downloaded, not_downloaded = await asyncio.wait(download_futures, timeout=DOWNLOAD_TIMEOUT,
                                                            return_when=asyncio.FIRST_EXCEPTION)
            for downloaded_future in downloaded:
                try:
                    downloaded_file = await downloaded_future
                    task_name = downloaded_future.get_name()
                    logger.info(f'Successfully downloaded {task_name} to {downloaded_file}')
                    downloaded_dnb_files[task_name] = downloaded_file
                except Exception as e:
                    raise e
            for pending_future in not_downloaded:
                try:
                    pending_future.cancel()
                    await pending_future
                except asyncio.exceptions.CancelledError as e:
                    dnb_file_type = pending_future.get_name()
                    msg = f'Failed to download {dnb_file_type} from {remote_dnb_files[dnb_file_type][0]}'
                    raise Exception(msg)



            ################### convert to COG ########################
            convert2cogtasks = list()
            for dnb_file_type, downloaded_dnb_file in downloaded_dnb_files.items():
                local_cog_file = os.path.splitext(downloaded_dnb_file)[0]
                _, dnb_file_desc = remote_dnb_files[dnb_file_type]
                cog_task = asyncio.ensure_future(
                    asyncio.to_thread(tiff2cog,
                                      src_path=downloaded_dnb_file,
                                      dst_path=local_cog_file,
                                      timeout_event=timeout_event,
                                      use_translate=False,
                                      description=dnb_file_desc,
                                      #lonmin=0, lonmax=5, latmin=-5, latmax=0
                                      )
                )
                cog_task.set_name(dnb_file_type)
                convert2cogtasks.append(cog_task)

            done, pending = await asyncio.wait(
                convert2cogtasks,
                return_when=asyncio.FIRST_EXCEPTION,
                timeout=CONVERT_TIMEOUT,
            )

            if len(done) == 0:
                error_message = f'Failed to convert {list(downloaded_dnb_files.values())} to COG in {CONVERT_TIMEOUT} seconds.'
                logger.error(error_message)
                timeout_event.set()

            for pending_future in pending:
                try:
                    pending_future.cancel()
                    await pending_future
                except asyncio.exceptions.CancelledError as e:
                    raise

            for done_future in done:
                try:
                    converted_cog_path = await done_future
                    dnb_file_type = done_future.get_name()
                    logger.info(f'Successfully created {dnb_file_type} COG {converted_cog_path}')
                    local_cog_files[dnb_file_type] = converted_cog_path

                except Exception as e:
                    raise

            ################### upload to azure########################
            for dnb_file_type, local_cog_file in local_cog_files.items():
                cog_blob_pth = azure_dnb_cogs[dnb_file_type]
                logger.info(f'Uploading {dnb_file_type} from {local_cog_file} to {cog_blob_pth}')
                upload(src_path=local_cog_file, dst_path=cog_blob_pth)
                if 'cloud' in dnb_file_type.lower():
                    daily_dnb_cloudmask_blob_path = cog_blob_pth


            ################### update stac ########################

            update_undp_stac(daily_dnb_blob_path=cog_dnb_blob_path,
                             daily_dnb_cloudmask_blob_path=daily_dnb_cloudmask_blob_path,
                             file_type=file_type.value
                             )



        else:
            logger.info(f'No nighttime lights data will be processed for {date} from Colorado EOG ')

    except asyncio.CancelledError as ce:

        logger.info(f'Cancelling all tasks and actions...')
        timeout_event.set()
    except Exception as e:

        logger.error(f"Failed to process data for {date.strftime('%Y-%m-%d')}: {e}")
        raise

#
# async def process_nighttime_data_old(date: datetime.datetime = None,
#                                  file_type=DNB_FILE_TYPES.DNB,
#                                  CONVERT_TIMEOUT=COG_CONVERT_TIMEOUT):
#     """
#
#     :param date:
#     :param file_type:
#     :param CONVERT_TIMEOUT:
#     :return:
#     """
#
#
#     year = date.strftime('%Y')
#     month = date.strftime('%m')
#     day = date.strftime('%d')
#     print(year, month, day)
#     timeout_event = multiprocessing.Event()
#
#     try:
#         remote_dnb_file = compute_ntl_filename(date=date,file_type=file_type)
#         remote_dnbcloudmask_file = compute_ntl_filename(date=date, file_type=DNB_FILE_TYPES.CLOUD_COVER)
#
#         _, dnb_file_name = os.path.split(remote_dnb_file)
#         _, dnb_cloudmask_file_name = os.path.split(remote_dnbcloudmask_file)
#         cog_dnb_blob_path = os.path.join(AZURE_DNB_COLLECTION_FOLDER,year,month, day, dnb_file_name)
#         cog_dnb_cloudmask_blob_path = os.path.join(AZURE_DNB_COLLECTION_FOLDER,year,month, day, dnb_cloudmask_file_name)
#         will_download=should_download(blob_name=cog_dnb_blob_path,remote_file_url=remote_dnb_file)
#         if will_download:
#             logger.info(f'Processing nighttime lights from Colorado EOG for {date}')
#             download_futures = list()
#             for remote_file in (remote_dnb_file, remote_dnbcloudmask_file):
#                 download_task = asyncio.ensure_future(
#                     download_file(file_url=remote_file, read_chunk_size=AIOHTTP_READ_CHUNKSIZE)
#                 )
#                 download_task.set_name(remote_file)
#                 download_futures.append(download_task)
#             downloaded, not_downloaded = await asyncio.wait(download_futures, timeout=1800,
#                                                     return_when=asyncio.FIRST_EXCEPTION)
#
#             local_dnb_files = list()
#             for downloaded_future in downloaded:
#                 try:
#                     downloaded_file = await downloaded_future
#                     task_name = downloaded_future.get_name()
#                     logger.info(f'Successfully downloaded {task_name}')
#                     local_dnb_files.append(downloaded_file)
#                 except Exception as e:
#                     raise e
#             for pending_future in not_downloaded:
#                 try:
#                     pending_future.cancel()
#                     await pending_future
#                 except asyncio.exceptions.CancelledError as e:
#                     failed_dnb_file = pending_future.get_name()
#                     msg = f'Failed to download {failed_dnb_file}'
#                     raise Exception(msg)
#
#             local_cog_files = [os.path.splitext(e)[0] for e in local_dnb_files]
#             files_to_convert = dict(zip(local_dnb_files, local_cog_files))
#
#             cog_tasks = list()
#             converted_cogs = list()
#
#             for local_dnb_file, local_cog_file in files_to_convert.items():
#                 cog_task = asyncio.ensure_future(
#                     asyncio.to_thread(tiff2cog,
#                                       src_path=local_dnb_file,
#                                       dst_path=local_cog_file,
#                                       timeout_event=timeout_event,
#                                       use_translate=False,
#                                       description=f'DNB mosaic for {date.date()}',
#                                       #lonmin=0, lonmax=5, latmin=-5, latmax=0
#                                       )
#                 )
#                 cog_task.set_name(local_cog_file)
#                 cog_tasks.append(cog_task)
#
#             done, pending = await asyncio.wait(
#                 cog_tasks,
#                 return_when=asyncio.FIRST_EXCEPTION,
#                 timeout=CONVERT_TIMEOUT,
#             )
#
#             if len(done) == 0:
#                 error_message = f'Failed to convert {files_to_convert} to COG in {CONVERT_TIMEOUT} seconds.'
#                 logger.error(error_message)
#                 timeout_event.set()
#
#             for pending_future in pending:
#                 try:
#                     pending_future.cancel()
#                     await pending_future
#                 except asyncio.exceptions.CancelledError as e:
#                     raise
#
#             for done_future in done:
#                 try:
#                     await done_future
#                     converted_cog = done_future.get_name()
#                     logger.info(f'Successfully created DNB COG {converted_cog}')
#                     converted_cogs.append(converted_cog)
#
#                 except Exception as e:
#                     raise
#
#             #upload to azure
#             #cog_blob_path = os.path.join(AZURE_DNB_FOLDER, str(year), f'{month:02d}', local_dnb_file)
#             start = datetime.datetime.now()
#
#             #upload(dst_path=cog_blob_path,src_path=local_cog_file, content_type='image/tiff', overwrite=True)
#             #upload_to_azure(local_path=local_cog_file, blob_name=cog_blob_path)
#
#             for ccog in converted_cogs:
#                 _, cog_file_name = os.path.split(ccog)
#                 cog_blob_pth = os.path.join(AZURE_DNB_COLLECTION_FOLDER, year, month, day,
#                                                        cog_file_name)
#                 #upload(src_path=ccog, dst_path=cog_blob_pth)
#
#             #update_stac
#
#             #update_undp_stac(daily_dnb_blob_path=cog_blob_path, daily_dnb_cloudmask_blob_path=cog_cloudmask_blob_path)
#
#             end = datetime.datetime.now()
#             logger.info(end-start)
#
#
#         else:
#             logger.info(f'No nighttime lights data will be processed for {date} from Colorado EOG ')
#
#     except asyncio.CancelledError as ce:
#
#         logger.info(f'Cancelling all tasks and actions...')
#         timeout_event.set()
#     except Exception as e:
#
#         logger.error(f"Failed to process data for {date.strftime('%Y-%m-%d')}: {e}")
#         raise

def process_historical_nighttime_data(start_date: datetime.datetime, end_date: datetime.datetime):
    """
    Process historical nighttime data
    :param start_date: datetime.datetime
    :param end_date: datetime.datetime
    :return: None
    """
    for date in range((end_date - start_date).days):
        process_nighttime_data(start_date + datetime.timedelta(days=date))

