import multiprocessing
import tempfile
import time
import asyncio
from nighttimelights_pipeline.utils import should_download, download_http_resurce
from nighttimelights_pipeline.colorado_eog import compute_ntl_filename, download_file
from nighttimelights_pipeline.const import DNB_FILE_TYPES, AZURE_DNB_FOLDER
import datetime
import logging
import os
import subprocess
import io
import signal
from multiprocessing import Event
from osgeo import gdal
logger = logging.getLogger(__name__)


def gdal_callback(complete, message, timeout_event):
    logger.info(f'{complete * 100:.2f}%')
    if timeout_event and timeout_event.is_set():
        logger.info(f'GDAL received timeout signal')
        return 0


def tiff2cog(src_path=None, dst_path=None, timeout_event=None, use_translate=True):

    logger.debug(f'Converting {src_path} to COG')
    if use_translate:
        cog = gdal.Translate(
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

            callback=gdal_callback,
            callback_data=timeout_event
        )
    else:
        cog = gdal.Warp(srcDSOrSrcDSTab=src_path, destNameOrDestDS=dst_path,
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
                        #options=gdal.WarpOptions(warpOptions=['NUM_THREADS=ALL_CPUS']), #if tis is enabled async mode does not work with CTRL^C
                        callback=gdal_callback,
                        callback_data=timeout_event,


                        )




def reproject_and_convert_to_cog(input_path: str, output_path: str, timeout_event=None):
    """
    Reproject and convert to COG
    :param input_path: str
    :param output_path: str
    :param timeout_event: multiprocessing.Event
    :return: None
    """
    reprojection_cmd = [
        'gdalwarp',
        '-s_srs', 'EPSG:4326',
        '-t_srs', 'EPSG:3857',
        '-of', 'COG',  # 'COG
        '-r', 'near',
        '-ovr', 'NONE',
        '-wo', 'NUM_THREADS=ALL_CPUS',
        '-co', 'BLOCKSIZE=256',
        '-co', 'OVERVIEWS=IGNORE_EXISTING',
        '-co', 'COMPRESS=ZSTD',
        '-co', 'PREDICTOR=YES',
        '-co', 'OVERVIEW_RESAMPLING=NEAREST',
        '-co', 'BIGTIFF=YES',
        '-overwrite',
        input_path,
        output_path
    ]
    # subprocess.run(reprojection_cmd, check=True)
    with subprocess.Popen(reprojection_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:

        with p.stdout:
            stream = io.open(p.stdout.fileno(), closefd=False)
            for line in stream:
                logger.info(line.strip('\r').strip('\n'))
            while p.poll() is None:
                output = stream.readline().strip('\r').strip('\n')
                if output:
                    logger.debug(output)
                if timeout_event and timeout_event.is_set():
                    logger.error(f'Terminating process')
                    p.terminate()
                    logger.error("Terminated process")
                    raise TimeoutError("Process took too long")
        with p.stderr:
            stream = io.open(p.stderr.fileno(), closefd=False)
            for line in stream:
                logger.info(line.strip('\r').strip('\n'))
            while p.poll() is None:
                err = stream.readline().strip('\r').strip('\n')
                if err:
                    logger.debug(err)
                if timeout_event and timeout_event.is_set():
                    logger.error(f'Terminating process')
                    p.terminate()
                    logger.error("Terminated process")
                    raise TimeoutError("Process took too long")
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)

async def process_nighttime_data(date: datetime.datetime = None, file_type=DNB_FILE_TYPES.DNB,
                                 CONVERT_TIMEOUT=15, sync=False):
    """

    :param date:
    :param file_type:
    :param CONVERT_TIMEOUT:
    :param sync:
    :return:
    """


    year = date.year
    month = date.month
    timeout_event = multiprocessing.Event()


    def inner_ctrl_c_signal_handler(sig, frame):

        """
        Function that gets called when the user issues a
        keyboard interrupt (ctrl+c). This has to have acccess to the timeout event by being internal
        to this function. Because gdaltranslate is not multithreaded GDAL catches this signal and exists with 0 code
        This makes it looks like the task terminated without error which is kinda true as we purposefully stopped it

        NB: This solution works here, however I am skeptical that it will work always. For example when using gdalwarp with
        NUM_THREADS=ALL_CPUS this does nto work anymore, as GDAL itself launches multiple threads and they are not terminated


        """
        logger.info('SIGINT caught!')
        timeout_event.set()

    try:
        remote_dnb_file = compute_ntl_filename(date=date,file_type=file_type)
        _, dnb_file_name = os.path.split(remote_dnb_file)
        cog_blob_path = os.path.join(AZURE_DNB_FOLDER,str(year),f'{month:02d}')
        will_download=should_download(blob_name=cog_blob_path,remote_file_url=remote_dnb_file)
        if will_download:
            logger.info(f'Processing nighttime lights from Colorado EOG for {date}')
            local_dnb_file = await download_file(file_url=remote_dnb_file, read_chunk_size=1024**2*4)
            #local_dnb_file = download_http_resurce(remote_dnb_file, '/tmp/a.tif')
            logger.info(f'Downloaded {remote_dnb_file} to {local_dnb_file}')
            local_cog_file = os.path.splitext(local_dnb_file)[0]

            if sync:
                reproject_and_convert_to_cog(input_path=local_dnb_file, output_path=local_cog_file, timeout_event=timeout_event)
            else:
                signal.signal(signal.SIGINT, inner_ctrl_c_signal_handler)
                cog_task = asyncio.ensure_future(
                    asyncio.to_thread(tiff2cog, src_path=local_dnb_file, dst_path=local_cog_file, timeout_event=timeout_event )
                )
                cog_task.set_name('cog')
                logger.info('going to wait')
                done, pending = await asyncio.wait(
                    [cog_task],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=CONVERT_TIMEOUT,
                )
                logger.info(done)
                logger.info(pending)
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
                for done_future in done:
                    try:
                        await done_future
                        logger.info(f'{done_future.get_name()} is done')

                    except Exception as e:
                        logger.error(f'done future error {e}')



                #tiff2cog(src_path=local_dnb_file, dst_path=local_cog_file, timeout_event=timeout_event)

        else:
            logger.info(f'No nighttime lights data will be processed for {date} from Colorado EOG ')

    except KeyboardInterrupt as ke:
        logger.info(f'Cancelling')
        timeout_event.set()
    except subprocess.CalledProcessError as ce:
        if ce.returncode ==  -2:
            logger.info(f'Cancelled')


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

