import io
import logging
import multiprocessing
import subprocess
from datetime import datetime, timedelta
import requests
from tqdm import tqdm
import os
import rasterio
from osgeo import gdal
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
gdal.SetConfigOption('CPL_DEBUG', 'ON')
gdal.UseExceptions()
gdal.PushErrorHandler('CPLLoggingErrorHandler')


def download_nighttime_data(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True)

    with open(save_path, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()


def reproject_and_convert_to_cog(input_path, output_path, timeout_event=None):
    # options = gdal.WarpOptions(
    #     format='COG',
    #     outputType=gdal.GDT_Unknown,
    #     srcSRS='EPSG:4326',
    #     dstSRS='EPSG:3857',
    #     resampleAlg=gdal.GRA_NearestNeighbour,
    #     warpOptions=['NUM_THREADS=ALL_CPUS'],
    #     warpMemoryLimit=0,
    #     creationOptions=['BLOCKSIZE=256', 'OVERVIEWS=IGNORE_EXISTING', 'COMPRESS=ZSTD', 'PREDICTOR=YES', 'BIGTIFF=YES'],
    #     multithread=True,
    # )
    # gdal.Warp(output_path, input_path, options=options)

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

    with subprocess.Popen(reprojection_cmd, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
        err = None
        with p.stdout:
            stream = io.open(p.stdout.fileno(), closefd=False)
            for line in stream:
                logger.info(line.strip('\r').strip('\n'))
            while p.poll() is None:
                output = stream.readline().strip('\r').strip('\n')
                if output:
                    logger.debug(output)
                    if err != output: err = output
                if timeout_event.is_set():
                    p.terminate()
                    logger.error("Terminated process")
                    raise TimeoutError("Process took too long")
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)


def create_vrt(start_date, end_date):
    assert start_date and end_date, "Start and end date must be provided"
    assert start_date < end_date, "Start date must be before end date"
    start_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')
    assert (end_date - start_date).days < 30, "Date range must be within 30 days"
    for i in range((end_date - start_date).days + 1):
        date = (start_date + timedelta(days=i)).strftime('%Y%m%d')
        if not os.path.exists(f'data/SVDNB_npp_d{date}.rade9d_sunfiltered.tif'):
            logger.info(f"Downloading data for {datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')}")
            download_nighttime_data(
                f'https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/SVDNB_npp_d{date}.rade9d_sunfiltered.tif',
                f'data/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')
        else:
            logger.info(f"Data for {datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')} already exists")
    gdal.BuildVRTOptions(
        separate=True,
        resolution='highest',
        resampleAlg=gdal.GRA_NearestNeighbour,
        addAlpha=True,
        srcNodata=0,
        VRTNodata=0,
        bandList=[1, 2, 3]
    )
    dates = [datetime.strftime(start_date + timedelta(days=i), "%Y%m%d") for i in range((end_date - start_date).days + 1)]
    gdal.BuildVRT('data/SVDNB_npp.vrt', [f'data/SVDNB_npp_d{dates[date]}.rade9d_sunfiltered.tif' for date in
                                         range((end_date - start_date).days + 1)])
    logger.info("Created VRT file")


def upload_to_azure():
    pass


if __name__ == "__main__":
    # current_date = datetime.today().strftime('%Y%m%d')
    # ROOT_URL = "https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/"
    # FILENAME = f"SVDNB_npp_d{current_date}.rade9d_sunfiltered.tif"
    # SAVE_PATH = os.path.join('data', FILENAME)
    # download_nighttime_data(ROOT_URL + FILENAME, SAVE_PATH)

    create_vrt('20240201', '20240203')

    # reproject_and_convert_to_cog('data/SVDNB_npp_d20240201.rade9d_sunfiltered.tif',
    #                              'data/SVDNB_npp_d20240201.rade9d_sunfiltered.tif_cog.tif',
    #                              timeout_event=multiprocessing.Event())
    # print("Downloaded data to:", SAVE_PATH)
    # print("Reprojected and converted to COG")
