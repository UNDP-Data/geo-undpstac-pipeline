import io
import os
import logging
import subprocess
import datetime
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from osgeo import gdal
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
gdal.SetConfigOption('CPL_DEBUG', 'ON')
gdal.UseExceptions()
gdal.PushErrorHandler('CPLLoggingErrorHandler')

ROOT_URL = "https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/"


def download_nighttime_data(url, save_path):
    response = requests.get(url, stream=True)
    res_code = response.status_code
    if res_code != 200:
        logger.error(f"Failed to download {url} with status code {res_code}")
    # response.raise_for_status()
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


def reproject_and_convert_to_cog(input_path, timeout_event=None):
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

    date = input_path.split('_d')[1].split('.')[0]
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    # make directory if it doesn't exist
    if not os.path.exists(f'data/{year}/{month}/{day}'):
        os.makedirs(f'data/{year}/{month}/{day}')

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
        f'data/cogs/{year}/{month}/{day}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif'
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


def create_vrt(year, month):
    start_date = datetime.date(year, month, 1)
    next_month = start_date.replace(day=28) + datetime.timedelta(days=4)
    end_date = next_month - datetime.timedelta(days=next_month.day)

    dates = [datetime.datetime.strftime(start_date + datetime.timedelta(days=i), "%Y%m%d") for i in
             range((end_date - start_date).days + 1)]
    # # check all raw files exist
    for date in dates:
        year = date[:4]
        month = date[4:6]
        if not os.path.exists(f'data/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif'):
            # logger.error(f"File data/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif does not exist. Will download and reproject it")
            # if not os.path.exists(f'data/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered.tif'):
            #     download_nighttime_data(
            #         f'{ROOT_URL}SVDNB_npp_d{date}.rade9d_sunfiltered.tif',
            #         f'data/raw/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')
            #     reproject_and_convert_to_cog(f'data/raw/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')
            logger.error(f"File data/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif does not exist")
            raise FileNotFoundError(f"File data/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif does not exist")
    options = gdal.BuildVRTOptions(
        separate=True, # NOTE: This is an important option for our use case - To create separate bands for each input file
        resolution='highest',
        resampleAlg=gdal.GRA_NearestNeighbour,
        srcNodata=0,
        VRTNodata=0,
        bandList=[dates.index(date) + 1 for date in dates]
    )
    gdal.BuildVRT(F'data/{year}/{month}/SVDNB_npp_rade9d_sunfiltered.vrt', [f'data/SVDNB_npp_d{dates[date]}.rade9d_sunfiltered_cog.tif' for date in
                                         range((end_date - start_date).days + 1)], options=options)
    logger.info("Created VRT file")


def upload_to_azure(local_path, blob_name):
    assert os.path.exists(local_path), f"File {local_path} does not exist"
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container='geo-nightlights', blob=blob_name)
    with open(local_path, "rb") as file:
        total_size = os.path.getsize(local_path)
        with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=blob_name) as pbar:
            # Upload the file in chunks
            chunk_size = 4 * 1024 * 1024  # 4MB chunks
            while True:
                data = file.read(chunk_size)
                if not data:
                    break
                blob_client.upload_blob(data, overwrite=True)
                pbar.update(len(data))


def process_nighttime_data(year, month):
    start_date = datetime.date(year, month, 1)
    next_month = start_date.replace(day=28) + datetime.timedelta(days=4)
    end_date = next_month - datetime.timedelta(days=next_month.day)
    dates_in_month = [datetime.datetime.strftime(start_date + datetime.timedelta(days=x), "%Y%m%d") for x in range((end_date - start_date).days + 1)]

    # check if data exists
    ROOT_FOLDER = 'data'
    raw_file_path = os.listdir(f'{ROOT_FOLDER}/raw')
    for date in dates_in_month:
        if f'SVDNB_npp_d{date}.rade9d_sunfiltered.tif' not in raw_file_path:  # if a local raw geotiff file does not exist, download it
            logger.info(f"Downloading data for {datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')}")
            download_nighttime_data(
                f'{ROOT_URL}SVDNB_npp_d{date}.rade9d_sunfiltered.tif',
                f'{ROOT_FOLDER}/raw/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')
        else:
            logger.info(f"Data for {datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')} already exists")
        reproject_and_convert_to_cog(f'{ROOT_FOLDER}/raw/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')
    create_vrt(year, month)
    # upload_to_azure(f'{ROOT_FOLDER}/cogs/{date[:4]}/{date[4:6]}/{date[6:]}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif',
    #                 f'{date[:4]}/{date[4:6]}/{date[6:]}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif')


if __name__ == "__main__":
    process_nighttime_data(2024, 2)
    # reproject_and_convert_to_cog('data/raw/SVDNB_npp_d20240201.rade9d_sunfiltered.tif')
