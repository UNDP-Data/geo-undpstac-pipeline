import io
import os
import logging
import subprocess
import datetime
import tempfile
import xml.etree.ElementTree as ET
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from osgeo import gdal
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from utils import blob_exists_in_azure, download_blob_from_azure

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
gdal.SetConfigOption('CPL_DEBUG', 'ON')
gdal.UseExceptions()
gdal.PushErrorHandler('CPLLoggingErrorHandler')

ROOT_URL = "https://eogdata.mines.edu/nighttime_light/nightly/rade9d_sunfiltered/"


def download_nighttime_data(url: str, save_path: str):
    """
    Download nighttime data
    :param url: str
    :param save_path: str
    :return: None
    """
    response = requests.get(url, stream=True)
    response.raise_for_status() # raise an exception if the request fails.
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


def create_vrt(input_path: str, year: int, month: int):
    """
    Create a VRT file from the input file
    :param input_path: str. Path to the input file (COG)
    :param year:
    :param month:
    :return:
    """

    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"File {input_path} does not exist")
        vrt_file = f'{input_path.split("/")[-2]}/SVDNB_npp_rade9d_sunfiltered.vrt'
        if blob_exists_in_azure(vrt_file):
            # download the vrt file
            logger.info(f"Downloading {vrt_file} from Azure")
            download_blob_from_azure(vrt_file, f'{input_path}/{vrt_file.split("/")[-1]}')
            input_dataset = gdal.Open(input_path)
            input_band = input_dataset.GetRasterBand(1)
            logger.info("VRT file already exists. Adding the input file as a new band...")
            tree = ET.parse(vrt_file)
            root = tree.getroot()
            xSize = input_dataset.RasterXSize
            ySize = input_dataset.RasterYSize
            blockXSize = input_band.GetBlockSize()[0]
            blockYSize = input_band.GetBlockSize()[1]
            no_data_value = input_band.GetNoDataValue()
            datatype = gdal.GetDataTypeName(input_band.DataType)
            number_of_bands = gdal.Open(vrt_file).RasterCount
            new_child_vrt = f"""
            <VRTRasterBand dataType="{datatype}" band="{number_of_bands + 1}">
            <NoDataValue>{no_data_value}</NoDataValue>
                <ComplexSource resampling="near">
                    <SourceFilename relativeToVRT="1">{input_path.split("/")[-1]}</SourceFilename>
                    <SourceBand>1</SourceBand>
                    <SourceProperties RasterXSize="{xSize}" RasterYSize="{ySize}" DataType="{datatype}" BlockXSize="{blockXSize}" BlockYSize="{blockYSize}" />
                    <SrcRect xOff="0" yOff="0" xSize="{xSize}" ySize="{ySize}" />
                    <DstRect xOff="0" yOff="0" xSize="{xSize}" ySize="{ySize}" />
                    <NODATA>{no_data_value}</NODATA>
                </ComplexSource>
            </VRTRasterBand>
            """
            root.append(ET.fromstring(new_child_vrt))
            tree.write(vrt_file)
        else:
            logger.info("VRT file does not exist yet. Creating one...")
            options = gdal.BuildVRTOptions(
                separate=True,
                # NOTE: This is an important option for our use case - To create separate bands for each input file
                resolution='highest',
                resampleAlg=gdal.GRA_NearestNeighbour,
                bandList=[1]
            )
            gdal.BuildVRT(vrt_file, input_path, options=options)
            logger.info("Created VRT file")
    except Exception as e:
        logger.error(f"Failed to create VRT file: {e}")
        raise e


def upload_to_azure(local_path: str, blob_name: str):
    """
    Upload a file to Azure Blob Storage
    :param local_path: str - As we are using a temporary directory, the file will be deleted after the function is done
    :param blob_name:
    :return:
    """
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container='geo-nightlights', blob=blob_name)
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
                    blob_client.upload_blob(data, overwrite=True)
                    pbar.update(len(data))
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to {blob_name}: {e}")
        raise e


def process_nighttime_data(date: datetime.datetime):
    """
    Download, reproject, convert to COG, and upload to Azure
    :param date:  datetime.datetime
    :return: None
    """
    # if not date:  # if date is not provided, use today's date
    #     date = datetime.datetime.now().strftime('%Y%m%d')
    ROOT_FOLDER = 'data'
    year = date.strftime('%Y')
    month = int(date.strftime('%m'))
    try:
        if not blob_exists_in_azure(f'{ROOT_FOLDER}/cogs/{year}/{month}/SVDNB_npp_d{date}.rade9d_sunfiltered_cog.tif'):
            logger.info(f"Retrieving raw data for {date.strftime('%Y-%m-%d')}")
            with tempfile.TemporaryDirectory() as temp_dir:
                cog_path = f'{temp_dir}/cogs/{year}/{month}'
                if not os.path.exists(cog_path):
                    os.makedirs(cog_path)
                # download_nighttime_data(
                #     f'{ROOT_URL}SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered.tif',
                #     f'{temp_dir}/SVDNB_npp_d{date}.rade9d_sunfiltered.tif')

                # TODO: Following line is only for Development purposes
                # copy to local file to temp dir
                os.system(
                    f'cp data/raw/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered.tif {temp_dir}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered.tif')
                # check if the file exists
                raw_file = f'{temp_dir}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered.tif'
                reproject_and_convert_to_cog(input_path=raw_file,
                                             output_path=f'{cog_path}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered_cog.tif')
                upload_to_azure(f'{cog_path}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered_cog.tif',
                                f'cogs/{year}/{month}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered_cog.tif')
                # create_vrt from here
                create_vrt(f'{cog_path}/SVDNB_npp_d{date.strftime("%Y%m%d")}.rade9d_sunfiltered_cog.tif', date.year,
                           date.month)
                upload_to_azure(f'{cog_path}/SVDNB_npp_rade9d_sunfiltered.vrt', f'cogs/{year}/{month}/SVDNB_npp_rade9d_sunfiltered.vrt')
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


if __name__ == "__main__":
    process_nighttime_data(date=datetime.datetime(2024, 2, 2))
