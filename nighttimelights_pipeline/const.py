import datetime
import os
from enum import Enum
from pystac.extensions.eo import Band

ROOT_EOG_URL = 'https://eogdata.mines.edu/nighttime_light/nightly/'

class DNB_FILE_TYPES(Enum):
    DNB = 'DNB'
    DNB_SUNFILTERED = 'DNB_SUNFILTERED'
    CLOUD_COVER = 'CLOUD_COVER'

DNB_FOLDER = 'rade9d'
DNB_SUNFILTERED_FOLDER = 'rade9d_sunfiltered'
DNB_CLOUD_COVER_FOLDER = 'cloud_cover'
DNB_BASE_TEMPLATE = 'SVDNB_npp_d{date}'
DNB_TEMPLATE = f'{DNB_BASE_TEMPLATE}.rade9d.tif'
DNB_SUNFILTERED_TEMPLATE= f'{DNB_BASE_TEMPLATE}.rade9d_sunfiltered.tif'
DNB_CLOUD_COVER_TEMPLATE = f'{DNB_BASE_TEMPLATE}.vcld.tif'

FILE_TYPE_FOLDERS = {DNB_FILE_TYPES.DNB:DNB_FOLDER, DNB_FILE_TYPES.DNB_SUNFILTERED:DNB_SUNFILTERED_FOLDER, DNB_FILE_TYPES.CLOUD_COVER:DNB_CLOUD_COVER_FOLDER}
FILE_TYPE_TEMPLATES = {DNB_FILE_TYPES.DNB:DNB_TEMPLATE, DNB_FILE_TYPES.DNB_SUNFILTERED:DNB_SUNFILTERED_TEMPLATE, DNB_FILE_TYPES.CLOUD_COVER:DNB_CLOUD_COVER_TEMPLATE}
FILE_TYPE_NAMES = set(FILE_TYPE_FOLDERS)

AZURE_CONTAINER_NAME = 'stacdata'
AZURE_DNB_COLLECTION_FOLDER = 'nighttime-lights'
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
COG_CONVERT_TIMEOUT= os.environ.get('COG_CONVERT_TIMEOUT', 1800)
AIOHTTP_READ_CHUNKSIZE = os.environ.get('AIOHTTP_READ_CHUNKSIZE',1024**2*4) # 4MB

STAC_CATALOG_NAME = 'catalog.json'
STAC_COLLECTION_NAME = 'collection.json'
STAC_ITEM_TEMPLATE = '${day}/SVDNB_npp_d${year}${month}${day}.json'
STAC_MONTHLY_CATALOG_TEMPLATE = '${month}/catalog.json'
STAC_YEARLY_COLLECTION_TEMPLATE = '${year}/catalog.json'


DNB_BANDS = dict( DNB=Band.create(name='DNB', description='COG mosaic representing nighttime lights from VIIRS intrument. Mosaic cretaed by Colorado School of Mines', common_name='nighttime lights', center_wavelength=0.7),
    CLM=Band.create(name='CLM', description='COG mosaic representing cloud mask nighttime lights from VIIRS intrument. Mosaic cretaed by Colorado School of Mines. 0-1 -> Clear; 2-3 -> Probably Cloudy;4-5 -> Confident Cloudy ', common_name='nighttime lights cloud mask', ),
)

DNB_BBOX = -180, -65, 180, 75

DNB_START_DATE = datetime.datetime(year=2022, month=1, day=1)


