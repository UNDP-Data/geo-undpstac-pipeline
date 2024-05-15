import json
import urllib
from undpstac_pipeline.colorado_eog import compute_dnb_filename
from undpstac_pipeline import azblob as a
from undpstac_pipeline.utils import extract_date_from_dnbfile, get_dnb_file_size_from_meta, fetch_resource_size
from undpstac_pipeline.const import COG_DNB_FILE_TYPE, AZURE_CONTAINER_NAME
import os
from osgeo import gdal
import datetime
def fix_dnb_size_metadata(relative_dnb_cog_path=None, read=True):
    exists, url = a.blob_exists_in_azure(blob_path=azure_dnb_cog_path)
    if exists:
        parse_res = urllib.parse.urlparse(url)
        _, orig_file_name = os.path.split(parse_res.path)
        item_date = extract_date_from_dnbfile(orig_file_name)
        item_id = f'SVDNB_npp_d{item_date.strftime("%Y%m%d")}'
        ctime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dnb_url = compute_dnb_filename(date=item_date, file_type=COG_DNB_FILE_TYPE)
        print(remote_dnb_url)
        original_size_bytes = fetch_resource_size(url=remote_dnb_url)
        azure_path = f'/vsiaz/{AZURE_CONTAINER_NAME}/{relative_dnb_cog_path}'

        src_ds = gdal.OpenEx(azure_path, gdal.OF_READONLY)
        meta_dict = src_ds.GetMetadata_Dict()
        item_name = None
        item_value = None
        for k, v in meta_dict.items():
            if k.startswith('DNB_FILE_SIZE'):
                item_name = k
                item_value = v
                break
        del src_ds
        if item_name and item_value:
            #delete first

            src_ds = gdal.OpenEx(azure_path, gdal.OF_UPDATE)
            src_ds.SetMetadataItem(item_name, f'{original_size_bytes}')
            src_ds.SetMetadataItem(f'DNB_FILE_SIZE_{ctime}', f'{original_size_bytes}')



if __name__ == '__main__':

    azure_dnb_cog_path = 'nighttime-lights/2024/01/24/SVDNB_npp_d20240124.rade9d_sunfiltered.tif'
    fix_dnb_size_metadata(relative_dnb_cog_path=azure_dnb_cog_path, read=True)


