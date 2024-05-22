from undpstac_pipeline.colorado_eog import compute_dnb_filename
from undpstac_pipeline.utils import fetch_resource_size
import os
import logging

from undpstac_pipeline import const
import pystac
from undpstac_pipeline.stac import AzureStacIO, CustomLayoutStrategy, item_f, catalog_f

logger = logging.getLogger(__name__)

def update_stac_items(dry_run=False):
    az_stacio = AzureStacIO()

    root_catalog_blob_path = const.STAC_CATALOG_NAME
    root_catalog_url = os.path.join(az_stacio.container_client.url, root_catalog_blob_path)

    logger.info(f'...reading ROOT STAC catalog from {root_catalog_url} ')
    root_catalog = pystac.Catalog.from_file(root_catalog_url, stac_io=az_stacio)

    nighttime_collection = root_catalog.get_child(const.AZURE_DNB_COLLECTION_FOLDER)
    for item in nighttime_collection.get_all_items():
        item_date = item.datetime.date()
        logger.info(f'checking {str(item_date)}')
        for asset_name in ('DNB_SUNFILTERED', 'DNB'):
            if asset_name not in item.assets:
                continue
            remote_dnb_url = compute_dnb_filename(date=item_date, file_type=asset_name)
            remote_size_bytes = fetch_resource_size(url=remote_dnb_url)
            asset = item.assets[asset_name]

            if not 'original_size_bytes' in asset.extra_fields:
                if not dry_run:
                    asset.extra_fields['original_size_bytes'] = remote_size_bytes
                    logger.info(
                        f'Created original_size_bytes={remote_size_bytes} property for {asset_name} - {item_date}')
                else:
                    logger.info(f'Ought to create original_size_bytes={remote_size_bytes} property for {asset_name} - {item_date}')
            else:
                original_size_bytes = asset.extra_fields['original_size_bytes']
                if original_size_bytes != remote_size_bytes:
                    if dry_run:
                        logger.info(f'Ought to change original_size_bytes from {original_size_bytes} to {remote_size_bytes} for {item_date}-{asset_name}')
                    else:
                        logger.info(
                            f'Replaced original_size_bytes from {original_size_bytes} with {remote_size_bytes} for {item_date}-{asset_name}')
                        asset.extra_fields['original_size_bytes'] = remote_size_bytes
                else:
                    logger.info(f'original_size_bytes for {item_date}-{asset_name} asset needs no change')
            if not dry_run:
                logger.info(f'Saving item {item_date}')
                item.make_asset_hrefs_relative()
                item.save_object(include_self_link=False, stac_io=az_stacio)
            # logger.debug(f'Updated original_file_size of {asset_name} to {original_size_bytes} in {str(item_date)}')






if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    update_stac_items(dry_run=False)
