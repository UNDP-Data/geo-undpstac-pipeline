from undpstac_pipeline.colorado_eog import compute_dnb_filename
from undpstac_pipeline.utils import fetch_resource_size
import os
import logging

from undpstac_pipeline import const
import pystac
from undpstac_pipeline.stac import AzureStacIO, CustomLayoutStrategy, item_f, catalog_f

logger = logging.getLogger(__name__)


def update_stac_items():
    az_stacio = AzureStacIO()

    root_catalog_blob_path = const.STAC_CATALOG_NAME
    root_catalog_url = os.path.join(az_stacio.container_client.url, root_catalog_blob_path)

    logger.info(f'...reading ROOT STAC catalog from {root_catalog_url} ')
    root_catalog = pystac.Catalog.from_file(root_catalog_url, stac_io=az_stacio)

    nighttime_collection = root_catalog.get_child(const.AZURE_DNB_COLLECTION_FOLDER)
    for item in nighttime_collection.get_all_items():
        item_date = item.datetime.date()
        # if item_date.strftime('%Y-%m-%d') != '2024-01-01':
        #     logger.info(f'skipped {str(item_date)}')
        #     continue
        logger.info(f'checking {str(item_date)}')
        for asset_name in ('DNB', 'DNB_SUNFILTERED'):
            if asset_name not in item.assets:
                continue
            try:
                remote_dnb_url = compute_dnb_filename(date=item_date, file_type=asset_name)
                original_size_bytes = fetch_resource_size(url=remote_dnb_url)
                asset = item.assets[asset_name]
                asset.extra_fields['original_file_size'] = original_size_bytes
                logger.debug(f'Updated original_file_size of {asset_name} to {original_size_bytes} in {str(item_date)}')
            except KeyError:
                pass

    logger.info('Saving STAC structure to Azure')
    root_catalog.normalize_hrefs(root_href=az_stacio.container_client.url,
                                 strategy=CustomLayoutStrategy(catalog_func=catalog_f, item_func=item_f))

    root_catalog.save(stac_io=az_stacio)


if __name__ == '__main__':
    update_stac_items()
