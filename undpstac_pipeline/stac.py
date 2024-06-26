import json

from osgeo import gdal
from azure.core.exceptions import ResourceNotFoundError
import itertools
from typing import Any
from urllib.parse import urlparse
import pystac
from pystac.extensions.eo import  EOExtension
from pystac.extensions.raster import RasterExtension, RasterBand
from pystac.provider import Provider, ProviderRole
from pystac import stac_io, HREF
import os
from undpstac_pipeline import const
from undpstac_pipeline.azblob import blob_exists_in_azure, get_container_client, upload, download
from undpstac_pipeline import utils as u

from pystac.layout import  CustomLayoutStrategy
import logging

logger = logging.getLogger(__name__)

def catalog_f(catalog:pystac.Catalog=None, parent_dir:str=None, is_root:bool=False ) -> str:
    if is_root:
        return os.path.join(parent_dir,const.STAC_CATALOG_NAME)
    else:
        id = catalog.id.split('-')[-1]
        if len(id) ==  4:
            return os.path.join(parent_dir,id, const.STAC_CATALOG_NAME)
        else:
            month = id
            return os.path.join(parent_dir, month, const.STAC_CATALOG_NAME)

def item_f(item:pystac.Item=None, parent_dir = None)->str:
    return os.path.join(parent_dir,item.datetime.strftime('%d'),f'{item.id}.json')


def to_set(self):
    return set(itertools.chain(*self.intervals))


class AzureStacIO(stac_io.StacIO):
    # container_client = get_blob_service_client()
    container_name = f'{const.AZURE_CONTAINER_NAME}/'
    content_type = 'application/json'
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.container_client = get_container_client()
    def get_relative_blob_path(self, url=None):
        purl = urlparse(url)
        return purl.path[1:].split(self.container_name)[1]

    def read_text(self, source: HREF, *args: Any, **kwargs: Any) -> str:
        src_blob_path = self.get_relative_blob_path(url=source)
        return download(blob_path=src_blob_path)


    def write_text(self, dest: HREF, txt: str, *args: Any, **kwargs: Any ) -> None:

        dst_blob_path = self.get_relative_blob_path(url=dest)
        upload(
            dst_path=dst_blob_path,
            data=txt.encode('utf-8'),
            content_type=self.content_type
        )
    def __del__(self):
        self.container_client.close()






def create_stac_catalog(
        id=None,
        description=None,
        title=None,
    ):


    catalog = pystac.Catalog(
        id=id,
        description=description,
        title=title,
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED,
    )
    #catalog.normalize_hrefs(strategy=TemplateLayoutStrategy(catalog_template='${catalog}'), root_href=root_href)
    return catalog


def create_dnb_stac_raster_item(
                        item_id=None,
                        local_cog_files=None,
                        azure_cog_files=None,
                        az_stacio=None,
                        file_type=None,
                        original_size_bytes=None,
):

    epsg_code, bbox, footprint = u.get_bbox_and_footprint(raster_path=local_cog_files[file_type])
    azure_cog_path = azure_cog_files[file_type]
    _, item_name = os.path.split(azure_cog_path)
    item_date = u.extract_date_from_dnbfile(item_name)
    item = pystac.Item(
        id=item_id,
        geometry=footprint,
        bbox=bbox,
        datetime=item_date,
        properties={},
        stac_extensions=['raster']
    )
    item.ext.add('proj')
    item.ext.proj.epsg = epsg_code

    for ftype, local_cog_file_path in local_cog_files.items():
        azure_cog_path = azure_cog_files[ftype]
        azure_blob_client = az_stacio.container_client.get_blob_client(blob=azure_cog_path)
        desc = const.DNB_FILE_TYPES_DESC[ftype]
        extra = dict(original_size_bytes=original_size_bytes) if ftype == file_type else None
        asset = pystac.Asset(
            href=azure_blob_client.url,
            media_type=pystac.MediaType.COG,
            title=f'{const.DNB_FILE_TYPE_NAMES[ftype]}',
            description=f'{desc} from Colorado schools of Mines',
            roles=[('analytics')],
            extra_fields=extra
        )
        raster = RasterExtension.ext(asset, add_if_missing=False)
        info = gdal.Info(local_cog_file_path, format='json')
        bprops = info['bands'].pop()
        props = dict()
        props['nodata'] = bprops['noDataValue']
        props['data_type'] = bprops['type']
        if 'scale' in bprops:
            props['scale'] = bprops['scale']
        if 'offset' in bprops:
            props['offset'] = bprops['offset']

        if ftype == file_type:
            props.update({'unit': 'nWcm-2sr-1'})
        rb = RasterBand(properties=props)
        raster.apply([rb])

        item.add_asset(
            key=ftype,
            asset=asset,
        )
    item.set_self_href(azure_blob_client.url)
    item.make_asset_hrefs_relative()
    return item



def create_dnb_stac_item(
                        item_id=None,
                        daily_dnb_blob_path=None,
                        daily_dnb_cloudmask_blob_path=None,
                        add_eo_extension=True,
                        az_stacio=None,
                        file_type=None, bbox=None, footprint=None
):




    dnb_blob_client = az_stacio.container_client.get_blob_client(blob=daily_dnb_blob_path)
    dnb_cloudmask_blob_client = az_stacio.container_client.get_blob_client(blob=daily_dnb_cloudmask_blob_path)



    _, item_name = os.path.split(daily_dnb_blob_path)
    item_date = u.extract_date_from_dnbfile(item_name)

    item = pystac.Item(
                        id=item_id,
                        geometry=footprint,
                        bbox=bbox,
                        datetime=item_date,
                        properties={},
                        stac_extensions=['eo', 'proj'] if add_eo_extension else ['proj']

                       )
    item.ext.add('proj')
    item.ext.proj.epsg = 3857
    dnb_asset = pystac.Asset(
            href=dnb_blob_client.url,
            media_type=pystac.MediaType.COG,
            title='VIIRS DNB mosaic from Colorado schools of Mines',
            roles=[('analytics')]

        )
    if add_eo_extension:
        eo = EOExtension.ext(dnb_asset, add_if_missing=False)
        eo.apply([const.DNB_BANDS['DNB']])
    item.add_asset(
        key=file_type,
        asset=dnb_asset,
    )
    clm_asset = pystac.Asset(
        href=dnb_cloudmask_blob_client.url,
        media_type=pystac.MediaType.COG,
        title='VIIRS DNB Cloud mask mosaic from Colorado schools of Mines'
    )
    if add_eo_extension:
        eo = EOExtension.ext(clm_asset, add_if_missing=False)
        eo.apply([const.DNB_BANDS['CLM']])
    item.add_asset(
        key=const.DNB_FILE_TYPES['CLOUD_COVER'],
        asset=clm_asset
    )

    return item


def create_nighttime_lights_collection():


    spatial_extent = pystac.SpatialExtent(bboxes=[const.DNB_BBOX])
    col = pystac.Collection(id='nighttime-lights',description='Nighttime lights mosaics in COG format from Colorado School of Mines',
                            title='Nighthly nighttime lights mosaics',
                            license='Creative Commons Attribution 4.0 International',
                            providers=[
                                Provider(name='Colorado Schools of Mines',roles=[ProviderRole.PRODUCER, ProviderRole.LICENSOR]),
                                Provider(name='UNDP', description='United Nations Development Programme', roles=[ProviderRole.PROCESSOR, ProviderRole.HOST])

                            ],
                            extent=pystac.Extent(spatial=spatial_extent, temporal=None),
                            catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED
                            )
    return col


def create_undp_stac_tree():
    """
    Create UNDP stac tree, that is the root catalog and the nighttime lights collection
    :param az_stacio:
    :return:
    """



    logger.info('...creating ROOT STAC catalog')
    root_catalog = create_stac_catalog(
        id='undp-stac',
        description='Geospatial data in COG format from UNDP GeoHub data store',
        title='UNDP STAC repository',
    )

    logger.info('...creating nighttime lights STAC collection')
    nighttime_collection = create_nighttime_lights_collection()
    # add collection to root catalog
    l = root_catalog.add_child(nighttime_collection )

    # save to azure through
    #root_catalog.save(stac_io=az_stacio)
    return root_catalog


def push_to_stac(
        local_cog_files=None,
        azure_cog_files=None,
        file_type=None,
        original_size_bytes=None,
        collection_folder=const.AZURE_DNB_COLLECTION_FOLDER
    ):

    az_stacio = AzureStacIO()
    root_catalog_url = os.path.join(az_stacio.container_client.url, const.STAC_CATALOG_NAME)

    try:
        logger.info(f'...reading ROOT STAC catalog from {root_catalog_url} ')
        root_catalog = pystac.Catalog.from_file(root_catalog_url, stac_io=az_stacio)
    except ResourceNotFoundError as e:
        root_catalog = create_undp_stac_tree()

    collection_ids = [e.id for e in root_catalog.get_collections()]
    assert collection_folder in collection_ids, f'{collection_folder} collection does not exist. Something enexpected happened!'

    nighttime_collection = root_catalog.get_child(collection_folder)


    pth, blob_name = os.path.split(azure_cog_files[file_type])
    item_date = u.extract_date_from_dnbfile(blob_name)
    year = item_date.strftime('%Y')
    month = item_date.strftime('%m')


    yearly_catalog_id = f'{collection_folder}-{year}'
    yearly_catalog = nighttime_collection.get_child(yearly_catalog_id, )
    yearly_catalog_exists = yearly_catalog is not None

    if not yearly_catalog_exists:
        yearly_catalog = create_stac_catalog(
            id=yearly_catalog_id,
            title=f'Nighttime lights in {year}',
            description=f'VIIRS DNB nighttime lights nightly mosaics in {year}'
        )
        nighttime_collection.add_child(yearly_catalog)
        nighttime_collection.links = sorted(nighttime_collection.links, key=lambda e: os.path.split(e.href)[-1])

    monthly_catalog_id = f'{yearly_catalog_id}-{month}'
    monthly_catalog = yearly_catalog.get_child(monthly_catalog_id)
    monthly_catalog_exists = monthly_catalog is not None
    if not monthly_catalog_exists:
        monthly_catalog = create_stac_catalog(
            id=monthly_catalog_id,
            title=f'Nighttime lights in {year}-{month}',
            description=f'VIIRS DNB nighttime lights nightly mosaics in {year}-{month}'
        )
        yearly_catalog.add_child(monthly_catalog)
        yearly_catalog.links = sorted(yearly_catalog.links, key=lambda e: os.path.split(e.href)[-1])

    item_id = f'SVDNB_npp_d{item_date.strftime("%Y%m%d")}'
    daily_dnb_item = create_dnb_stac_raster_item(
        item_id=item_id,
        local_cog_files=local_cog_files,
        azure_cog_files=azure_cog_files,
        az_stacio=az_stacio,
        file_type=file_type,
        original_size_bytes=original_size_bytes

    )

    items = monthly_catalog.get_items(item_id)

    for item in items:
        if item.id == daily_dnb_item.id:
            logger.info(f'updating item id {item.id}')
            monthly_catalog.remove_item(item_id=item.id)
    link = monthly_catalog.add_item(daily_dnb_item)
    link.extra_fields = {'date':item_date.strftime('%Y%m%d')}
    #sort item links so the updated item is not last
    monthly_catalog.links = sorted(monthly_catalog.links, key=lambda e: os.path.split(e.href)[-1])
    item_datetime = daily_dnb_item.datetime
    temporal_extent = nighttime_collection.extent.temporal

    if temporal_extent is None:
        temporal_extent = pystac.TemporalExtent(intervals=[[item_datetime, item_datetime]])
        nighttime_collection.extent.temporal = temporal_extent
    else:
        update_temporal_extent(item_datetime=item_datetime, temporal_extent=temporal_extent)
    logger.info(f'...normalizing links')
    root_catalog.normalize_hrefs(root_href=az_stacio.container_client.url,
                                 strategy=CustomLayoutStrategy(catalog_func=catalog_f, item_func=item_f),
                                 skip_unresolved=True) # skip unresolved is essential as it will skip
    # saving any children or items which have not been changed

    logger.info('Saving STAC structure to Azure')
    root_catalog.save(stac_io=az_stacio, )



def update_temporal_extent(item_datetime = None, temporal_extent=None):
    """
    The update ignored the fact the first interval is global

    :param item_datetime:
    :param temporal_extent:
    :return:
    """

    interval = temporal_extent.intervals.pop()
    interval_start, interval_end = interval
    if item_datetime < interval_start:
        interval_start  = item_datetime
    if item_datetime > interval_end:
        interval_end = item_datetime
    temporal_extent.intervals.append([interval_start, interval_end])

if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    push_to_stac()
    # te = pystac.TemporalExtent([const.DNB_START_DATE, const.DNB_START_DATE])
    # #print(te.to_dict())
    # ndt = const.DNB_START_DATE + datetime.timedelta(days=1)
    # update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    # print(te.to_dict())
    # ndt = const.DNB_START_DATE + datetime.timedelta(days=5)
    # update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    # print(te.to_dict())
    #
    # ndt = const.DNB_START_DATE + datetime.timedelta(days=3)
    # update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    # print(te.to_dict())
    # ndt = const.DNB_START_DATE + datetime.timedelta(days=4)
    # update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    # print(te.to_dict())

