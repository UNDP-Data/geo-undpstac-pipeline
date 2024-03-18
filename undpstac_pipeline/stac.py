import datetime
import itertools
from typing import Any
from urllib.parse import urlparse
import pystac
from pystac.extensions.eo import Band, EOExtension
from pystac.provider import Provider, ProviderRole
from pystac import stac_io, HREF
from osgeo import gdal, osr
from shapely.geometry import Polygon, mapping
from tempfile import TemporaryDirectory
import os
from undpstac_pipeline import const
from undpstac_pipeline.azblob import blob_exists_in_azure, get_container_client, upload, download
from undpstac_pipeline import utils as u
from pystac.layout import  CustomLayoutStrategy
import logging

tmp_dir = TemporaryDirectory()
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
def as_timeranges(self):
    return [TimeRange(*e) for e in self.intervals].pop()
pystac.TemporalExtent.to_set = to_set
pystac.TemporalExtent.as_timeranges = as_timeranges

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

def get_bbox_and_footprint(raster_path=None):
    ds = gdal.OpenEx(raster_path, gdal.OF_RASTER )
    ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()
    lrx = ulx + (ds.RasterXSize * xres)
    lry = uly + (ds.RasterYSize * yres)
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())
    dst_srs = src_srs.CloneGeogCS()
    ct = osr.CoordinateTransformation(src_srs, dst_srs)
    uly, ulx, _ =  [round(e, 2) for e in ct.TransformPoint(ulx, uly)]
    lry, lrx, _ = [round(e,2) for e in ct.TransformPoint(lrx, lry)]
    bbox = [ulx, lry, lrx, uly]

    footprint = Polygon([
        [ulx, uly],
        [lrx, uly],
        [lrx, lry],
        [ulx, lry],
        [ulx, uly]
    ])
    return bbox, mapping(footprint)

def create_stac_item(item_path=None):

    bbox, footprint = get_bbox_and_footprint(raster_path=item_path)
    _, item_name = os.path.split(item_path)
    item_date = u.extract_date_from_dnbfile(item_name)

    item = pystac.Item(id=u.generate_id(name=item_path),
                       geometry=footprint,
                       bbox=bbox,
                       datetime=item_date,
                       properties={}
                       )
    item.add_asset(
        key='DNB',
        asset=pystac.Asset(
            href=item_path,
            media_type=pystac.MediaType.COG,
            title='VIIRS DNB mosaic from Colorado schools of Mines'
        )
    )

    return item

def create_dnb_stac_item(
                        item_id=None,
                        daily_dnb_blob_path=None,
                        daily_dnb_cloudmask_blob_path=None,
                        add_eo_extension=True,
                        az_stacio=None,
                        file_type=None
):




    dnb_blob_client = az_stacio.container_client.get_blob_client(blob=daily_dnb_blob_path)
    dnb_cloudmask_blob_client = az_stacio.container_client.get_blob_client(blob=daily_dnb_cloudmask_blob_path)

    # both assets have same exptent
    bbox, footprint = get_bbox_and_footprint(raster_path=dnb_blob_client.url)
    _, item_name = os.path.split(daily_dnb_blob_path)
    item_date = u.extract_date_from_dnbfile(item_name)

    item = pystac.Item(#id=u.generate_id(name=item_path),
                        #id=f'nighttime-lights-{item_date.strftime("%Y-%m-%d")}',
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
        key=const.DNB_FILE_TYPES.CLOUD_COVER.value,
        asset=clm_asset
    )

    return item


def create_nighttime_lights_collection():


    spatial_extent = pystac.SpatialExtent(bboxes=[const.DNB_BBOX])
    col = pystac.Collection(id='nighttime-lights',description='Nighttime lights mosaics in COG format from Colorado School of Mines  ', title='Nighthly VIIRS DNB mosaics',
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
    root_catalog.add_child(nighttime_collection )
    # save to azure through
    #root_catalog.save(stac_io=az_stacio)
    return root_catalog



def update_undp_stac(
        daily_dnb_blob_path=None,
        daily_dnb_cloudmask_blob_path=None,
        file_type=None,
        collection_folder=const.AZURE_DNB_COLLECTION_FOLDER
    ):
    """

    :param blob_path:
    :param container_name:
    :param collection_folder:
    :return:
    """
    az_stacio = AzureStacIO()

    root_catalog_blob_path = const.STAC_CATALOG_NAME
    root_catalog_url = os.path.join(az_stacio.container_client.url, root_catalog_blob_path)
    root_catalog_exists, url = blob_exists_in_azure(blob_path=root_catalog_blob_path, container_client=az_stacio.container_client)

    if not root_catalog_exists :
        root_catalog = create_undp_stac_tree()
    else:
        logger.info(f'...reading ROOT STAC catalog from {root_catalog_url} ')
        root_catalog = pystac.Catalog.from_file(root_catalog_url,stac_io=az_stacio)

    collection_ids = [e.id for e in root_catalog.get_collections()]
    assert collection_folder in collection_ids, f'{collection_folder} collection does not exist. Something enexpected happened!'

    nighttime_collection = root_catalog.get_child(collection_folder)

    pth, blob_name = os.path.split(daily_dnb_blob_path)
    item_date = u.extract_date_from_dnbfile(blob_name)
    year = item_date.strftime('%Y')
    month = item_date.strftime('%m')
    day = item_date.strftime('%d')
    time_el = []
    time_path_catalog = None


    for time_unit in year, month:
        time_el.append(time_unit)
        time_id = '-'.join(time_el)
        time_path_id = f'{collection_folder}-{time_id}'
        time_path_catalog = nighttime_collection.get_child(time_path_id, recursive=True)
        catalog_exists = time_path_catalog is not None

        if not catalog_exists:
            time_path_catalog = create_stac_catalog(
                id=time_path_id,
                title=f'Nighttime lights in {time_id}',
                description=f'VIIRS DNB nighttime lights nightly mosaics in {time_id}'
            )

            nelem = len(time_el)
            if nelem == 1:
                nighttime_collection.add_child(time_path_catalog)
            else:
                parent_id = f'{collection_folder}-{time_el[nelem-2]}'
                parent = nighttime_collection.get_child(parent_id, recursive=True)
                parent.add_child(time_path_catalog)

    root_catalog.normalize_hrefs(root_href=az_stacio.container_client.url,strategy=CustomLayoutStrategy(catalog_func=catalog_f))


    # dnb_year_catalog_blob_path = os.path.join(collection_folder, year, const.STAC_CATALOG_NAME)
    # if not blob_exists_in_azure(dnb_year_catalog_blob_path):
    #     year_catalog = create_stac_catalog(
    #         id=f'nighttime-lights-{year}',
    #         title=f'Nighttime lights in {year}'
    #     )
    # else:
    #     year_catalog = pystac.Catalog.from_file(dnb_year_catalog_blob_path)
    #
    # dnb_year_month_catalog_blob_path = os.path.join(collection_folder, year, month, 'catalog.json')
    # if not blob_exists_in_azure(dnb_year_month_catalog_blob_path):
    #     year_month_catalog = create_stac_catalog(id=f'nighttime-lights-{year}-{month}', title=f'Nighttime lights in {year}-{month}')
    # else:
    #     year_month_catalog = pystac.Catalog.from_file(dnb_year_month_catalog_blob_path)

    item_id = f'SVDNB_npp_d{item_date.strftime("%Y%m%d")}'
    daily_dnb_item = create_dnb_stac_item(
        item_id=item_id,
        daily_dnb_blob_path=daily_dnb_blob_path,
        daily_dnb_cloudmask_blob_path=daily_dnb_cloudmask_blob_path,
        add_eo_extension=False,
        az_stacio=az_stacio,
        file_type=file_type
    )

    if not time_path_catalog.get_items(item_id,recursive=True):
        time_path_catalog.add_item(daily_dnb_item)

    root_catalog.normalize_hrefs(root_href=az_stacio.container_client.url,
                                 strategy=CustomLayoutStrategy(catalog_func=catalog_f, item_func=item_f))
    item_datetime = daily_dnb_item.datetime


    temporal_extent = nighttime_collection.extent.temporal


    if temporal_extent is None:
        temporal_extent = pystac.TemporalExtent(intervals=[[item_datetime, item_datetime]])
        nighttime_collection.extent.temporal = temporal_extent
    else:
        update_temporal_extent(item_datetime=item_datetime, temporal_extent=temporal_extent)
    logger.info('Saving STAC structure to Azure')
    root_catalog.save(stac_io=az_stacio)


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

    te = pystac.TemporalExtent([const.DNB_START_DATE, const.DNB_START_DATE])
    #print(te.to_dict())
    ndt = const.DNB_START_DATE + datetime.timedelta(days=1)
    update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    print(te.to_dict())
    ndt = const.DNB_START_DATE + datetime.timedelta(days=5)
    update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    print(te.to_dict())

    ndt = const.DNB_START_DATE + datetime.timedelta(days=3)
    update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    print(te.to_dict())
    ndt = const.DNB_START_DATE + datetime.timedelta(days=4)
    update_temporal_extent(item_datetime=ndt, temporal_extent=te)
    print(te.to_dict())

