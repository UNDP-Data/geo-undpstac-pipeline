import datetime
import json
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
from nighttimelights_pipeline import const
from nighttimelights_pipeline.azblob import blob_exists_in_azure, get_container_client, upload, download
from nighttimelights_pipeline import utils as u
from pystac.layout import TemplateLayoutStrategy
import logging

tmp_dir = TemporaryDirectory()
logger = logging.getLogger(__name__)



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
        root_href=None
    ):


    catalog = pystac.Catalog(
        id=id,
        description=description,
        title=title,
        href=os.path.join(root_href, const.STAC_CATALOG_NAME) ,
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED
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

def create_dnb_stac_item(daily_dnb_blob_path=None, daily_dnb_cloudmask_blob_path=None, add_eo_extension=True):


    container_client = get_container_client()

    dnb_blob_client = container_client.get_blob_client(blob=daily_dnb_blob_path)
    dnb_cloudmask_blob_client = container_client.get_blob_client(blob=daily_dnb_cloudmask_blob_path)

    # both assets have same exptent
    bbox, footprint = get_bbox_and_footprint(raster_path=dnb_blob_client.url)
    _, item_name = os.path.split(daily_dnb_blob_path)
    item_date = u.extract_date_from_dnbfile(item_name)

    item = pystac.Item(#id=u.generate_id(name=item_path),
                        id=f'nighttime-lights-{item_date.strftime("%Y-%m-%d")}',
                        #id=f'SVDNB_npp_d{item_date.strftime("%Y%m%d")}',
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
        key='DNB',
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
        key='DNBCLM',
        asset=clm_asset
    )

    return item


def create_nighttime_lights_collection():


    spatial_extent = pystac.SpatialExtent(bboxes=[const.DNB_BBOX])
    collection_interval = [const.DNB_START_DATE, const.DNB_START_DATE]
    temporal_extent = pystac.TemporalExtent(intervals=collection_interval)
    col = pystac.Collection(id='nighttime-lights',description='a good description', title='Nighthly nighttime lights mosaics',
                            license='Creative Commons Attribution 4.0 International',
                            providers=[
                                Provider(name='Colorado Schools of Mines',roles=[ProviderRole.PRODUCER, ProviderRole.LICENSOR]),
                                Provider(name='UNDP', description='United Nations Development Programme', roles=[ProviderRole.PROCESSOR, ProviderRole.HOST])

                            ],
                            extent=pystac.Extent(spatial=spatial_extent, temporal=temporal_extent),
                            catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED
                            )
    return col


def create_undp_stac(container_name=const.AZURE_CONTAINER_NAME,
                     collection_folder=const.AZURE_DNB_COLLECTION_FOLDER):
    az_stacio = AzureStacIO()

    root_catalog_blob_path = const.STAC_CATALOG_NAME
    print(blob_exists_in_azure(blob_path=root_catalog_blob_path, container_client=az_stacio.container_client))
    root_catalog_url = f'{az_stacio.container_client.url}/catalog.json'
    c = pystac.Catalog.from_file(root_catalog_url, stac_io=az_stacio )
    c.describe()

    logger.info('...creating ROOT STAC catalog')
    root_catalog = create_stac_catalog(
        id='undp-stac',
        description='Geospatial data in COG format from UNDP GeoHub data store',
        title='VIIRS DNB/nighttime lights daily mosaics',
        root_href=container_client.url
    )

    logger.info('...creating nighttime lights STAC collection')
    nighttime_collection = create_nighttime_lights_collection()
    # add collection to root catalog
    root_catalog.add_child(nighttime_collection )
    # save to azure through
    root_catalog.save(stac_io=az_stacio)

def update_undp_stac(
        daily_dnb_blob_path=None,
        daily_dnb_cloudmask_blob_path=None,
        container_name=const.AZURE_CONTAINER_NAME,
        collection_folder=const.AZURE_DNB_COLLECTION_FOLDER
    ):
    """

    :param blob_path:
    :param container_name:
    :param collection_folder:
    :return:
    """
    az_stacio = AzureStacIO()
    container_client = get_container_client()
    root_catalog_blob_path = container_client.url


    root_catalog_exists =blob_exists_in_azure(root_catalog_blob_path)
    dnb_collection_blob_path = os.path.join(root_catalog_blob_path, collection_folder, 'collection.json')
    dnb_collection_exists = blob_exists_in_azure(dnb_collection_blob_path)


    if not root_catalog_exists :
        logger.info('...creating ROOT STAC catalog')
        root_catalog = create_stac_catalog(
            id='undp-stac',
            description='Geospatial data in COG format from UNDP GeoHub data store',
            title='VIIRS DNB/nighttime lights daily mosaics',
            root_href=container_client.url
        )
    else:
        root_catalog = pystac.Catalog.from_file(root_catalog_blob_path,stac_io=az_stacio)
    print(json.dumps(root_catalog.to_dict(), indent=4))
    if not dnb_collection_exists:
        logger.info('...creating STAC collection')
        nighttime_collection = create_nighttime_lights_collection()
        root_catalog.add_child(nighttime_collection,)
        root_catalog.save(stac_io=az_stacio)

    else:
        nighttime_collection = pystac.Collection.from_file(dnb_collection_blob_path,)

    print(json.dumps(nighttime_collection.to_dict(), indent=4))
    exit()
    pth, blob_name = os.path.split(daily_dnb_blob_path)
    item_date = u.extract_date_from_dnbfile(blob_name)
    year = item_date.strftime('%Y')
    month = item_date.strftime('%m')
    day = item_date.strftime('%d')

    dnb_year_catalog_blob_path = os.path.join(collection_folder, year, 'catalog.json')
    if not blob_exists_in_azure(dnb_year_catalog_blob_path):
        year_catalog = create_stac_catalog(id=f'nighttime-lights-{year}',title=f'Nighttime lights in {year}')
    else:
        year_catalog = pystac.Catalog.from_file(dnb_year_catalog_blob_path)

    dnb_year_month_catalog_blob_path = os.path.join(collection_folder, year, month, 'catalog.json')
    if not blob_exists_in_azure(dnb_year_month_catalog_blob_path):
        year_month_catalog = create_stac_catalog(id=f'nighttime-lights-{year}-{month}', title=f'Nighttime lights in {year}-{month}')
    else:
        year_month_catalog = pystac.Catalog.from_file(dnb_year_month_catalog_blob_path)

    # dnb_item_path = daily_dnb_blob_path.replace('.tif', '.json')
    dnb_item_path = daily_dnb_cloudmask_blob_path.replace('.vcld.tif', '.json')
    daily_dnb_item = create_dnb_stac_item(
        daily_dnb_blob_path=daily_dnb_blob_path,
        daily_dnb_cloudmask_blob_path=daily_dnb_cloudmask_blob_path,
        add_eo_extension=False
    )

    # going back in reverse
    # 1. upload item
    # upload(dst_path=dnb_item_path,
    #        data=json.dumps(daily_dnb_item.to_dict(), indent=4).encode('utf-8'),
    #        content_type='application/json')
    #2 upload monthly catalog
    year_month_catalog.add_item(daily_dnb_item)
    p = os.path.join(collection_folder, year, month,)
    print(p)
    template = TemplateLayoutStrategy(item_template=const.STAC_ITEM_TEMPLATE)
    year_month_catalog.normalize_and_save(root_href='./out/',
                                          strategy= template,
                                          catalog_type=pystac.CatalogType.SELF_CONTAINED
                                          )
    print(json.dumps(year_month_catalog.to_dict(), indent=4))
    print(json.dumps(daily_dnb_item.to_dict(), indent=4))


if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    #catalog = create_stac_catalog(title="UNDP STAC data store")
    # print(json.dumps( catalog.to_dict(), indent=4))
    path = '/work/tmp/ntl/SVDNB_npp_d20240125.rade9d_3857.tif'
    from nighttimelights_pipeline.utils import generate_id
    #print(generate_id(path))
    item = create_stac_item(item_path=path)
    d = item.to_dict()
    #d = catalog.to_dict()
    #print(json.dumps(d, indent=4))
    #catalog.normalize_hrefs(os.path.join(tmp_dir.name, "stac"))
    blob_path = os.path.join(const.AZURE_DNB_COLLECTION_FOLDER,'2024/01/SVDNB_npp_d20240125.rade9d.tif')
    clmask_blob_path = os.path.join(const.AZURE_DNB_COLLECTION_FOLDER,'2024/01/SVDNB_npp_d20240125.vcld.tif')
    print(blob_path)
    #update_undp_stac(daily_dnb_blob_path=blob_path, daily_dnb_cloudmask_blob_path=clmask_blob_path)
    create_undp_stac()