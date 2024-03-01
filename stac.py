import datetime
import json
import pystac
from osgeo import gdal
from shapely.geometry import Polygon, mapping
from tempfile import TemporaryDirectory
import os
from nighttimelights_pipeline import const
from nighttimelights_pipeline.azblob import blob_exists_in_azure
from nighttimelights_pipeline import utils as u
tmp_dir = TemporaryDirectory()


def create_stac_catalog(id='undp-stac', description='Geospatial data in COG format from UNDP GeoHub data store'):

    return pystac.Catalog(id=id, description=description)


def get_bbox_and_footprint(raster_path=None):
    ds = gdal.OpenEx(raster_path, gdal.OF_RASTER )
    ulx, xres, xskew, uly, yskew, yres = ds.GetGeoTransform()
    lrx = ulx + (ds.RasterXSize * xres)
    lry = uly + (ds.RasterYSize * yres)
    bbox = [ulx, lry, lrx, uly]
    footprint = Polygon([
        [ulx, uly],
        [lrx, uly],
        [lrx, ulx],
        [ulx, uly]
    ])
    return bbox, mapping(footprint)

def create_stac_item(item_path=None):

    bbox, footprint = get_bbox_and_footprint(raster_path=item_path)
    _, item_name = os.path.split(item_path)
    item_acquisition_date_str = item_name.split('_')[2].split('.')[0][1:]
    item_date = datetime.datetime.strptime(item_acquisition_date_str, '%Y%m%d')

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

    print(
            json.dumps(item.to_dict(), indent=4)
          )


def upload_cog_to_undp_stac(
        blob_path=None,
        container_name=const.AZURE_CONTAINER_NAME,
        collection_folder=const.AZURE_DNB_COLLECTION_FOLDER
    ):
    """

    :param blob_path:
    :param container_name:
    :param collection_folder:
    :return:
    """






if __name__ == '__main__':

    catalog = create_stac_catalog()
    # print(json.dumps( catalog.to_dict(), indent=4))
    path = '/work/tmp/ntl/SVDNB_npp_d20240125.rade9d_3857.tif'
    from nighttimelights_pipeline.utils import generate_id
    #print(generate_id(path))
    create_stac_item(item_path=path)
    print(json.dumps(catalog.to_dict(), indent=4))
    catalog.normalize_hrefs(os.path.join(tmp_dir.name, "stac"))