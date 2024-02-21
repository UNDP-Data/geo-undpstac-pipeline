import pystac
from osgeo import gdal
from shapely.geometry import Polygon, mapping


def create_stac_catalog(id='nighttime-lights', description='Night-time lighst nightly mosaics from Colorado EOG in COG format'):

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

def add_item_to_catalog(catalog_path=None, item_path=None):
    pass

if __name__ == '__main__':


    path = '/work/tmp/ntl/SVDNB_npp_d20240125.rade9d_3857.tif'