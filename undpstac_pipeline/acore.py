import multiprocessing
import asyncio
import numpy as np
from undpstac_pipeline.utils import should_download, transform_bbox
from undpstac_pipeline.colorado_eog import get_dnb_files, download_file
from undpstac_pipeline.const import COG_DNB_FILE_TYPE, AZURE_DNB_COLLECTION_FOLDER, COG_CONVERT_TIMEOUT,AIOHTTP_READ_CHUNKSIZE, COG_DOWNLOAD_TIMEOUT
import datetime
import logging
import os
import tqdm
from osgeo import gdal
from osgeo import gdal_array
from undpstac_pipeline.validate import validate
from undpstac_pipeline.azblob import  upload_file_to_blob
from undpstac_pipeline.stac import  push_to_stac
import math
gdal.UseExceptions()

logger = logging.getLogger(__name__)



def gdal_callback_pre(complete, message, data):
    timeout_event = data
    if timeout_event and timeout_event.is_set():
        logger.info(f'GDAL was signalled to stop...')
        return 0
def gdal_callback(complete, message, data):

    timeout_event, progressbar = data
    progressbar.update(complete*100-progressbar.n)
    if timeout_event and timeout_event.is_set():
        logger.info(f'GDAL was signalled to stop...')
        return 0


def set_metadata(src_path=None, dst_path=None, description=None):
    logger.info(f'Converting {src_path} to COG')
    if os.path.exists(dst_path): os.remove(dst_path)
    logger.info(f'Setting custom metadata ')
    ctime = datetime.datetime.fromtimestamp(os.path.getctime(src_path)).strftime('%Y%m%d%H%M%S')
    src_cog_ds = gdal.OpenEx(src_path, gdal.OF_UPDATE, )
    band = src_cog_ds.GetRasterBand(1)
    dtp = band.DataType
    # COGS do not like to be edited. So adding metadata will BREAK them
    src_cog_ds.SetMetadata({f'DNB_FILE_SIZE_{ctime}': f'{os.path.getsize(src_path)}'})
    band.SetMetadata({'DESCRIPTION': description})
    band.SetMetadata({'Source': 'Colorado School of mines'})
    band.SetMetadata({'Unit': 'nWcm-2sr-1'})
    del src_cog_ds
    return dtp




def scale_and_convert(src_arr=None, nodata=None, max_threshold=None, scale_factor=None, dtype=None ):
    """
    Convert float data to a different dtype

    """
    neg = src_arr < 0
    out_arr = (src_arr / scale_factor).astype(dtype)
    pos = out_arr > max_threshold
    out_arr[pos] = max_threshold
    out_arr[neg] = nodata
    return out_arr


def preprocess_dnb_bbox(src_path=None, scale_factor=0.01, dtype='uint16', timeout_event=None,
                        lonmin=None, latmin=None, lonmax=None, latmax=None):
    """
    Preprocess src_path as to reduce it's size. This si achieved by
        1 - changing the dtype to uint16 from float32
        2 - applying a scale factor = 0.01 because the original data was rounded to this precision

        The dtype and scale factor contrained the nodata value to be 655 and max value to be
        654. These are store in the processed GTIFF as 65500  respectively 65400
    """

    ii = np.iinfo(np.dtype(dtype))

    nodata = ii.max-35
    max_threshold = nodata-1

    src_ds = gdal.OpenEx(src_path, gdal.OF_READONLY|gdal.OF_RASTER)
    dst_path = f'{src_path}i'

    ulx, xres, xskew, uly, yskew, yres = src_ds.GetGeoTransform()
    if lonmin is None:lonmin = ulx
    if lonmax is None:lonmax =  ulx + (src_ds.RasterXSize * xres)
    if latmax is None:latmax = uly
    if latmin is None: latmin = uly + (src_ds.RasterYSize * yres)

    xminc = math.floor((lonmin - ulx) / xres)
    xmaxc = math.floor((lonmax - ulx) / xres )
    ymaxr = math.floor((latmin - uly) / yres)
    yminr = math.floor((latmax - uly) / yres)


    width = xmaxc-xminc + 1
    height = ymaxr-yminr + 1
    gtype = gdal_array.NumericTypeCodeToGDALTypeCode(np.dtype(dtype))
    dst_ds = gdal.GetDriverByName('GTiff').Create(dst_path, width, height, bands=1, eType=gtype,
                                                  options = [
                                                      'TILED=YES',
                                                      'BLOCKXSIZE=2560',
                                                      'BLOCKYSIZE=2560',
                                                      'BIGTIFF=IF_SAFER',
                                                      'COMPRESS=NONE',
                                                      'INTERLEAVE=BAND'
                                                  ],

                                              )

    dst_ds.SetGeoTransform([lonmin, xres, xskew, latmax, yskew, yres])
    dst_ds.SetProjection(src_ds.GetProjectionRef())
    band = dst_ds.GetRasterBand(1)
    band.SetScale(scale_factor)
    band.SetNoDataValue(nodata)
    #blocks = [e for e in gen_blocks(blockxsize=2650, blockysize=2650, width=width, height=src_ds.RasterYSize)]
    blocks = [e for e in gen_blocks1(
        ds=src_ds,
        blockxsize=2650, blockysize=2650,
        xminc=xminc, xmaxc=xmaxc, yminr=yminr, ymaxr=ymaxr
        )]

    with tqdm.tqdm(blocks, desc=f'Preprocessing {src_path}', total=len(blocks)) as pbar:
        for e in blocks:
            col_start, row_start, col_size, row_size = e

            block_data = src_ds.ReadAsArray(xoff=col_start, yoff=row_start, xsize=col_size, ysize=row_size,
                                            band_list=[1],
                                            callback=gdal_callback_pre,
                                            callback_data=timeout_event
                                           )

            out_block_data = scale_and_convert(src_arr=block_data,nodata=nodata,max_threshold=max_threshold,
                                               scale_factor=scale_factor, dtype=dtype)

            dst_ds.WriteArray(out_block_data, xoff=(col_start-xminc), yoff=row_start-yminr, band_list=[1],
                            callback = gdal_callback_pre,
                            callback_data = timeout_event
            )
            pbar.update()

    src_ds =None
    dst_ds = None

    return dst_path



def preprocess_dnb(src_path=None, scale_factor=0.01, dtype='uint16', timeout_event=None):
    """
    Preprocess src_path as to redcue it's size. Thsi si achieved by
        1 - changing the dtype to uint16 from float32
        2 - applying a scale factor = 0.01 because the original data was rounded to this precision

        The dtype and scale factor contrained the nodata value to be 655 and max value to be
        654. These are store in the processed GTIFF as 65500  respectively 65400
    """

    ii = np.iinfo(np.dtype(dtype))

    nodata = ii.max-35
    max_threshold = nodata-1

    src_ds = gdal.OpenEx(src_path, gdal.OF_READONLY|gdal.OF_RASTER)
    dst_path = f'{src_path}i'
    width = src_ds.RasterXSize
    height = src_ds.RasterYSize
    gtype = gdal_array.NumericTypeCodeToGDALTypeCode(np.dtype(dtype))

    dst_ds = gdal.GetDriverByName('GTiff').Create(dst_path, width, height, bands=1, eType=gtype,
                                                  options = [
                                                      'TILED=YES',
                                                      'BLOCKXSIZE=2560',
                                                      'BLOCKYSIZE=2560',
                                                      'BIGTIFF=IF_SAFER',
                                                      'COMPRESS=NONE',
                                                      'INTERLEAVE=BAND'
                                                  ],

                                              )

    dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
    dst_ds.SetProjection(src_ds.GetProjectionRef())
    band = dst_ds.GetRasterBand(1)
    band.SetScale(scale_factor)
    band.SetNoDataValue(nodata)
    blocks = [e for e in gen_blocks(blockxsize=2650, blockysize=2650, width=width, height=src_ds.RasterYSize)]

    with tqdm.tqdm(blocks, desc=f'Preprocessing {src_path}', total=len(blocks)) as pbar:
        for e in blocks:
            col_start, row_start, col_size, row_size = e
            block_data = src_ds.ReadAsArray(xoff=col_start, yoff=row_start, xsize=col_size, ysize=row_size,
                                            band_list=[1],
                                            callback=gdal_callback_pre,
                                            callback_data=timeout_event
                                           )
            out_block_data = scale_and_convert(src_arr=block_data,nodata=nodata,max_threshold=max_threshold,
                                               scale_factor=scale_factor, dtype=dtype)
            dst_ds.WriteArray(out_block_data, xoff=col_start, yoff=row_start, band_list=[1],
                            callback = gdal_callback_pre,
                            callback_data = timeout_event
            )
            pbar.update()

    src_ds =None
    dst_ds = None

    return dst_path




def gen_blocks1(ds=None,blockxsize=None, blockysize=None, xminc=None, yminr=None, xmaxc=None, ymaxr=None ):
    """
    Generate reading block for gdal ReadAsArray
    """


    width = ds.RasterXSize
    height = ds.RasterXSize
    wi = list(range(0, width, blockxsize))
    if width % blockxsize != 0:
        wi += [width]
    hi = list(range(0, height, blockysize))
    if height % blockysize != 0:
        hi += [height]
    for col_start, col_end in zip(wi[:-1], wi[1:]):
        col_size = col_end - col_start
        if  xminc > col_end or xmaxc < col_start:continue
        if col_start < xminc:col_start = xminc
        if col_start+col_size>xmaxc:col_size=xmaxc-col_start
        for row_start, row_end in zip(hi[:-1], hi[1:]):
            if yminr > row_end or ymaxr < row_start :continue
            if row_start<yminr:row_start=yminr
            row_size = row_end - row_start
            if row_start+row_size>ymaxr:row_size= ymaxr-row_start
            yield col_start, row_start, col_size, row_size



def gen_blocks(blockxsize=None, blockysize=None, width=None, height=None ):
    """
    Generate reading block for gdal ReadAsArray
    """
    wi = list(range(0, width, blockxsize))
    if width % blockxsize != 0:
        wi += [width]
    hi = list(range(0, height, blockysize))
    if height % blockysize != 0:
        hi += [height]
    for col_start, col_end in zip(wi[:-1], wi[1:]):
        col_size = col_end - col_start
        for row_start, row_end in zip(hi[:-1], hi[1:]):
            row_size = row_end - row_start
            yield col_start, row_start, col_size, row_size











def warp_cog(
        src_path=None, dst_path=None,
        timeout_event=None,description=None,
        lonmin=-180, latmin=-65, lonmax=178, latmax=75

    ):

    dtp = set_metadata(src_path=src_path, dst_path=dst_path, description=description)
    with tqdm.tqdm(range(0, 100), desc=f'running gdalwarp on {src_path}', unit_scale=True) as progressbar:

        wo = gdal.WarpOptions(
            format='COG',
            warpMemoryLimit=500,
            srcSRS='EPSG:4326',
            dstSRS='EPSG:3857',
            overviewLevel=None,
            #outputType= gdal.GDT_Int16 if dtp == gdal.GDT_Float32 else dtp,
            multithread=True,
            outputBounds=[lonmin, latmin, lonmax, latmax],  # <xmin> <ymin> <xmax> <ymax>
            outputBoundsSRS='EPSG:4326',
            resampleAlg='NEAREST',
            targetAlignedPixels=True,
            xRes=500,
            yRes=500,
            creationOptions=[
                "BLOCKSIZE=256",
                "OVERVIEWS=IGNORE_EXISTING",
                "COMPRESS=ZSTD",
                "LEVEL=9",
                "PREDICTOR=YES",
                "OVERVIEW_RESAMPLING=NEAREST",
                "BIGTIFF=IF_SAFER",
                "NUM_THREADS=ALL_CPUS",
                "ADD_ALPHA=NO",
                "STATISTICS=YES"
            ],
            copyMetadata=True,
            callback=gdal_callback,
            callback_data=(timeout_event, progressbar)

        )
        cog_ds = gdal.Warp(
            srcDSOrSrcDSTab=src_path,
            destNameOrDestDS=dst_path,
            options=wo,
        )


    del cog_ds
    warnings, errors, details = validate(dst_path, full_check=True)
    if warnings:
        for wm in warnings:
            logger.warning(wm)
    if errors:
        for em in errors:
            logger.error(em)
        raise Exception('\n'.join(errors))
    return dst_path

def translate_cog(
        src_path=None, dst_path=None,
        timeout_event=None,description=None,
        lonmin=-179.9999, latmin=-65, lonmax=179.9999, latmax=75

    ):

    set_metadata(src_path=src_path, dst_path=dst_path, description=description)

    logger.info(f'running gdal_translate on {src_path}')
    progressbar = tqdm.tqdm(range(0, 100), desc=f'Creating COG {dst_path}', unit_scale=True)

    xmin, ymin, xmax, ymax = transform_bbox(lonmin=lonmin,lonmax=lonmax, latmin=latmin, latmax=latmax)

    cog_ds = gdal.Translate(
        destName=dst_path,
        srcDS=src_path,
        format='COG',
        creationOptions=[

            "BLOCKSIZE=256",
            "OVERVIEWS=IGNORE_EXISTING",
            "COMPRESS=ZSTD",
            "LEVEL=9",
            "PREDICTOR=YES",
            "OVERVIEW_RESAMPLING=NEAREST",
            "BIGTIFF=IF_SAFER",
            "TARGET_SRS=EPSG:3857",
            #"RES=500",
            #f"EXTENT={xmin},{ymin},{xmax},{ymax}"
            "RESAMPLING=NEAREST",
            # "STATISTICS=YES",
            "ADD_ALPHA=NO",
            #"COPY_SRC_MDD=YES"

        ],
        stats=True,
        xRes= 0.00449,
        yRes= 0.00449,
        projWin=(lonmin, latmax, lonmax, latmin),
        projWinSRS='EPSG:4326',
        callback=gdal_callback,
        callback_data=(timeout_event, progressbar)
    )
    del cog_ds
    warnings, errors, details = validate(dst_path, full_check=True)
    if warnings:
        for wm in warnings:
            logger.warning(wm)
    if errors:
        for em in errors:
            logger.error(em)
        raise Exception('\n'.join(errors))
    return dst_path


async def process_nighttime_data(date: datetime.date = None,
                                 file_type=COG_DNB_FILE_TYPE,
                                 DOWNLOAD_TIMEOUT=COG_DOWNLOAD_TIMEOUT,
                                 CONVERT_TIMEOUT=COG_CONVERT_TIMEOUT,
                                 lonmin=None,
                                 latmin=None,
                                 lonmax=None,
                                 latmax=None,
                                 force_processing=False,
                                 concurrent=False,
                                 archive=False
                                 ):
    """

    :param date: the date for which the mosaics are produced
    :param file_type:
    :param DOWNLOAD_TIMEOUT:
    :param CONVERT_TIMEOUT:
    :return:
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    timeout_event = multiprocessing.Event()

    try:

        remote_dnb_files = get_dnb_files(date=date,file_type=file_type)


        dnb_file_names = dict()
        azure_dnb_cogs =  dict()
        downloaded_dnb_files = dict()
        local_cog_files = dict()
        for k, v in remote_dnb_files.items():
            furl, fdesc = v
            _, fname = os.path.split(furl)
            dnb_file_names[k] = fname
            azure_dnb_cogs[k] = os.path.join(AZURE_DNB_COLLECTION_FOLDER,year,month, day, fname)

        cog_dnb_blob_path = azure_dnb_cogs[file_type]
        remote_dnb_file = remote_dnb_files[file_type][0]
        if force_processing:
            will_download = force_processing
        else:
            will_download=should_download(blob_name=cog_dnb_blob_path,remote_file_url=remote_dnb_file)
        if will_download:
            logger.info(f'Processing nighttime lights from Colorado EOG for {date}')
            ################### download from remote  ########################
            download_futures = list()
            for dnb_file_type, remote in remote_dnb_files.items():
                dnb_file_url, fdesc = remote
                download_task = asyncio.ensure_future(
                    download_file(file_url=dnb_file_url, read_chunk_size=AIOHTTP_READ_CHUNKSIZE)
                )
                download_task.set_name(dnb_file_type)
                download_futures.append(download_task)
            downloaded, not_downloaded = await asyncio.wait(download_futures, timeout=DOWNLOAD_TIMEOUT,
                                                   return_when=asyncio.FIRST_EXCEPTION)

            if downloaded:
                m = []
                for downloaded_future in downloaded:
                    try:
                        downloaded_file = await downloaded_future
                        task_name = downloaded_future.get_name()
                        logger.info(f'Successfully downloaded {task_name} to {downloaded_file}')
                        downloaded_dnb_files[task_name] = downloaded_file
                    except Exception as e:
                        m.append(str(e))
                if m:
                    raise Exception('\n'.join(m))

            if not_downloaded:
                m = []
                for pending_future in not_downloaded:
                    try:
                        pending_future.cancel()
                        await pending_future
                    except asyncio.exceptions.CancelledError as e:
                        dnb_file_type = pending_future.get_name()
                        msg = f'Failed to download {dnb_file_type} from {remote_dnb_files[dnb_file_type][0]}'
                        m.append(msg)
                if m:
                    raise Exception('\n'.join(m))

            ################### preprocess DNB ########################

            down_dnb_file = downloaded_dnb_files[file_type]

            preproc_dnb_file=preprocess_dnb_bbox(src_path=down_dnb_file,
                                                 timeout_event=timeout_event,
                                                 lonmin=lonmin, latmin=latmin, lonmax=lonmax, latmax=latmax
                                                 )

            downloaded_dnb_files[file_type] = preproc_dnb_file

            ################### convert to COG ########################
            if concurrent:
                convert2cogtasks = list()
                for dnb_file_type, downloaded_dnb_file in downloaded_dnb_files.items():
                    local_cog_file = os.path.splitext(downloaded_dnb_file)[0]
                    _, dnb_file_desc = remote_dnb_files[dnb_file_type]
                    cog_task = asyncio.ensure_future(
                        asyncio.to_thread(warp_cog,
                                          src_path=downloaded_dnb_file,
                                          dst_path=local_cog_file,
                                          timeout_event=timeout_event,
                                          description=dnb_file_desc,
                                          lonmin=lonmin, latmin=latmin, lonmax=lonmax, latmax=latmax
                                          )
                    )
                    cog_task.set_name(dnb_file_type)
                    convert2cogtasks.append(cog_task)

                done, pending = await asyncio.wait(
                    convert2cogtasks,
                    return_when=asyncio.FIRST_EXCEPTION,
                    timeout=CONVERT_TIMEOUT,
                )

                if len(done) == 0:
                    error_message = f'Failed to convert {list(downloaded_dnb_files.values())} to COG in {CONVERT_TIMEOUT} seconds.'
                    logger.error(error_message)
                    timeout_event.set()
                pm = []
                for pending_future in pending:
                    try:
                        pending_future.cancel()
                        await pending_future
                    except asyncio.exceptions.CancelledError as e:
                        pm.append(str(e))
                dm = []
                for done_future in done:
                    try:
                        converted_cog_path = await done_future
                        dnb_file_type = done_future.get_name()
                        logger.info(f'Successfully created {dnb_file_type} COG {converted_cog_path}')
                        local_cog_files[dnb_file_type] = converted_cog_path

                    except Exception as e:
                        dm.append(str(e))
                if pm:raise Exception('\n'.join(pm))
                if dm:raise Exception('\n'.join(dm))
            else:
                try:
                    for dnb_file_type, downloaded_dnb_file in downloaded_dnb_files.items():
                        local_cog_file = os.path.splitext(downloaded_dnb_file)[0]
                        _, dnb_file_desc = remote_dnb_files[dnb_file_type]
                        converted_cog_path = warp_cog(
                            src_path = downloaded_dnb_file,
                            dst_path = local_cog_file,
                            timeout_event = timeout_event,
                            description = dnb_file_desc,
                            lonmin = lonmin, latmin = latmin, lonmax = lonmax, latmax = latmax
                        )
                        local_cog_files[dnb_file_type] = converted_cog_path
                except Exception as eee:
                    if 'user terminated' in str(eee):
                        raise KeyboardInterrupt
                    else:
                        raise eee

            ################### upload to azure########################
            for dnb_file_type, local_cog_file in local_cog_files.items():
                cog_blob_pth = azure_dnb_cogs[dnb_file_type]
                logger.info(f'Uploading {dnb_file_type} from {local_cog_file} to {cog_blob_pth}')
                upload_file_to_blob(src_path=local_cog_file, dst_path=cog_blob_pth)
            ################### update stac ########################

            push_to_stac(
                local_cog_files=local_cog_files,
                azure_cog_files=azure_dnb_cogs,
                file_type=file_type
            )

        else:
            logger.info(f'No nighttime lights data will be processed for {date} from Colorado EOG ')

    except asyncio.CancelledError as ce:
        logger.info(f'Cancelling all tasks and actions...')
        timeout_event.set()
        raise ce

    except Exception as e:
        timeout_event.set()
        logger.error(f"Failed to process data for {date.strftime('%Y-%m-%d')}: {e.__class__.__name__} {e} ")
        if 'user terminated' in str(e).lower():
            raise KeyboardInterrupt
        else:
            raise e


