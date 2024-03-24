FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.4

RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app

COPY . /app

RUN pip3 install --no-cache-dir aiohttp shapely azure-storage-blob python-dotenv tqdm pystac aiofiles


CMD ["python3", "-m",  "undpstac_pipeline.cli"]
