import logging
import io
import subprocess
logger = logging.getLogger(__name__)
def reproject_and_convert_to_cog(input_path: str, output_path: str, timeout_event=None):
    """
    Reproject and convert to COG
    :param input_path: str
    :param output_path: str
    :param timeout_event: multiprocessing.Event
    :return: None
    """
    reprojection_cmd = [
        'gdalwarp',
        '-s_srs', 'EPSG:4326',
        '-t_srs', 'EPSG:3857',
        '-of', 'COG',  # 'COG
        '-r', 'near',
        '-ovr', 'NONE',
        '-wo', 'NUM_THREADS=ALL_CPUS',
        '-co', 'BLOCKSIZE=256',
        '-co', 'OVERVIEWS=IGNORE_EXISTING',
        '-co', 'COMPRESS=ZSTD',
        '-co', 'PREDICTOR=YES',
        '-co', 'OVERVIEW_RESAMPLING=NEAREST',
        '-co', 'BIGTIFF=YES',
        '-overwrite',
        input_path,
        output_path
    ]
    # subprocess.run(reprojection_cmd, check=True)
    with subprocess.Popen(reprojection_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:

        with p.stdout:
            stream = io.open(p.stdout.fileno(), closefd=False)
            for line in stream:
                logger.info(line.strip('\r').strip('\n'))
            while p.poll() is None:
                output = stream.readline().strip('\r').strip('\n')
                if output:
                    logger.debug(output)
                if timeout_event and timeout_event.is_set():
                    logger.error(f'Terminating process')
                    p.terminate()
                    logger.error("Terminated process")
                    raise TimeoutError("Process took too long")
        with p.stderr:
            stream = io.open(p.stderr.fileno(), closefd=False)
            for line in stream:
                logger.info(line.strip('\r').strip('\n'))
            while p.poll() is None:
                err = stream.readline().strip('\r').strip('\n')
                if err:
                    logger.debug(err)
                if timeout_event and timeout_event.is_set():
                    logger.error(f'Terminating process')
                    p.terminate()
                    logger.error("Terminated process")
                    raise TimeoutError("Process took too long")
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)