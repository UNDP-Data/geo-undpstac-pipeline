import logging


class ProgressHandler(logging.StreamHandler):
    progress = False
    spit = False
    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """

        try:
            stream = self.stream
            if not self.progress:
                if self.spit:stream.write(' |' + self.terminator)
                msg = self.format(record)
                # issue 35046: merged two stream.writes into one.
                stream.write(msg + self.terminator)
                self.flush()
            else:

                if not self.spit:stream.write('| ')
                value = int(float(record.getMessage()))

                if value % 10 == 0:
                    stream.write(f'{value}')
                if value%3==0 and not value%10==0:
                    stream.write('.')
                self.spit = True
        except RecursionError:  # See issue 36272
            raise
        except Exception as e:
            print(e)
            self.handleError(record)

def __setattr__(self, name, value):
    if name == 'progress':
        self.handlers[0].progress = value
    else:
        return object.__setattr__(self, name, value)

def main():
    import sys
    logger = logging.getLogger(__name__)
    logger.__class__.__setattr__ = __setattr__
    stream_handler = ProgressHandler(stream=sys.stdout)
    stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(stream_handler)
    logger.name = 'nighttimelights'
    print(111, end='', flush=True)
    print(222, flush=True)



    logger.info('regular log')
    logger.info('enabling progress log')

    logger.progress = True
    for i in range(101):
        logger.info(f'{i+ 0.0002}')
    logger.progress = False
    logger.info('again regular')
if __name__ == '__main__':
    #main()
    local_cog_file_path = '/tmp/SVDNB_npp_d20240125.rade9d.tif.locali'
    from osgeo import gdal
    info = gdal.Info(local_cog_file_path, format='json')
    print(info['bands'][0].keys())