# TODO: add docstring

import logging
from pathlib import Path


def setup_logger(
    logger_name, log_file=None, level=logging.INFO,
    formatter_str='%(asctime)s - %(name)s - %(levelname)s: %(message)s'
):
    """Setup logger with a given name. Creates a log folder if it does not exist."""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    formatter = logging.Formatter(formatter_str)

    # console output
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    # file output
    if log_file is not None:
        # TODO: spilt to path and file_name parameters
        Path(log_file.rsplit('/', 1)[0]).mkdir(parents=True, exist_ok=True)
        fileHandler = logging.FileHandler(log_file, encoding='utf-8')
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
