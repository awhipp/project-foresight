import logging


def generate_logger(
    name: str,
    log_level: str = "INFO",
    formatter_str: str = "%(asctime)s %(levelname)-8s %(message)s",
) -> logging.Logger:
    """Generate a logger with the given name and log level.

    Args:
        name (str): The name of the logger.
        log_level (str, optional): The log level. Defaults to "INFO".
        formatter_str (str, optional): The formatter string. Defaults to "".

    Returns:
        logging.Logger: The logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    formatter = logging.Formatter(formatter_str)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
