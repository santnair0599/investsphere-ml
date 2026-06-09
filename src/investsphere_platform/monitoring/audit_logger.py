"""A tiny helper to get a configured logger. One simple function."""
import logging


def get_logger(name="investsphere"):
    """Return a logger that prints time, level and message."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # only configure once
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
