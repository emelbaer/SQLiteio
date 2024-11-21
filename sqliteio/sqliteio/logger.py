import logging
import os

def setup_logger(name: str, log_file: str = None, console_level: int = logging.DEBUG, file_level: int = logging.DEBUG):
    """
    Set up a logger with console and optional file handlers.

    Args:
        name (str): Name of the logger.
        log_file (str, optional): Path to the log file. If None, logs are only sent to the console.
        console_level (int, optional): Logging level for the console handler. Defaults to logging.DEBUG.
        file_level (int, optional): Logging level for the file handler. Defaults to logging.DEBUG.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        # Avoid adding handlers multiple times
        return logger

    logger.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)  # Ensure the directory exists
        fh = logging.FileHandler(log_file)
        fh.setLevel(file_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
