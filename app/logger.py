import logging
import os

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger("nba_dashboard")
logger.setLevel(logging.DEBUG)

# Console: INFO and above
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

# File: ERROR and above only — keeps logs/error.log clean
_file = logging.FileHandler(os.path.join(LOG_DIR, "error.log"), encoding="utf-8")
_file.setLevel(logging.ERROR)
_file.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

logger.addHandler(_console)
logger.addHandler(_file)
