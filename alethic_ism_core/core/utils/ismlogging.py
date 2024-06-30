import logging
import dotenv
import os

dotenv.load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def ism_logger(name: str):
    logger = logging.getLogger(name)
    logger.info(f'initialized logging with level {LOG_LEVEL}')
    return logger
