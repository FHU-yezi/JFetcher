from sspeedup.logging.run_logger import RunLogger

from utils.config import CONFIG
from utils.db import RUN_LOG_COLLECTION

logger = RunLogger(
    save_level=CONFIG.log.save_level,
    print_level=CONFIG.log.print_level,
    mongo_collection=RUN_LOG_COLLECTION,
)
