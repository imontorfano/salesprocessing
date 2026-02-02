import logging
import json
from datetime import datetime

def get_logger(name="sales_etl"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger

def log_event(logger, event_type, details):
    event = {
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        **details
    }
    logger.info(json.dumps(event))
