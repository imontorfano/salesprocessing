import os
import yaml
import logging
import argparse
from dotenv import load_dotenv
from sqlalchemy import create_engine

from etl.orchestrator import SalesETLOrchestrator

# ----------------------------
# ARGUMENTOS DE EJECUCIÓN
# ----------------------------
parser = argparse.ArgumentParser(description="Ejecutar ETL de ventas")
parser.add_argument(
    "--reprocess_from",
    type=str,
    help="Fecha desde la cual forzar reproceso (YYYY-MM-DD)"
)
args = parser.parse_args()

# ----------------------------
# CARGA CONFIGURACIÓN
# ----------------------------
load_dotenv()

with open("config.yaml") as f:
    config = yaml.safe_load(f)

# ----------------------------
# CONSTRUCCIÓN DB URL
# ----------------------------
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_schema = os.getenv("DB_SCHEMA", "public")

db_url = (
    f"postgresql+psycopg2://"
    f"{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

config["db"] = {
    "url": db_url,
    "schema": db_schema
}

# ----------------------------
# SOURCE
# ----------------------------
config["source"] = {
    "path": os.getenv("SOURCE_FILE"),
    "chunksize": int(os.getenv("CHUNKSIZE", 10000))
}

# ----------------------------
# SECURITY
# ----------------------------
config.setdefault("security", {})
config["security"]["encryption_key"] = os.getenv("ENCRYPTION_KEY")
config["security"]["hash_salt"] = os.getenv("HASH_SALT", "")

# ----------------------------
# LOGGER
# ----------------------------
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("sales_etl")

# ----------------------------
# REPROCESS_FROM
# ----------------------------
if args.reprocess_from:
    config["target"]["reprocess_from"] = args.reprocess_from

# ----------------------------
# ENGINE (POOL EXPLÍCITO)
# ----------------------------
engine = create_engine(
    db_url,
    pool_size=int(os.getenv("DB_POOL_SIZE", 5)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 0)),
    pool_pre_ping=True
)

logger.info(
    "DB Engine created | pool_size=%s | max_overflow=%s",
    os.getenv("DB_POOL_SIZE"),
    os.getenv("DB_MAX_OVERFLOW")
)

# ----------------------------
# EJECUTAR ETL
# ----------------------------
etl = SalesETLOrchestrator(
    config=config,
    engine=engine,
    logger=logger
)
etl.run()
