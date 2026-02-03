import pandas as pd
from sqlalchemy import text
from datetime import datetime
import logging


def extract_csv(path, logger=None, engine=None, table_name=None, incremental_col=None, reprocess_from=None):
    """
    Lee el CSV y filtra incrementalmente según la tabla etl_control.
    Devuelve el DataFrame filtrado y el máximo de la columna incremental en el CSV.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # -------------------
    # Leer CSV
    # -------------------
    logger.info(f"[EXTRACT] Extrayendo datos desde {path}")
    df = pd.read_csv(path)

    if df.empty:
        logger.info("[EXTRACT] CSV vacío, no hay datos para procesar")
        return df, None

    # -------------------
    # Calcular máximo timestamp del CSV
    # -------------------
    max_incremental = None
    if incremental_col in df.columns:
        df[incremental_col] = pd.to_datetime(df[incremental_col], errors="coerce")
        max_incremental = df[incremental_col].max()
        logger.info(f"[EXTRACT] Max {incremental_col} en CSV: {max_incremental}")

    # -------------------
    # Filtro incremental usando etl_control
    # -------------------
    if engine is not None and table_name and incremental_col:
        last_value = None
        try:
            with engine.connect() as conn:
                if reprocess_from:
                    last_value = pd.to_datetime(reprocess_from)
                    logger.info(f"[EXTRACT] Reproceso activado desde {last_value}")
                else:
                    res = conn.execute(text(f"""
                        SELECT last_loaded_timestamp
                        FROM sales.etl_control
                        WHERE table_name = :tbl
                    """), {"tbl": table_name})
                    last_value = res.scalar()
                    if last_value:
                        last_value = pd.to_datetime(last_value)
                        logger.info(f"[EXTRACT] Última fecha cargada en etl_control: {last_value}")
        except Exception as e:
            logger.warning(f"[EXTRACT] No se pudo leer la última fecha de ETL: {e}")
            last_value = None

        if last_value is not None:
            df = df[df[incremental_col] > last_value]
            logger.info(f"[EXTRACT] Filtrado incremental aplicado: {len(df)} registros nuevos")

    return df, max_incremental
