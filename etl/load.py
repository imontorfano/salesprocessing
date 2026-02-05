from sqlalchemy import text
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd


def build_upsert_statement(table_full_name, columns, primary_key):
    insert_cols = ", ".join(columns)
    values_cols = ", ".join([f":{c}" for c in columns])
    conflict_cols = ", ".join(primary_key)
    update_cols = [c for c in columns if c not in primary_key]

    update_clause = (
        "DO UPDATE SET " + ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        if update_cols else "DO NOTHING"
    )

    return f"""
        INSERT INTO {table_full_name} ({insert_cols})
        VALUES ({values_cols})
        ON CONFLICT ({conflict_cols})
        {update_clause}
    """


def _load_chunk(engine, upsert_sql, records, chunk_id, logger):
    logger.info(f"[LOAD] Iniciando carga chunk {chunk_id} ({len(records)} registros)")
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)
    logger.info(f"[LOAD] Chunk {chunk_id} cargado correctamente")


def load_data(engine, df, table_name, schema, table_contract, logger=None, run_id=None, max_incremental=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    if df.empty:
        logger.info("[LOAD] No hay registros para cargar")
        return

    table_full_name = f"{schema}.{table_name}"
    primary_key = table_contract.get("primary_key", [])
    last_run_type = "reprocess" if table_contract.get("reprocess_from") else "normal"

    # -------------------
    # SQL Upsert
    # -------------------
    columns = list(df.columns)
    upsert_sql = build_upsert_statement(table_full_name, columns, primary_key)
    logger.info(f"[LOAD] Iniciando carga de {len(df)} registros a {table_full_name}")

    # -------------------
    # Chunks + concurrencia
    # -------------------
    chunk_size = int(os.getenv("CHUNKSIZE", 10000))
    max_workers = int(os.getenv("MAX_WORKERS", 1))
    num_chunks = math.ceil(len(df) / chunk_size)

    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(num_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk_df = df.iloc[start:end]
            records = chunk_df.to_dict(orient="records")

            futures.append(
                executor.submit(
                    _load_chunk,
                    engine,
                    upsert_sql,
                    records,
                    i + 1,
                    logger
                )
            )

        for future in as_completed(futures):
            future.result()

    logger.info("[LOAD] Carga finalizada correctamente")

    # -------------------
    # Actualizar tabla de control
    # -------------------
    if max_incremental is not None:
        now = datetime.utcnow()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sales.etl_control 
                    (table_name, last_loaded_timestamp, last_run_id, last_run_type, updated_at)
                VALUES (:tbl, :ts, :run_id, :run_type, :updated_at)
                ON CONFLICT (table_name)
                DO UPDATE SET
                    last_loaded_timestamp = EXCLUDED.last_loaded_timestamp,
                    last_run_id = EXCLUDED.last_run_id,
                    last_run_type = EXCLUDED.last_run_type,
                    updated_at = EXCLUDED.updated_at
            """), {
                "tbl": table_name,
                "ts": max_incremental,
                "run_id": run_id or str(pd.Timestamp.utcnow().value),
                "run_type": last_run_type,
                "updated_at": now
            })
        logger.info(f"[LOAD] Tabla de control sales.etl_control actualizada con timestamp {max_incremental}")
