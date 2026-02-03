from sqlalchemy import text
import logging
import math
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


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
    logger.info(f"Iniciando carga chunk {chunk_id} ({len(records)} registros)")
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)
    logger.info(f"Chunk {chunk_id} cargado correctamente")


def load_data(engine, df, table_name, schema, table_contract, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    if df.empty:
        logger.info("No hay registros para cargar")
        return

    table_full_name = f"{schema}.{table_name}"
    primary_key = table_contract.get("primary_key", [])
    incremental_col = table_contract.get("incremental_field")
    reprocess_from = table_contract.get("reprocess_from")

    # -------------------
    # 1️⃣ Incremental / Reprocess
    # -------------------
    if incremental_col and incremental_col in df.columns:
        with engine.connect() as conn:
            if reprocess_from:
                last_value = pd.to_datetime(reprocess_from).date()
                logger.info(f"Reprocesando desde {last_value}")
            else:
                last_value = conn.execute(
                    text(f"SELECT MAX({incremental_col}) FROM {table_full_name}")
                ).scalar()
                if last_value:
                    last_value = pd.to_datetime(last_value).date()

        if last_value:
            df = df[df[incremental_col] > last_value]
            logger.info(f"Filtrado incremental aplicado: {len(df)} registros nuevos")

    if df.empty:
        logger.info("No hay registros nuevos para cargar")
        return

    # -------------------
    # 2️⃣ SQL Upsert
    # -------------------
    columns = list(df.columns)
    upsert_sql = build_upsert_statement(
        table_full_name=table_full_name,
        columns=columns,
        primary_key=primary_key
    )

    logger.info(f"Iniciando carga de {len(df)} registros a {table_full_name}")

    # -------------------
    # 3️⃣ Chunks + Concurrencia
    # -------------------
    chunk_size = int(os.getenv("CHUNKSIZE", 10000))
    max_workers = int(os.getenv("MAX_WORKERS", 1))

    num_chunks = math.ceil(len(df) / chunk_size)
    logger.info(
        f"Carga en {num_chunks} chunks | chunk_size={chunk_size} | max_workers={max_workers}"
    )

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
            future.result()  # Propaga errores si hay

    logger.info("Carga finalizada correctamente")
