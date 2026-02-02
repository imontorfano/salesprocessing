from sqlalchemy import text
import logging
import math
import os
import pandas as pd

def build_upsert_statement(table_full_name, columns, primary_key):
    """
    Genera un SQL de upsert dinámico según columnas y primary key.
    """
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

def load_data(engine, df, table_name, schema, table_contract, logger=None):
    """
    Carga un DataFrame en la tabla destino usando upsert según el contrato.
    Soporta:
        - Incremental load según incremental_field
        - Reprocesos desde fecha definida en reprocess_from
        - Batching y chunks automáticos
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if df.empty:
        logger.info("No hay registros para cargar")
        return

    table_full_name = f"{schema}.{table_name}"
    primary_key = table_contract.get("primary_key", [])
    incremental_col = table_contract.get("incremental_field")
    reprocess_from = table_contract.get("reprocess_from")  # fecha opcional para reprocesos

    # -------------------
    # 1️⃣ Filtrado incremental o reproceso
    # -------------------
    if incremental_col and incremental_col in df.columns:
        with engine.connect() as conn:
            if reprocess_from:
                # Convertir a datetime.date para poder comparar con df[col] que ya es date
                last_value = pd.to_datetime(reprocess_from).date()
                logger.info(f"Reprocesando desde {last_value} según reprocess_from")
            else:
                last_value = conn.execute(
                    text(f"SELECT MAX({incremental_col}) FROM {table_full_name}")
                ).scalar()
                # Convertir a date si no es None
                if last_value is not None:
                    last_value = pd.to_datetime(last_value).date()

        if last_value is not None:
            df = df[df[incremental_col] > last_value]
            logger.info(f"Filtrado incremental aplicado: {len(df)} registros nuevos después de {last_value}")

    if df.empty:
        logger.info("No hay registros nuevos después del filtro incremental/reproceso")
        return

    # -------------------
    # 2️⃣ Generar SQL upsert
    # -------------------
    columns = list(df.columns)
    upsert_sql = build_upsert_statement(
        table_full_name=table_full_name,
        columns=columns,
        primary_key=primary_key
    )

    logger.info(f"Iniciando carga: {len(df)} registros a {table_full_name}", extra={"primary_key": primary_key})

    # -------------------
    # 3️⃣ Ejecutar en batch por chunks
    # -------------------
    chunk_size = int(os.getenv("CHUNKSIZE", 10000))  # parámetro configurable desde .env
    num_chunks = math.ceil(len(df) / chunk_size)

    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk_df = df.iloc[start:end]
        logger.info(f"Cargando chunk {i+1}/{num_chunks} con {len(chunk_df)} registros")
        records = chunk_df.to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(text(upsert_sql), records)

    logger.info("Carga finalizada correctamente")
