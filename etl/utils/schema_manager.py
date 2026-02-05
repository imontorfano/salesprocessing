from sqlalchemy import text
import logging

def reconcile_table_schema(engine, table_name, schema, table_contract=None, logger=None):
    """
    Crea o ajusta la tabla según el contrato.

    Args:
        engine: SQLAlchemy engine
        table_name: nombre de la tabla
        schema: esquema en la base de datos
        table_contract: dict con keys:
            - schema: {col_name: type}
            - primary_key: [list of PK columns]
        logger: instancia de logging
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    table_full_name = f"{schema}.{table_name}"

    # extraer columnas y PK del contrato
    columns = table_contract.get("schema", {}) if table_contract else {}
    primary_key = table_contract.get("primary_key", []) if table_contract else []

    # construir SQL para CREATE TABLE
    columns_def = ", ".join([f"{c} {t}" for c, t in columns.items()])
    pk_clause = f", PRIMARY KEY ({', '.join(primary_key)})" if primary_key else ""

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_full_name} (
        {columns_def}
        {pk_clause}
    );
    """

    with engine.begin() as conn:
        # crear tabla si no existe
        conn.execute(text(create_table_sql))
        logger.info(f"[SCHEMA] Tabla {table_full_name} creada o verificada.")

        # obtener columnas existentes
        res = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
        """), {"schema": schema, "table": table_name})

        existing_cols = {r[0] for r in res.fetchall()}

        # agregar columnas nuevas según contrato
        for col, col_type in columns.items():
            if col not in existing_cols:
                conn.execute(text(f"ALTER TABLE {table_full_name} ADD COLUMN {col} {col_type}"))
                logger.info(f"[SCHEMA] Columna {col} agregada a {table_full_name}")

    #logger.info(f"Esquema reconciliado para {table_full_name}")


def reconcile_control_table(engine, schema, control_table_config, logger=None):
    """
    Crea la tabla de control ETL si no existe.

    Args:
        engine: SQLAlchemy engine
        schema: esquema en la DB
        control_table_config: dict con keys:
            - name: nombre de la tabla
            - schema: {col_name: type}
        logger: instancia de logging
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    table_name = control_table_config["name"]
    columns = control_table_config["schema"]
    table_full_name = f"{schema}.{table_name}"

    # separar PK de columnas normales
    pk_columns = [col for col, typ in columns.items() if "PRIMARY KEY" in typ.upper()]
    normal_columns = {col: typ.replace("PRIMARY KEY", "").strip() for col, typ in columns.items()}

    # construir definición de columnas
    columns_def_list = [f"{col} {typ}" for col, typ in normal_columns.items()]
    if pk_columns:
        columns_def_list.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

    columns_def = ", ".join(columns_def_list)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_full_name} (
        {columns_def}
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        logger.info(f"[SCHEMA] Tabla de control {table_full_name} creada o verificada.")
