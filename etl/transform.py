import logging
import pandas as pd
import hashlib
from security.encryption import ColumnEncryptor


def transform_data(df: pd.DataFrame, config: dict = None, logger: logging.Logger = None) -> pd.DataFrame:
    """
    Transformaciones genéricas para ETL basadas en config YAML:
    - Renombrado de columnas según column_mapping
    - Conversión de tipos según target.schema
    - Hash determinístico con SHA256 + salt para joins
    - Cifrado de columnas sensibles (Fernet)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("[TRANSFORM] Iniciando transformación")

    if not config:
        logger.warning("[TRANSFORM] No se pasó config, retornando dataframe original")
        return df

    # -------------------------------
    # 1️⃣ Renombrar columnas según column_mapping
    # -------------------------------
    mapping = config.get("column_mapping", {})
    valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
    if valid_mapping:
        df = df.rename(columns=valid_mapping)
        logger.info(f"[TRANSFORM] Columnas renombradas: {valid_mapping}")

    # -------------------------------
    # 2️⃣ Convertir tipos según target.schema
    # -------------------------------
    if "target" in config and "schema" in config["target"]:
        schema = config["target"]["schema"]
        for col, col_type in schema.items():
            if col in df.columns:
                try:
                    col_type_upper = col_type.upper()
                    if col_type_upper in ("DATE", "DATETIME"):
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                    elif col_type_upper in ("INT", "BIGINT"):
                        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")
                    elif col_type_upper == "FLOAT":
                        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="float")
                    else:
                        df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"[TRANSFORM] No se pudo convertir columna {col} a {col_type}: {e}")

    # -------------------------------
    # 3️⃣ Hash determinístico con SHA256 + salt
    # -------------------------------
    security_cfg = config.get("security", {})
    hash_columns = security_cfg.get("hash_columns", [])
    hash_salt = security_cfg.get("hash_salt", "")

    for col in hash_columns:
        if col in df.columns:
            df[f"{col}_hash"] = df[col].apply(
                lambda x: hashlib.sha256((hash_salt + str(x)).encode()).hexdigest()
            )
            logger.info(f"[TRANSFORM] Columna hash generada: {col}_hash")

    # -------------------------------
    # 4️⃣ Cifrado Fernet
    # -------------------------------
    encrypt_columns = security_cfg.get("encrypt_columns", [])
    encryption_key = security_cfg.get("encryption_key")

    if encrypt_columns and encryption_key:
        encryptor = ColumnEncryptor(encryption_key)
        for col in encrypt_columns:
            if col in df.columns:
                df[col] = encryptor.encrypt_series(df[col])
                logger.info(f"[TRANSFORM] Cifrada columna: {col}")

    # -------------------------------
    # 5️⃣ Verificación final
    # -------------------------------
    #logger.info(f"Columnas finales del DataFrame: {list(df.columns)}")
    logger.info("[TRANSFORM] Transformación completada")

    return df