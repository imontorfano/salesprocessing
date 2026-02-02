import logging
import pandas as pd

class DataQualityError(Exception):
    pass


def validate_source_schema(df: pd.DataFrame, required_columns, logger=None, allow_extra_columns=True):
    """
    Valida que el CSV tenga las columnas definidas en el contrato.
    
    Args:
        df (pd.DataFrame): DataFrame a validar
        required_columns (list): columnas obligatorias
        logger (logging.Logger, optional)
        allow_extra_columns (bool): Si True, permite columnas extra y solo avisa
    Returns:
        pd.DataFrame: DataFrame filtrado a las columnas obligatorias + opcionales permitidas
    """

    if logger is None:
        logger = logging.getLogger(__name__)

    incoming_columns = set(df.columns)
    expected_columns = set(required_columns)

    missing_columns = expected_columns - incoming_columns
    extra_columns = incoming_columns - expected_columns

    logger.info(
        "Validando contrato de esquema de origen",
        extra={
            "expected_columns": sorted(expected_columns),
            "incoming_columns": sorted(incoming_columns)
        }
    )

    if missing_columns:
        logger.error(
            "Faltan columnas obligatorias en el CSV",
            extra={"missing_columns": sorted(missing_columns)}
        )
        raise DataQualityError(
            f"Columnas obligatorias faltantes: {missing_columns}"
        )

    if extra_columns:
        if allow_extra_columns:
            logger.warning(
                "Se detectaron columnas extra en el CSV y serán ignoradas",
                extra={"extra_columns": sorted(extra_columns)}
            )
            # Filtrar DataFrame para dejar solo las columnas esperadas
            df = df[[c for c in df.columns if c in expected_columns]]
        else:
            logger.error(
                "Schema drift detectado",
                extra={"extra_columns": sorted(extra_columns)}
            )
            raise DataQualityError(
                f"Columnas extra detectadas: {extra_columns}"
            )

    return df


def validate_data_rules(df: pd.DataFrame, dq_rules, logger=None):
    """
    Valida reglas de calidad definidas en el contrato YAML.
    Ej:
      quantity:
        gt: 0
    """

    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(
        "Validando reglas de calidad de datos",
        extra={"rules": dq_rules}
    )

    violations = []

    for column, rules in dq_rules.items():
        if column not in df.columns:
            continue  # ya fue validado en schema

        series = df[column]

        for rule, value in rules.items():
            if rule == "gt":
                invalid = df[series <= value]
            elif rule == "gte":
                invalid = df[series < value]
            elif rule == "lt":
                invalid = df[series >= value]
            elif rule == "lte":
                invalid = df[series > value]
            elif rule == "not_null":
                invalid = df[series.isnull()]
            else:
                raise ValueError(f"Regla de DQ no soportada: {rule}")

            if not invalid.empty:
                violations.append({
                    "column": column,
                    "rule": rule,
                    "value": value,
                    "invalid_rows": len(invalid)
                })

    if violations:
        logger.error(
            "Violaciones de reglas de calidad detectadas",
            extra={"violations": violations}
        )
        raise DataQualityError(
            f"Violaciones de data quality: {violations}"
        )

    logger.info("Reglas de calidad validadas correctamente")
    return df
