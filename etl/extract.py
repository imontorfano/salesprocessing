import pandas as pd


def extract_csv(csv_path: str, logger):
    logger.info(
        "Starting CSV extraction",
        extra={"path": csv_path}
    )

    try:
        df = pd.read_csv(csv_path)

        logger.info(
            "CSV extracted successfully",
            extra={
                "rows": len(df),
                "columns": list(df.columns)
            }
        )

        return df

    except Exception as exc:
        logger.exception(
            "Failed to extract CSV",
            extra={
                "path": csv_path,
                "error": str(exc)
            }
        )
        raise
