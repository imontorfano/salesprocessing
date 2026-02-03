import uuid
from datetime import datetime
import logging

from etl.extract import extract_csv
from etl.transform import transform_data
from etl.load import load_data
from etl.utils.data_quality import (
    validate_source_schema,
    validate_data_rules,
    validate_unique_constraints,
    DataQualityError
)
from etl.utils.schema_manager import reconcile_table_schema, reconcile_control_table


class SalesETLOrchestrator:
    def __init__(self, config, engine, logger=None):
        self.config = config
        self.engine = engine
        self.schema = config["db"]["schema"]
        self.target_table = config["target"]["table"]
        self.table_contract = config["target"]
        self.logger = logger or logging.getLogger(__name__)

        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.utcnow()

    def run(self):
        self.logger.info(f"ETL run started: {self.run_id}")

        try:
            # ----------------------
            # 4️⃣ Schema Manager - tabla destino
            # ----------------------
            reconcile_table_schema(
                engine=self.engine,
                table_name=self.target_table,
                schema=self.schema,
                table_contract=self.table_contract,
                logger=self.logger
            )

            # ----------------------
            # 5️⃣ Schema Manager - tabla de control
            # ----------------------
            if "control_table" in self.config:
                reconcile_control_table(
                    engine=self.engine,
                    schema=self.schema,
                    control_table_config=self.config["control_table"],
                    logger=self.logger
                )

            # ----------------------
            # 1️⃣ Extract con incremental
            # ----------------------
            incremental_col_csv = self.table_contract.get("incremental_field")  # 'timestamp' en CSV
            df, max_incremental = extract_csv(
                path=self.config["source"]["path"],
                logger=self.logger,
                engine=self.engine,
                table_name=self.target_table,
                incremental_col=incremental_col_csv,
                reprocess_from=self.table_contract.get("reprocess_from")
            )

            # ----------------------
            # 2️⃣ Data Quality
            # ----------------------
            df = validate_source_schema(
                df,
                self.config["required_source_columns"],
                self.logger,
                allow_extra_columns=True
            )

            if "unique_constraints" in self.config:
                validate_unique_constraints(df, self.config["unique_constraints"], self.logger)

            validate_data_rules(
                df,
                self.config.get("data_quality", {}),
                self.logger
            )

            # ----------------------
            # 3️⃣ Transform
            # ----------------------
            df = transform_data(df, self.config, self.logger)
            # Aquí timestamp → sale_date según column_mapping

            # ----------------------
            # 6️⃣ Load
            # ----------------------
            load_data(
                engine=self.engine,
                df=df,
                schema=self.schema,
                table_name=self.target_table,
                table_contract=self.table_contract,
                logger=self.logger,
                run_id=self.run_id,
                max_incremental=max_incremental
            )

            self.logger.info(f"ETL run finished successfully: {self.run_id}")

        except DataQualityError as dq_err:
            self.logger.error(f"ETL failed due to data quality error: {dq_err}")
            raise

        except Exception as exc:
            self.logger.exception(f"ETL run failed: {exc}")
            raise

        finally:
            self.logger.info(
                f"ETL run finalized: {self.run_id} | "
                f"started_at={self.started_at.isoformat()} | finished_at={datetime.utcnow().isoformat()}"
            )
