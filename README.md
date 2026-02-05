# Sales ETL Project

This project implements an ETL process to process sales data from CSV files into a PostgreSQL database. 
It includes transformations, data quality checks, incremental loading, chunk-based processing for large volumes, and security with encryption and hashing of sensitive data.

The current implementation is designed as a **Silver-layer ETL**, prioritizing correctness, traceability, and idempotent loads.  
To meet the exercise requirement of **concurrent processing**, bulk ingestion strategies like PostgreSQL `COPY` and partition-aware loading are intentionally deferred. These are described in the **Next steps** section, as they require clearer source guarantees (file frequency, ordering) and database capabilities. Same for  deployment strategies (Docker, scheduling, secrets management) are intentionally deferred and described in the Next steps section.


---

## 📁 Folder structure
```
root/
├─ run_etl.py
├─ .env
├─ config.yaml
├─ requirements.txt
├─ Data/
│  └─ sales.csv
├─ security/
│  ├─ encryption.py
│  └─ decryption.py
├─ etl/
│  ├─ __init__.py
│  ├─ extract.py
│  ├─ transform.py
│  ├─ load.py
│  ├─ orchestrator.py
│  └─ utils/
│     ├─ __init__.py
│     ├─ data_quality.py
│     ├─ logger.py
│     └─ schema_manager.py

```

---

## 📝 Descripción de los archivos principales

### 1. `extract.py`
- Extracts data from CSV files.
- Supports chunk size configured in `.env`.
- Returns a pandas `DataFrame` ready for validation and transformation.

### 2. `transform.py`
- Applies generic transformations according to `config.yaml`.
- Features:
- Column renaming according to `column_mapping`.
- Type conversion according to `target.schema`.
- **Fernet** encryption of sensitive columns (`customer_id`).
- Deterministic hash with SHA256 (`customer_id_hash`) for joins.
- Ready to add `salt` if required.

### 3. `load.py`
- Load the transformed data into PostgreSQL using upsert.
- Supports incremental loading (based on the `last_loaded_timestamp` table in `etl_control`) and chunk loading for large datasets. 

### 4. `utils/schema_manager.py`
- Manages the creation or reconciliation of tables based on `target.schema`.
- Allows adding columns if necessary, preventing errors due to schema changes.
- Also creates the ETL control table, which stores the last timestamp value loaded into the database.

### 5. `utils/data_quality.py`
- Validates the **data contract** of the source CSV (`required_source_columns`).
- Applies data quality rules (`data_quality`) such as values ​​greater than 0 or not null.
- Throws `DataQualityError` if the data does not comply.

### 6. `utils/logger.py`
- Configure a standard logger for the entire ETL process.
- Generate logs with timestamps and context (`run_id`).

### 7. `security/encryption.py`
- The `ColumnEncryptor` class allows you to encrypt columns using Fernet.
- Methods:
- `encrypt_series`: Encrypts a pandas `Series`.
- `encrypt_value`: Encrypts an individual value. (not used but possible)

### 8. `security/decryption.py`
- Allows you to decrypt one or more `customer_id` values ​​using their `transaction_id`.
- Reads a database connection from `.env`.
- Returns a DataFrame with the decrypted values.
To use the decryption tool, run: `python decryption.py --key "encryptionkey" --transaction_ids 1,2`

---


## ⚙️ Configuración config.yaml

The `config.yaml` file is the core contract of the ETL process. It defines how data is transformed, validated, and loaded, and allows you to control:

- Target table and schema
- Required columns and quality rules (`required_source_columns` and `data_quality`)
- Transformations and security (`column_mapping`, `security`)
- Incremental loading of source fields (`incremental_field`)
Each section is detailed below:

1️⃣ Target (destination table)
```yaml
target:
  table: sales_transactions
  schema:
    transaction_id: BIGINT
    customer_id: TEXT
    customer_id_hash: TEXT
    product_id: BIGINT
    quantity: INT
    sale_date: DATE
  primary_key:
    - transaction_id
```
- `table`: Name of the destination table in Postgres.
- `schema:`: Columns and data types that will be used in the database.
- `primary_key`: Primary key for performing upserts.
- `incremental_field`: Column used for incremental loading from the source, for example, timestamp.

Behavior with schema_manager:

- Table does not exist → it is automatically created with the defined schema.
- A new column is added to the target → the table is automatically modified to include it.
- A column is deleted from the target → it is not automatically deleted (to protect historical data).

2️⃣ Source contract (required_source_columns)

```yaml
required_source_columns:
  - transaction_id
  - customer_id
  - product_id
  - quantity
  - timestamp
```
- Define the contract with the source.
- The ETL process will validate that these columns are present:
  - If columns are missing, a data_quality error is generated.
  - If there are extra columns, they are removed from the DataFrame before the insert, and a warning is generated in the log.

Schema drift:

- If the contract is added only to the source and not the target: DQ warning is removed.
- If the contract is updated and added to both the target and source, `schema_manager` automatically creates them in the table and inserts the data.
- If columns are missing in the source compared to the contract, DQ blocks the load.


3️⃣ Transformations
```yaml
column_mapping:
  timestamp: sale_date
```
- Maps column names from the source to the expected names in the target.
- Ensures consistency between different versions of the CSV.
- The transform also maps and casts all columns according to the target source defined in the contract.

4️⃣ Data quality rules
```yaml
data_quality:
  quantity:
    gt: 0
  transaction_id:
    not_null: true

unique_constraints:
  - columns: [transaction_id]       #if activated can validate duplicates
```
- Allows defining rules per column.
- Allows defining unique constraints if you want to validate primary keys or duplicates.
- Supported rules:
  - `gt, gte, lt, lte` → Numerical comparison (Greater than, Greater than equal, etc.)
  - `not_null` → Checks for null values.

Violations are logged and generate a DataQualityError.

5️⃣ Security / Encryption and Hash

```yaml
security:
  encrypt_columns:
    - customer_id
  hash_columns:
    - customer_id
```

- `encrypt_columns`: Columns encrypted with Fernet to protect sensitive information.
- `hash_columns`: Columns transformed into deterministic hashes using SHA256 with an optional salt. This allows for secure joins without exposing the original values. The hashed column is created separately.

5️⃣ Execution control table

```yaml
control_table:
  name: etl_control
  schema:
    table_name: VARCHAR(255) PRIMARY KEY
    last_loaded_timestamp: TIMESTAMP
    last_run_id: VARCHAR(36)
    last_run_type: VARCHAR(20)
    updated_at: TIMESTAMP
```
This is used to save the last load timestamp in the destination table (watermark), allowing for incremental loading based on that field. It is updated after the load is complete in load.py.
INFO: Currently, it loads and updates the last execution (only one record). However, it can be configured as a table for full execution control by removing the primary key and searching for the last updated_at.

Behavior with schema_manager:
- If the table does not exist, it is automatically created using the defined schema.


---
## 🔐 Encryptation and hash
- `customer_id` is encrypted with Fernet, ensuring confidentiality during transit and storage. Can be decrypted but for same customer_id the value can be different. Not for joins but for decryption. The Fernet encryption key (`ENCRYPTION_KEY`) is defined in the .env file.

- `customer_id_hash` allows for deterministic joins without revealing the actual value. It is automatically created in the destination schema if added to hash_columns. The hash is always the same for the same customer_id.
Optionally, a salt can be added to make the hash more secure (in .env file).

## 📊 Carga incremental y performance

The use of PostgreSQL COPY was intentionally avoided in this implementation in order to:
- Maintain fine-grained data quality validations
- Support idempotent upserts with primary key control
- Enable controlled concurrency at chunk level, as required by the exercise

* `incremental_field` in config.yaml specifies the source column for incremental loading.

* The `last_loaded_timestamp` field in the `sales.etl_control` table specifies the source column for incremental loading. It only loads new records from that date onward.

* For large datasets, configurable chunk sizes are used (CHUNKSIZE in .env), preventing memory or database overload.

* Loading is implemented using upsert to maintain consistency and avoid duplicates. (Configurable in DQ: duplicate error by primary key)

* If reprocessing is desired, a `reprocess_from` parameter can be used in execution (see below).

## 🔄 ETL Flow (Conceptual summary)
```
schema_manager.py ──> If not exist, create/alter tables (contract based)
      │
      ▼
CSV (Data/sales_YYYYMMDD.csv)
      │
      ▼
extract.py  ──> data_quality.py (validaciones)
      │
      ▼
transform.py ──> 
    - column_mapping
    - tipos según target.schema
    - customer_id (Fernet)
    - customer_id_hash (SHA256)
      │
      ▼
load.py ──> Incremental Insertion in PostgreSQL
            - Configurable chunk size
            - Load information (duplicate pk loading can be blocked using data_quality.py)
            - Incremental for new records

```

## 🚀 How to execute

Requisitos:

- Postgres must be installed and a database with a schema already created. (In the development environment, the database named "sales" and the schema "sales" were used - configurable in the .env file)

- Clone the repository:
```
  git clone https://github.com/imontorfano/salesprocessing.git
  cd salesprocessing
```

- Create and activate the virtual environment:
```
  python -m venv venv
  .\venv\Scripts\Activate.ps1
```

- Install the necessary libraries:
```
    pip install -r requirements.txt
```

- Config the `.env`

```
  copy .env.example .env
```
Note: The Fernet key must be 32 bytes url-safe in base64.


- Basic execution:
```
  python run_etl.py
```
- Execution with reprocessing from a specific date:
```
  python run_etl.py --reprocess_from 2023-01-01
```
--reprocess_from overwrites the last incremental value of the incremental_field configured in the YAML.

It supports dates in YYYY-MM-DD format.



## Next steps and trade-offs

The current implementation prioritizes correctness, data quality, and controlled concurrency, which is appropriate for a **Silver layer**.  
Depending on real production constraints (data volume, file frequency, database engine, and query patterns) and/or database layer, the following improvements could be applied. 
Also insted of dynamic schema manager, tables and evolution could be stored in a .sql insted of a .py automation to reduce rechecking.

### 1️⃣ Partitioned tables

If the database engine supports table partitioning, the target table could be partitioned by day using a business date column (e.g. `sale_date`).

Benefits:
- Faster analytical queries (partition pruning)
- Easier data lifecycle management (drop or archive partitions)
- Reduced index size per partition

Possible strategy:
- Create the target table as a partitioned table by `sale_date`
- Pre-create daily partitions (or create them on demand)
- Route inserts automatically to the correct partition

In a more advanced setup, an IKM-like approach could be applied:
- Load data into a staging table
- Disable or drop indexes on target partitions
- Insert into the correct partitions
- Rebuild indexes after the load

This partitioned strategy would be applied without changing the core ETL logic: only the target table and insert routing would be adjusted.


---

### 2️⃣ COPY-based ingestion for very large volumes

For much larger datasets (tens or hundreds of millions of rows), the load strategy could shift to a bulk ingestion pattern using `COPY FROM`.

Typical approach:
- COPY data from CSV into a staging table (no constraints, no indexes)
- Perform validation and transformations upstream (or in SQL)
- MERGE or INSERT INTO target SELECT FROM staging with `ON CONFLICT`
- TRUNCATE the staging table

Advantages:
- Maximum raw ingestion speed
- Minimal Python-side overhead
- Reduced database round-trips

Trade-offs:
- Less granular control over row-level errors
- Harder to integrate per-row data quality checks
- Conflict handling and encryption must be carefully orchestrated

For these reasons, COPY was not used in the current implementation, as the exercise required concurrency and strong control over data correctness.

This approach could be revisited in a high-volume OLAP scenario, once strict ordering and partitioning strategies are in place.


---

### 3️⃣ Concurrency strategy tuning

Concurrency is currently implemented using chunked upserts with configurable thread pools.

```
CHUNKSIZE=10000
MAX_WORKERS=3
DB_POOL_SIZE=6
DB_MAX_OVERFLOW=0
```

These values balance parallel throughput and database connection limits. They can be tuned according to dataset size, lock behavior, and available I/O/CPU.

Depending on the database engine and workload:
- Medium volumes: moderate concurrency (current approach)
- Very large volumes: fewer threads + larger chunks (to reduce lock contention)
- COPY-based loads: no concurrency at COPY level, but parallelism at the file or partition level


---

### 4️⃣ Source-driven incremental loading (multi-file support)

Currently, the ETL processes a single CSV file defined in the environment configuration. If files are daily and stable.

Possible improvements:
- Support loading multiple daily files (e.g. `sales_YYYYMMDD.csv`)
- Apply glob patterns and sorted file processing
- Maintain strict ordering to ensure correct incremental loads
- Parallelize file processing when partitions are independent

This would better align the pipeline with real-world ingestion patterns where data arrives daily or hourly.

---

### 5️⃣ Execution control and observability

The execution control table (`etl_control`) currently stores only the last successful run.

Possible enhancements:
- Store all executions (remove PK on `table_name`)
- Track row counts, rejected rows, and processing times
- Add basic profiling metrics
- Enable replay or backfill strategies per execution

---

### 6️⃣ Deployment and production hardening

The ETL process is designed to run in a controlled environment with reproducible results. A simple production setup could include:
- Docker container: Package Python, dependencies, and ETL scripts in a container.
  - Ensures consistency across dev, test, and production environments.
  - Environment variables can be provided at runtime for database credentials, encryption keys, and file paths.
- Job scheduling:
  - Simple option: run the Docker container using a cron job for daily or hourly execution.
  - Advanced option: use Airflow or Prefect to orchestrate tasks, monitor failures, and implement retries.
- Secrets management:
  - Store sensitive information (Fernet key, database credentials) in a secure vault or as Kubernetes Secrets in production.
  - Avoid hardcoding secrets in the repository.
- CI/CD (optional but recommended):
  - Use GitHub Actions to run tests, build Docker images, and deploy to the production environment automatically.
- Observability:
  - Logs should be captured from the container output.
  - Optional: integrate with a monitoring service for alerts and metrics on ETL runs (rows processed, duration, errors).

Summary: The ETL can run as a self-contained Docker job, scheduled via cron or orchestrated with Airflow, using secure credentials and monitored logs. This keeps the process simple, reproducible, and maintainable while allowing future scaling or cloud deployment.

---

These improvements were intentionally left out of the initial implementation to maintain clarity, portability, and focus on the core ETL logic.


