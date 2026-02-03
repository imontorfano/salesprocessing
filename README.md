# Sales ETL Project

Este proyecto implementa un **ETL para procesar datos de ventas** desde archivos CSV hacia una base de datos PostgreSQL. Incluye **transformaciones, data quality checks, carga incremental, chunks para grandes volúmenes y seguridad con encriptación y hash de datos sensibles**.

---

## 📁 Estructura de carpetas
```
root/
├─ run_etl.py
├─ .env
├─ config.yaml
├─ requirements.txt
├─ Data/
│  └─ sales_YYYYMMDD.csv
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
- Extrae datos desde archivos CSV.
- Soporta `chunksize` configurado desde `.env`.
- Devuelve un `DataFrame` de pandas listo para validación y transformación.

### 2. `transform.py`
- Aplica transformaciones genéricas según `config.yaml`.
- Funcionalidades:
  - Renombrado de columnas según `column_mapping`.
  - Conversión de tipos según `target.schema`.
  - Encriptación **Fernet** de columnas sensibles (`customer_id`).
  - Hash determinístico con SHA256 (`customer_id_hash`) para joins.
  - Preparado para agregar `salt` si se requiere.

### 3. `load.py`
- Carga los datos transformados en PostgreSQL usando **upsert**.
- Soporta carga incremental (`incremental_field` en `config.yaml`) y chunks para datasets grandes.

### 4. `utils/schema_manager.py`
- Administra la creación o reconciliación de tablas según `target.schema`.
- Permite agregar columnas si es necesario, evitando errores por cambios de esquema.

### 5. `utils/data_quality.py`
- Valida **contrato de datos** del CSV de origen (`required_source_columns`).
- Aplica reglas de calidad (`data_quality`) como valores mayores a 0 o no nulos.
- Lanza `DataQualityError` si los datos no cumplen.

### 6. `utils/logger.py`
- Configura un logger estándar para todo el ETL.
- Genera logs con timestamps y contexto (`run_id`).

### 7. `security/encryption.py`
- Clase `ColumnEncryptor` que permite cifrar columnas con Fernet.
- Métodos:
  - `encrypt_series`: encripta un `Series` de pandas.
  - `decrypt_series`: descifra un `Series`.

### 8. `security/decryption.py`
- Permite descifrar uno o varios `customer_id` usando su `transaction_id`.
- Lee conexión a DB desde `.env`.
- Devuelve un DataFrame con los valores descifrados.

---


## ⚙️ Configuración config.yaml

El `config.yaml` es el contrato central del ETL. Define cómo se transforman, validan y cargan los datos, y permite controlar:

- Tabla de destino y esquema (target)
- Columnas obligatorias y reglas de calidad (`required_source_columns` y `data_quality`)
- Transformaciones y seguridad (`column_mapping`, `security`)
- Carga incremental (`incremental_field`)
A continuación se detalla cada sección:

1️⃣ Target (tabla destino)
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
  incremental_field: sale_date
```
- `table`: Nombre de la tabla destino en Postgres.
- `schema:`: Columnas y tipos de datos que tendrán en la base de datos.
- `primary_key`: Clave primaria para realizar upserts.
- `incremental_field`: Columna usada para carga incremental, por ejemplo, fechas de venta.

Comportamiento con schema_manager:

- Tabla no existe → se crea automáticamente con el `schema` definido.
- Se agrega una columna nueva al target → se altera la tabla automáticamente para incluirla.
- Se elimina una columna del target → no se elimina automáticamente (proteger datos históricos).

2️⃣ Contrato de origen (required_source_columns)

```yaml
required_source_columns:
  - transaction_id
  - customer_id
  - product_id
  - quantity
  - timestamp
```
- Define el contrato con la fuente.
- El ETL validará que estas columnas estén presentes:
  - Si faltan columnas, se genera un error de data_quality.
  - Si hay columnas extra, se eliminan del DataFrame antes del insert, y se genera un warning en el log.

Schema drift:

- Si se agrega en el contrato solo al source y no al target: Se quita aviso de DQ
- Si se actualiza el contrato y se agrega al target y source, `schema_manager` las crea automaticamente en la tabla e inserta datos.
- Si faltan columnas en origen con respecto al contrato, DQ bloquea la carga.


3️⃣ Transformaciones
```yaml
column_mapping:
  timestamp: sale_date
```
- Mapea nombres de columnas de la fuente a los nombres esperados en el target.
- Permite mantener consistencia entre diferentes versiones del CSV.
- A su vez el transform mapea y castea todas las columnas segun el target source definido en el contrato.

4️⃣ Reglas de calidad de datos
```yaml
data_quality:
  quantity:
    gt: 0
  transaction_id:
    not_null: true
```
- Permite definir reglas por columna.
- Reglas soportadas:
  - `gt, gte, lt, lte` → Comparación numérica (Greater than, Greater than equal, etc)
  - `not_null` → Verifica que no haya valores nulos.

Violaciones se registran en logs y generan un DataQualityError.

5️⃣ Seguridad / Encriptación y Hash

```yaml
security:
  encrypt_columns:
    - customer_id
  hash_columns:
    - customer_id
```

- `encrypt_columns`: Columnas que se cifran con Fernet para proteger información sensible.

- `hash_columns`: Columnas que se transforman en hash determinístico con SHA256 + salt opcional.
Esto permite hacer joins seguros sin exponer los valores originales.


---
## 🔐 Encriptación y hash
- `customer_id` se cifra con Fernet, asegurando confidencialidad en tránsito y almacenamiento.

- `customer_id_hash` permite realizar joins determinísticos sin revelar el valor real. Se crea automaticamente en squema destino si se agrega en hash_columns.

- La clave de cifrado (`ENCRYPTION_KEY`) se define en el .env.
Opcionalmente, se puede añadir salt para hacer el hash más seguro.

- Las claves se almacenan en .env, no en YAML ni código fuente.

## 📊 Carga incremental y performance

* `incremental_field` en config.yaml indica la columna que define la última fecha procesada.
* Carga solo registros nuevos desde esa fecha.

* Para datasets grandes, se usan chunks de tamaño configurable (CHUNKSIZE en .env), evitando saturar memoria o la base de datos.

* Carga implementada con upsert para mantener consistencia y evitar duplicados.

* Si se quiere reprocesar se puede utilizar un parametro en la ejecución `reprocess_from` (ver mas adelante)


## 🔄 Flujo ETL (Resumen conceptual)
```
CSV (Data/sales_YYYYMMDD.csv)
      │
      ▼
extract.py ──> DataFrame ──> data_quality.py (validaciones)
      │
      ▼
transform.py ──> 
    - column_mapping
    - tipos según target.schema
    - customer_id (Fernet)
    - customer_id_hash (SHA256)
      │
      ▼
schema_manager.py ──> Crea/actualiza tabla en PostgreSQL
      │
      ▼
load.py ──> Inserción incremental en PostgreSQL
    - chunksize configurable
    - upsert por primary key
    - incremental_field para nuevos registros

```

## 🚀 Cómo ejecutar

Requisitos:

- Tener instalado Postgres y una creada una BBDD con un squema. (En dev se ha usado DB con nombre sales y esquema sales - configurable en .env)


- Clonar el repositorio
```
git clone https://github.com/imontorfano/salesprocessing.git
cd salesprocessing
```

- Crear y activar el entorno virtual
```
python -m venv venv
.\venv\Scripts\Activate.ps1
```

- Instalar las librerías necesarias:
```
    pip install -r requirements.txt
```

- Configuración `.env`

```
copy .env.example .env
```
Nota: La clave Fernet debe ser 32 bytes url-safe en base64.


- Ejecución básica:
```
python run_etl.py
```
- Ejecución con reproceso desde una fecha específica:
```
python run_etl.py --reprocess_from 2023-01-01
```
--reprocess_from sobrescribe el último valor incremental del incremental_field configurado en el YAML.

Soporta fechas en formato YYYY-MM-DD



## Next steps / Suposiciones

- Presupongo que el archivo CSV es un archivo enviado diariamente por lo que valido contra sales_date (fecha) sino tendria que crear una columna (u otra tabla) para guardar el watermark de fecha_datos con timestamp.
- Según que capa del EDW es: Particionar tablas para insertar datos segun particiones (fecha_datos), indexar columnas que luego sean necesarias hacer queries sobre ellas, agregar columna de fecha_carga para controlar procesamientos.
- Se podrían procesar los archivos o los chunks en paralelo usando concurrent.futures o multiprocessing.
- Hoy en dia permite la carga de un archivo especifico definido en el .env. Se podría aplicar glob + sorted + iteración secuencial para cargar todos los archivos sales_*.csv de manera secuencial, en orden e incremental y paralelo con el punto anterior.
- Crear una tabla de control de ejecuciones o guardar los logs de ejecucion en algun sistema para controlar las ejecuciones. 
- Ejecutar el orchestrator con alguna herramienta de Workload managment. 
- Agregar mas reglas de calidad del dato.
- Agregar un codigo salt al hash para mas seguridad.
- Mover al Cloud: 
  - Revisar encriptacion y utilizar DDM propia del cloud. 
  - Revisar devops segun entornos, secrets en cloud (ej Azure Key Vault o Githubs secrets)

