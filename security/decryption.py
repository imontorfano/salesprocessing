import os
import sys
import argparse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from cryptography.fernet import Fernet

# ----------------------------
# Cargar variables de entorno
# ----------------------------
load_dotenv()

DB_URL = os.getenv("DB_URL")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

if not DB_URL:
    raise ValueError("DB_URL no definido en .env")

# ----------------------------
# Función de descifrado
# ----------------------------
def decrypt_customer_ids(encryption_key: str, transaction_ids: list, db_url: str = DB_URL, db_schema: str = DB_SCHEMA) -> pd.DataFrame:
    """
    Descifra los customer_id de los registros especificados.

    Args:
        encryption_key (str): Fernet key
        transaction_ids (list): lista de transaction_id
        db_url (str): URL SQLAlchemy
        db_schema (str): schema de la tabla

    Returns:
        pd.DataFrame: DataFrame con columns ['transaction_id', 'customer_id']
    """
    fernet = Fernet(encryption_key.encode() if isinstance(encryption_key, str) else encryption_key)
    engine = create_engine(db_url)
    table_full_name = f"{db_schema}.sales_transactions"

    placeholders = ", ".join([":tid" + str(i) for i in range(len(transaction_ids))])
    sql = f"SELECT transaction_id, customer_id FROM {table_full_name} WHERE transaction_id IN ({placeholders})"

    params = {f"tid{i}": tid for i, tid in enumerate(transaction_ids)}

    with engine.connect() as conn:
        result = conn.execute(text(sql), params).fetchall()

    if not result:
        raise ValueError(f"No se encontraron registros para transaction_id={transaction_ids}")

    # Construir DataFrame y descifrar
    df = pd.DataFrame(result, columns=["transaction_id", "customer_id"])
    df["customer_id"] = df["customer_id"].apply(lambda x: fernet.decrypt(x.encode()).decode())
    return df

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descifra uno o varios customer_id de sales_transactions")
    parser.add_argument("--key", required=True, help="Fernet encryption key")
    parser.add_argument("--transaction_ids", required=True, help="Transaction IDs separados por comas, ej: 1,2,3")
    args = parser.parse_args()

    try:
        tids = [int(t.strip()) for t in args.transaction_ids.split(",")]
        df = decrypt_customer_ids(encryption_key=args.key, transaction_ids=tids)
        print(df)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)



# Para desencriptar usar: python decryption.py --key "3mF6G6wGkL8q0eQk5s7Wv8Yb9oIhQxK1lzT4d7Fm3pA=" --transaction_ids 1,2
