from cryptography.fernet import Fernet
import pandas as pd

class ColumnEncryptor:
    """
    Clase para cifrar columnas de un DataFrame usando Fernet.
    """
    def __init__(self, key: str):
        """
        key: clave Fernet en base64 url-safe
        """
        self.fernet = Fernet(key.encode() if isinstance(key, str) else key)

    def encrypt_value(self, value: str) -> str:
        """
        Cifra un valor individual.
        """
        if pd.isnull(value):
            return None
        return self.fernet.encrypt(str(value).encode()).decode()

    def encrypt_series(self, series: pd.Series) -> pd.Series:
        """
        Cifra una columna completa de un DataFrame.
        """
        return series.apply(self.encrypt_value)
