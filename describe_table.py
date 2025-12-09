"""
Quick helper to inspect column metadata for Kodi tables.
"""

from __future__ import annotations

import pyodbc

SERVER = r"BELKLXX15503\Karlosserver"
DATABASE = "Kodi"
TABLE = "Kosten_Kodi_spreadsheet"
SCHEMA = "dbo"

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()

cursor.execute(
    """
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """,
    SCHEMA,
    TABLE,
)

for column in cursor.fetchall():
    print(column)

cursor.close()
conn.close()

