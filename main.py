"""
Minimal script to exercise basic input/output against the local SQL Server
database. The code intentionally stays procedural so it can be ported to
JavaScript with minimal changes later on.
"""

from __future__ import annotations

import getpass
import os
import sys
import pyodbc


SERVER = r"BELKLXX15503\Karlosserver"
DATABASE = "Kodi"
USERNAME = os.environ.get("KODI_DB_USER", r"SUPPLYCHAIN.COM\Klintermans")
PASSWORD = os.environ.get("KODI_DB_PASSWORD", "MisterCoronaCuckMagic123")
DEFAULT_SCHEMA = "dbo"
ROW_LIMIT = 5


def choose_driver() -> str:
    preferred = (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    )
    installed = set(pyodbc.drivers())
    for driver in preferred:
        if driver in installed:
            return driver
    raise RuntimeError(
        "No SQL Server ODBC driver found. Install ODBC Driver 18 for SQL Server."
    )


def build_connection_string(driver: str) -> str:
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={SERVER}",
        f"DATABASE={DATABASE}",
    ]
    if USERNAME and PASSWORD:
        parts.append(f"UID={USERNAME}")
        parts.append(f"PWD={PASSWORD}")
    else:
        parts.append("Trusted_Connection=yes")
    parts.append("TrustServerCertificate=yes")
    return ";".join(parts)


def create_connection() -> pyodbc.Connection:
    driver = choose_driver()
    conn_str = build_connection_string(driver)
    return pyodbc.connect(conn_str, autocommit=False, timeout=5)


def prompt(message: str) -> str:
    try:
        return input(message)
    except KeyboardInterrupt:
        print("\nAborted by user.")
        sys.exit(1)


def list_tables(cursor: pyodbc.Cursor, limit: int = 20) -> list[tuple[str, str]]:
    cursor.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
    )
    return cursor.fetchmany(limit)


def sanitize_identifier(name: str, fallback: str) -> str:
    cleaned = name.strip("[] ").strip()
    return cleaned or fallback


def quote_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def preview_table(cursor: pyodbc.Cursor, schema: str, table: str, limit: int) -> None:
    sql = (
        f"SELECT TOP ({limit}) * FROM "
        f"{quote_identifier(schema)}.{quote_identifier(table)}"
    )
    cursor.execute(sql)
    rows = cursor.fetchall()
    if not rows:
        print("No rows returned.")
        return

    columns = [column[0] for column in cursor.description]
    print(" | ".join(columns))
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))


def get_credentials_from_user(
    default_user: str | None, default_password: str | None
) -> tuple[str | None, str | None]:
    """Prompt for username/password; blank username keeps defaults."""
    print("Enter custom SQL credentials (leave username blank to cancel).")
    user = prompt("Username: ").strip()
    if not user:
        return default_user, default_password
    password = getpass.getpass("Password: ")
    return user, password


def main() -> None:
    print(f"Connecting to SQL Server at {SERVER} / database {DATABASE}")

    global USERNAME, PASSWORD
    print(
        "Authentication options:\n"
        "  1) Stored credentials (default)\n"
        "  2) Windows Integrated Security (use current login)\n"
        "  3) Enter credentials now"
    )
    auth_choice = prompt("Select option [1/2/3]: ").strip() or "1"
    if auth_choice == "2":
        USERNAME = PASSWORD = None
        print("Using Windows Integrated Security.")
    elif auth_choice == "3":
        USERNAME, PASSWORD = get_credentials_from_user(USERNAME, PASSWORD)
        if not USERNAME:
            print("No username entered; falling back to stored credentials.")

    try:
        with create_connection() as conn:
            cursor = conn.cursor()
            table_choice = prompt(
                "Enter table name to preview (schema.table or blank to list tables): "
            ).strip()

            if not table_choice:
                entries = list_tables(cursor)
                if not entries:
                    print("No tables found.")
                    return
                print("First tables found in Kodi:")
                for schema, table in entries:
                    print(f"- {schema}.{table}")
                return

            if "." in table_choice:
                schema_name, table_name = table_choice.split(".", 1)
            else:
                schema_name, table_name = DEFAULT_SCHEMA, table_choice

            schema_name = sanitize_identifier(schema_name, DEFAULT_SCHEMA)
            table_name = sanitize_identifier(table_name, table_choice)

            limit_value = prompt(
                f"How many rows to preview? [default {ROW_LIMIT}]: "
            ).strip()
            limit = ROW_LIMIT if not limit_value else max(1, int(limit_value))

            preview_table(cursor, schema_name, table_name, limit)
    except pyodbc.Error as exc:
        print("Database error:", exc)
        sys.exit(1)
    except RuntimeError as exc:
        print("Configuration error:", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

