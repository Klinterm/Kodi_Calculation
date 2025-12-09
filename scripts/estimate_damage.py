"""
Simple helper to invoke Damage.EstimateDamageCost and print the breakdown.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from typing import Sequence

import pyodbc

DEFAULT_SERVER = r"BELKLXX15503\Karlosserver"
DEFAULT_DATABASE = "Kodi"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate damage cost via stored procedure.")
    parser.add_argument("--server", default=os.environ.get("DAMAGE_SQL_SERVER", DEFAULT_SERVER))
    parser.add_argument("--database", default=os.environ.get("DAMAGE_SQL_DATABASE", DEFAULT_DATABASE))
    parser.add_argument("--username", help="SQL login username (omit for integrated security).")
    parser.add_argument("--password", help="SQL login password.")
    parser.add_argument("damage_type_code", help="Code of the damage type (e.g., UNKNOWN_GENERAL).")
    parser.add_argument("size_value", type=float, help="Measured size value (numeric).")
    parser.add_argument("unit_symbol", help="Unit symbol (e.g., m2).")
    parser.add_argument("price_year", type=int, help="Price book year.")
    return parser.parse_args(argv)


def build_connection_string(args: argparse.Namespace) -> str:
    parts = [
        "DRIVER={ODBC Driver 17 for SQL Server}",
        f"SERVER={args.server}",
        f"DATABASE={args.database}",
        "TrustServerCertificate=yes",
    ]
    if args.username:
        password = args.password or getpass.getpass("SQL Password: ")
        parts.extend([f"UID={args.username}", f"PWD={password}"])
    else:
        parts.append("Trusted_Connection=yes")
    return ";".join(parts)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    conn = pyodbc.connect(build_connection_string(args))
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            EXEC Damage.EstimateDamageCost
                @DamageTypeCode=?,
                @SizeValue=?,
                @UnitSymbol=?,
                @PriceYear=?,
                @Verbose=0
            """,
            args.damage_type_code,
            args.size_value,
            args.unit_symbol,
            args.price_year,
        )
        columns = [desc[0] for desc in cursor.description]
        print(" | ".join(columns))
        for row in cursor.fetchall():
            print(" | ".join("" if value is None else str(value) for value in row))
    finally:
        cursor.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

