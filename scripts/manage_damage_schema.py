"""
Utility CLI to execute the SQL batches that create and populate the
normalized damage-cost schema.

Examples:
    python scripts/manage_damage_schema.py --all
    python scripts/manage_damage_schema.py --scripts sql/01_create_damage_schema.sql
"""

from __future__ import annotations

import argparse
import getpass
import os
import pathlib
import sys
import textwrap
from typing import Iterable, Sequence

import pyodbc

DEFAULT_SERVER = r"BELKLXX15503\Karlosserver"
DEFAULT_DATABASE = "Kodi"
SQL_DIR = pathlib.Path("sql")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute the SQL scripts that define the normalized damage schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Authentication:
              - Integrated (default): use your current Windows login.
              - SQL login: pass --username and you will be prompted for the password.
            """
        ),
    )
    parser.add_argument(
        "--server",
        default=os.environ.get("DAMAGE_SQL_SERVER", DEFAULT_SERVER),
        help="SQL Server instance name",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("DAMAGE_SQL_DATABASE", DEFAULT_DATABASE),
        help="Target database name",
    )
    parser.add_argument(
        "--scripts",
        nargs="+",
        type=pathlib.Path,
        help="List of SQL files to execute (defaults to the curated order).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run the standard scripts (01 create, 02 seed, 03 procedure) in that order.",
    )
    parser.add_argument(
        "--username",
        help="SQL authentication username. If omitted, uses Windows Integrated Security.",
    )
    parser.add_argument(
        "--password",
        help="SQL authentication password. Prompted if username is supplied and password is omitted.",
    )
    parser.add_argument(
        "--trust-cert",
        action="store_true",
        help="Add TrustServerCertificate=yes to the connection string.",
    )
    return parser.parse_args(argv)


def discover_scripts(args: argparse.Namespace) -> list[pathlib.Path]:
    if args.scripts:
        return [pathlib.Path(p).resolve() for p in args.scripts]
    if args.all or not args.scripts:
        ordered = [
            pathlib.Path("migrations/01_schema_up.sql"),
            pathlib.Path("migrations/02_staging_and_load.sql"),
            pathlib.Path("migrations/03_view_and_proc.sql"),
            pathlib.Path("migrations/04_seed_from_legacy.sql"),
            pathlib.Path("migrations/05_recode_codes_and_names.sql"),
            pathlib.Path("migrations/06_seed_keywords.sql"),
        ]
        return [path.resolve() for path in ordered]
    return []


def build_connection_string(args: argparse.Namespace) -> str:
    parts = [
        "DRIVER={ODBC Driver 17 for SQL Server}",
        f"SERVER={args.server}",
        f"DATABASE={args.database}",
    ]
    if args.username:
        password = args.password or getpass.getpass("SQL Password: ")
        parts.append(f"UID={args.username}")
        parts.append(f"PWD={password}")
    else:
        parts.append("Trusted_Connection=yes")
    if args.trust_cert:
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts)


def split_batches(sql_text: str) -> Iterable[str]:
    """Split SQL text into batches separated by GO on its own line."""
    batch: list[str] = []
    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            joined = "\n".join(batch).strip()
            if joined:
                yield joined
            batch = []
        else:
            batch.append(line)
    if batch:
        joined = "\n".join(batch).strip()
        if joined:
            yield joined


def run_script(cursor: pyodbc.Cursor, path: pathlib.Path) -> None:
    print(f"\n== Executing {path} ==")
    sql_text = path.read_text(encoding="utf-8")
    for batch in split_batches(sql_text):
        cursor.execute(batch)
        cursor.commit()
    print(f"Completed {path.name}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    scripts = discover_scripts(args)
    missing = [path for path in scripts if not path.exists()]
    if missing:
        print("Missing scripts:", ", ".join(str(p) for p in missing), file=sys.stderr)
        return 1

    conn_str = build_connection_string(args)
    print(f"Connecting to {args.server}/{args.database}")
    conn = pyodbc.connect(conn_str, autocommit=False)
    cursor = conn.cursor()
    try:
        for script in scripts:
            run_script(cursor, script)
        print("\nAll scripts executed successfully.")
    finally:
        cursor.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

