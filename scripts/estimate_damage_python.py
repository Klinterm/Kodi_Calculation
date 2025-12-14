"""
Estimate damage costs in Python (no T-SQL proc), with optional OpenAI
classification and automatic latest-year selection.

Usage examples:
    # Provide a known code
    python scripts/estimate_damage_python.py --damage-code UNKNOWN_GENERAL --size 12 --unit m2

    # Let OpenAI classify from text (requires OPENAI_API_KEY)
    python scripts/estimate_damage_python.py --description "broken pipe water damage" --size 10 --unit m2

Notes:
- If --price-year is omitted, the script picks the latest year that has costs
  for the chosen damage type.
- OpenAI is optional; if no key/model is configured, you must pass --damage-code.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from typing import Any, Iterable

import pyodbc

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None


DEFAULT_SERVER = r"BELKLXX15503\Karlosserver"
DEFAULT_DATABASE = "Kodi"
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate damage costs in Python.")
    parser.add_argument(
        "--server",
        default=os.environ.get("DAMAGE_SQL_SERVER", DEFAULT_SERVER),
        help="SQL Server instance",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("DAMAGE_SQL_DATABASE", DEFAULT_DATABASE),
        help="Target database",
    )
    parser.add_argument(
        "--username",
        help="SQL login username (omit for Windows Integrated Security).",
    )
    parser.add_argument(
        "--password",
        help="SQL login password (prompted if username is supplied and password omitted).",
    )
    parser.add_argument(
        "--language",
        choices=["nl", "en"],
        default="nl",
        help="Language for code matching and labeling.",
    )
    parser.add_argument(
        "--damage-code",
        help="Damage type code (skips OpenAI classification when provided).",
    )
    parser.add_argument(
        "--description",
        help="Free-text description; used for OpenAI classification if no damage code is given.",
    )
    parser.add_argument(
        "--size",
        type=float,
        required=True,
        help="Measured size value (numeric).",
    )
    parser.add_argument(
        "--unit",
        required=True,
        help="Unit symbol (e.g., m2).",
    )
    parser.add_argument(
        "--price-year",
        type=int,
        help="Price book year. If omitted, the latest available year is used.",
    )
    parser.add_argument(
        "--openai-model",
        default=DEFAULT_MODEL,
        help="OpenAI chat model to use for classification (default: gpt-4o-mini).",
    )
    return parser.parse_args(argv or sys.argv[1:])


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


def fetch_damage_type_code(
    conn: pyodbc.Connection, language: str, damage_code: str | None, description: str | None, model: str
) -> tuple[int, str]:
    """
    Resolve damage type id + canonical code.
    - If damage_code is supplied, match on code_en/code_nl.
    - Otherwise, classify via OpenAI against available codes/names/keywords.
    Returns (damage_type_id, code_used).
    """
    cursor = conn.cursor()
    if damage_code:
        cursor.execute(
            """
            SELECT damage_type_id, CASE WHEN ?='en' THEN code_en ELSE code_nl END AS code_used
            FROM Damage.DamageType
            WHERE code_en = ? OR code_nl = ?
            """,
            language,
            damage_code,
            damage_code,
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Damage type code not found: {damage_code}")
        return row.damage_type_id, row.code_used

    if not description:
        raise ValueError("Either --damage-code or --description must be provided.")

    if not OpenAI:
        raise ImportError("openai package not installed; install it or provide --damage-code.")
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is required for classification when no damage code is provided.")

    cursor.execute(
        """
        SELECT TOP (200)
            dt.damage_type_id,
            dt.code_en,
            dt.code_nl,
            dt.name_en,
            dt.name_nl,
            STRING_AGG(ISNULL(dtk.keyword_text, ''), ', ') WITHIN GROUP (ORDER BY dtk.keyword_text) AS keywords
        FROM Damage.DamageType dt
        LEFT JOIN Damage.DamageTypeKeyword dtk ON dtk.damage_type_id = dt.damage_type_id
        GROUP BY dt.damage_type_id, dt.code_en, dt.code_nl, dt.name_en, dt.name_nl
        ORDER BY dt.damage_type_id
        """
    )
    candidates = cursor.fetchall()

    choices = []
    for c in candidates:
        choices.append(
            {
                "id": c.damage_type_id,
                "code": c.code_en if language == "en" else c.code_nl,
                "name": c.name_en if language == "en" else c.name_nl,
                "keywords": (c.keywords or "").split(", "),
            }
        )

    prompt_lines = ["You are a classifier. Pick the best damage type code and ONLY return the code.", ""]
    prompt_lines.append(f"User description: {description}")
    prompt_lines.append("")
    prompt_lines.append("Candidates:")
    for c in choices:
        prompt_lines.append(
            f"- code: {c['code']} | name: {c['name']} | keywords: {', '.join(k for k in c['keywords'] if k)}"
        )

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Return exactly one damage type code from the list."},
            {"role": "user", "content": "\n".join(prompt_lines)},
        ],
        temperature=0,
        max_tokens=10,
    )
    code_guess = resp.choices[0].message.content.strip().split()[0]

    cursor.execute(
        """
        SELECT damage_type_id, CASE WHEN ?='en' THEN code_en ELSE code_nl END AS code_used
        FROM Damage.DamageType
        WHERE code_en = ? OR code_nl = ?
        """,
        language,
        code_guess,
        code_guess,
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Model returned an unknown code: {code_guess}")
    return row.damage_type_id, row.code_used


def resolve_price_book_id(conn: pyodbc.Connection, damage_type_id: int, language: str, damage_code: str, price_year: int | None) -> tuple[int, int]:
    cursor = conn.cursor()
    if price_year is None:
        cursor.execute(
            """
            SELECT MAX(price_year) AS max_year
            FROM Damage.vActivityCostFull
            WHERE (CASE WHEN ?='en' THEN damage_type_code_en ELSE damage_type_code_nl END) = ?
            """,
            language,
            damage_code,
        )
        row = cursor.fetchone()
        if not row or row.max_year is None:
            raise ValueError("No price book years found for the selected damage type.")
        price_year = int(row.max_year)

    cursor.execute(
        "SELECT price_book_id FROM Damage.PriceBookVersion WHERE year_label = ?",
        price_year,
    )
    pb = cursor.fetchone()
    if not pb:
        raise ValueError(f"Price book for year {price_year} not found.")
    return pb.price_book_id, price_year


def pick_severity_band(conn: pyodbc.Connection, damage_type_id: int, size_value: float, unit_symbol: str) -> tuple[int, dict[str, Any], float]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT unit_id, conversion_to_base, COALESCE(base_symbol, symbol) AS base_symbol FROM Damage.Unit WHERE symbol = ?",
        unit_symbol,
    )
    u = cursor.fetchone()
    if not u:
        raise ValueError(f"Unit not found: {unit_symbol}")
    size_in_base = size_value * u.conversion_to_base

    cursor.execute(
        """
        SELECT sb.severity_band_id, sb.band_label, sb.range_min, sb.range_max,
               u.symbol AS severity_unit, u.conversion_to_base AS conv
        FROM Damage.SeverityBand sb
        JOIN Damage.Unit u ON u.unit_id = sb.unit_id
        WHERE sb.damage_type_id = ?
        """,
        damage_type_id,
    )
    bands = cursor.fetchall()
    if not bands:
        raise ValueError("No severity bands configured for this damage type.")

    best = None
    best_score = None
    for b in bands:
        min_base = b.range_min * b.conv
        max_base = b.range_max * b.conv
        in_range = min_base <= size_in_base <= max_base
        mid = (min_base + max_base) / 2.0
        score = (0 if in_range else 1) + abs(mid - size_in_base)
        if best_score is None or score < best_score:
            best_score = score
            best = b

    assert best is not None
    band_info = {
        "severity_band_id": best.severity_band_id,
        "band_label": best.band_label,
        "range_min": best.range_min,
        "range_max": best.range_max,
        "severity_unit": best.severity_unit,
    }
    return best.severity_band_id, band_info, size_in_base


def fetch_cost_rows(
    conn: pyodbc.Connection,
    damage_type_id: int,
    price_book_id: int,
    severity_band_id: int,
    language: str,
) -> Iterable[pyodbc.Row]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            a.activity_id,
            CASE WHEN ?='en' THEN a.code_en ELSE a.code_nl END AS activity_code,
            CASE WHEN ?='en' THEN a.name_en ELSE a.name_nl END AS activity_name,
            dta.sequence_order,
            dta.is_required,
            ac.labor_unit_cost,
            ac.labor_cost_min,
            ac.labor_cost_max,
            ac.material_unit_cost,
            ac.material_cost_min,
            ac.material_cost_max,
            ul.symbol AS labor_unit,
            um.symbol AS material_unit
        FROM Damage.DamageTypeActivity dta
        JOIN Damage.Activity a ON a.activity_id = dta.activity_id
        JOIN Damage.ActivityCost ac
          ON ac.activity_id = a.activity_id
         AND ac.price_book_id = ?
         AND (ac.severity_band_id IS NULL OR ac.severity_band_id = ?)
        LEFT JOIN Damage.Unit ul ON ul.unit_id = ac.labor_unit_id
        LEFT JOIN Damage.Unit um ON um.unit_id = ac.material_unit_id
        WHERE dta.damage_type_id = ?
        ORDER BY dta.sequence_order, activity_code
        """,
        language,
        language,
        price_book_id,
        severity_band_id,
        damage_type_id,
    )
    return cursor.fetchall()


def estimate_costs(rows: Iterable[pyodbc.Row], size_in_base: float) -> list[dict[str, Any]]:
    results = []
    for r in rows:
        labor_est = (
            r.labor_unit_cost * size_in_base
            if r.labor_unit_cost is not None
            else ((r.labor_cost_min + r.labor_cost_max) / 2.0 if r.labor_cost_min is not None and r.labor_cost_max is not None else r.labor_cost_min)
        )
        material_est = (
            r.material_unit_cost * size_in_base
            if r.material_unit_cost is not None
            else ((r.material_cost_min + r.material_cost_max) / 2.0 if r.material_cost_min is not None and r.material_cost_max is not None else r.material_cost_min)
        )
        results.append(
            {
                "activity_code": r.activity_code,
                "activity_name": r.activity_name,
                "is_required": bool(r.is_required),
                "sequence_order": r.sequence_order,
                "labor_unit": r.labor_unit,
                "material_unit": r.material_unit,
                "labor_cost_min": r.labor_cost_min,
                "labor_cost_max": r.labor_cost_max,
                "material_cost_min": r.material_cost_min,
                "material_cost_max": r.material_cost_max,
                "labor_unit_cost": r.labor_unit_cost,
                "material_unit_cost": r.material_unit_cost,
                "estimated_labor": labor_est,
                "estimated_material": material_est,
            }
        )
    return results


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    conn = pyodbc.connect(build_connection_string(args))
    try:
        damage_type_id, code_used = fetch_damage_type_code(
            conn, args.language, args.damage_code, args.description, args.openai_model
        )
        price_book_id, price_year = resolve_price_book_id(conn, damage_type_id, args.language, code_used, args.price_year)
        severity_band_id, band_info, size_in_base = pick_severity_band(conn, damage_type_id, args.size, args.unit)
        rows = fetch_cost_rows(conn, damage_type_id, price_book_id, severity_band_id, args.language)
        if not rows:
            raise ValueError("No costs found for the selected type/year/severity.")
        estimates = estimate_costs(rows, size_in_base)

        total_labor = sum(e["estimated_labor"] or 0 for e in estimates)
        total_material = sum(e["estimated_material"] or 0 for e in estimates)

        print(f"Damage type: {code_used}")
        print(f"Language: {args.language}")
        print(f"Price year: {price_year}")
        print(f"Severity band: {band_info['band_label']} ({band_info['range_min']}–{band_info['range_max']} {band_info['severity_unit']})")
        print(f"Input size: {args.size} {args.unit} (base-adjusted: {size_in_base})")
        print("")
        print("Activities:")
        for e in estimates:
            print(
                f"- {e['activity_code']} | {e['activity_name']} | labor={e['estimated_labor']} | material={e['estimated_material']} "
                f"(unit costs: labor {e['labor_unit_cost']} {e['labor_unit'] or ''}, material {e['material_unit_cost']} {e['material_unit'] or ''})"
            )
        print("")
        print(f"Totals: labor={total_labor} material={total_material} grand_total={total_labor + total_material}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


