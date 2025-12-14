## Damage Cost Normalization Toolkit

Everything you need to migrate `Kodi.dbo.Kosten_Kodi_spreadsheet` into a
normalized schema, seed it, and test the estimator so future JS tooling can
consume the data cleanly.

---

### Prerequisites

- Windows workstation with Python 3.11+ (already installed here) and `pyodbc`.
- Access to SQL Server `BELKLXX15503\Karlosserver` and the `Kodi` database
  (Windows Integrated Security works; SQL login optional).
- ODBC Driver 17 or 18 for SQL Server.

Environment overrides (optional):

```
setx DAMAGE_SQL_SERVER "server\instance"
setx DAMAGE_SQL_DATABASE "YourDb"
setx KODI_DB_USER "domain\user"
setx KODI_DB_PASSWORD "secret"
```

---

### Repository contents

| File / folder | Purpose |
| --- | --- |
| `main.py` | Prompt-based explorer to list tables or preview rows (handy sanity check). |
| `describe_table.py` | Dumps column metadata for the legacy spreadsheet table. |
| `migrations/01_schema_up.sql` | Idempotent schema setup with bilingual codes/names (no GO). |
| `migrations/02_staging_and_load.sql` | Creates staging table, CSV bulk loader, and bilingual upsert proc. |
| `migrations/03_view_and_proc.sql` | Creates full join view and language-aware estimator proc (no GO). |
| `scripts/manage_damage_schema.py` | Runs the SQL scripts in order (or individually) with either Windows or SQL auth. |
| `scripts/estimate_damage.py` | Calls the stored procedure for quick verification runs. |

All Python files compile (`python -m py_compile …`) and rely only on stdlib
plus `pyodbc`.

---

### Step-by-step workflow

1. **Create / update schema (idempotent, GO-free)**

   ```powershell
   python scripts/manage_damage_schema.py --all
   ```

   Flags:
   - `--username` / `--password` for SQL authentication (password prompts if omitted).
   - `--trust-cert` to bypass certificate warnings.
   - `--scripts <file.sql ...>` to run a custom subset (e.g., `migrations/02_staging_and_load.sql` to rerun the upsert tools).

2. **Validate seed results**

   - `python main.py` → choose option 2 for Windows auth, list `Damage.*` tables,
     preview rows to confirm data landed correctly.
   - `python describe_table.py` if you need to re-check the legacy column layout.

3. **Estimate a scenario**

   ```powershell
   python scripts/estimate_damage.py <DamageTypeCode> <Size> <Unit> <Year>
   # Example:
   python scripts/estimate_damage.py UNKNOWN_GENERAL 12 m2 2017
   ```

   - `DamageTypeCode` follows `<AC_CD>_<AC_Subcategorie_CD>` as produced by the
     migration script.
   - Unit must exist in `Damage.Unit` (migration inserts legacy units).
   - Year must exist in `Damage.PriceBookVersion` (auto-created per distinct `Jaartal_Prijs`).

4. **Use the stored procedure directly (optional)**

   ```sql
   EXEC Damage.EstimateDamageCost
       @DamageTypeCode = 'UNKNOWN_GENERAL',
       @SizeValue = 12,
       @UnitSymbol = 'm2',
       @PriceYear = 2017,
       @Verbose = 1;
   ```

   `@Verbose = 1` also returns the severity band the proc selected.

---

### Schema overview

- `Damage.DamageCategory` / `Damage.DamageType`: bilingual labels + codes; types
  can carry keywords (future NLP hook via `Damage.DamageTypeKeyword`).
- `Damage.Unit`: canonical measurement symbols plus basic conversion factors
  (size values are normalized to base units before band selection).
- `Damage.SeverityBand`: size/intensity ranges per damage type (default band
  created for every type during migration; add more as data matures).
- `Damage.Activity`: atomic work/material tasks. `Damage.DamageTypeActivity`
  bridges them with ordering + required flags.
- `Damage.PriceBookVersion`: captures yearly price lists with validity windows.
- `Damage.ActivityCost`: stores min/max or unit-based labor/material amounts
  per activity + price book + optional severity band.

---

### Working with the database (day-to-day)

- Tables and relationships (see `migrations/01_schema_up.sql`)
  - `DamageCategory` → `DamageType` (1:N) hold bilingual codes/names.
  - `DamageType` → `SeverityBand` (1:N) defines size ranges per type + unit.
  - `DamageType` ↔ `Activity` via `DamageTypeActivity`; ordering/required flags
    live on the bridge.
  - `PriceBookVersion` is the yearly price context; `ActivityCost` links
    an activity + price book + optional severity band to labor/material costs
    and units.
  - `DamageTypeKeyword` is optional metadata for future text matching.

- Insert or refresh data from a CSV (preferred) using `migrations/02_staging_and_load.sql`
  1) Bulk drop rows into `dbo.StgDamageCost` via `Damage.LoadCsvToStaging @FilePath`.
  2) Run `Damage.UpsertFromStaging @Language='nl'` (or `'en'` for code matching);
     it MERGEs staging rows into all normalized tables, creating categories,
     types, units, price books, severity bands, activities, bridges, and costs
     as needed.
  3) Clear the staging table if desired (`TRUNCATE TABLE dbo.StgDamageCost`).

- Insert data manually (when not using staging)
  - Make sure the price year exists in `Damage.PriceBookVersion`.
  - Ensure units exist in `Damage.Unit` before referencing them.
  - Create/lookup the `DamageType`, then create at least one `SeverityBand`
    for it (range + unit).
  - Insert activities in `Damage.Activity`, connect them via
    `DamageTypeActivity`, and finally add costs in `Damage.ActivityCost`
    keyed by activity + price book (+ severity band).

- How the Python helpers tie in (file references)
  - `scripts/manage_damage_schema.py`: runs all SQL migrations in order
    (01–06) or a subset; use `--all` for full setup/refresh.
  - `scripts/estimate_damage.py`: calls `Damage.EstimateDamageCost`
    (defined in `migrations/03_view_and_proc.sql`) to verify prices for a
    given damage code, size, unit, and year.
  - `main.py`: quick table lister/previewer to sanity-check loaded data.
  - `describe_table.py`: inspects the legacy `dbo.Kosten_Kodi_spreadsheet`
    layout if you need to re-map incoming columns.

---

### Migration details (current path)

- `01_schema_up`: builds the Damage schema with bilingual codes/names and a computed severity key for indexing.
- `02_staging_and_load`: defines `dbo.StgDamageCost`, a CSV loader proc (`Damage.LoadCsvToStaging`), and `Damage.UpsertFromStaging` to push staging rows into all normalized tables (idempotent; language-aware).
- `03_view_and_proc`: creates `Damage.vActivityCostFull` (full join view) and `Damage.EstimateDamageCost` (language-aware code matching, severity pick, cost breakdown).

---

### Expanding the dataset

- Add new keywords for NLP matching:

  ```sql
  INSERT INTO Damage.DamageTypeKeyword (damage_type_id, language_code, keyword_text)
  VALUES (@DamageTypeId, 'en', 'burst pipe');
  ```

- Insert extra severity bands per damage type (ensure non-overlapping ranges).
- If spreadsheets start providing price-per-unit data, populate
  `labor_unit_cost` / `material_unit_cost` instead of min/max columns.
- For large imports, stage rows into temp tables, validate units + price years,
  then merge into the production tables to maintain quality.

---

### Porting to JavaScript later

The future JS service only needs to:

1. Ask the user for damage type + size.
2. Resolve the damage type (use `DamageType` + `DamageTypeKeyword`).
3. Call `Damage.EstimateDamageCost`.
4. Render the returned activity rows.

Every stored value is accessible via straightforward SQL, so translating the
Python helpers into JS/Node or a web API will be direct once the schema is live.

---

### Quick reference commands

```powershell
# List GO-split SQL scripts and execute them sequentially
python scripts/manage_damage_schema.py --all

# Preview Damage tables after seeding
python main.py

# Run the estimator for a sample scenario
python scripts/estimate_damage.py UNKNOWN_GENERAL 12 m2 2017

# Re-check legacy table schema
python describe_table.py
```

When returning to the project, start by re-running `manage_damage_schema.py`
if anything changed in the SQL scripts, then use `estimate_damage.py` to sanity
check a known damage type before diving deeper.

