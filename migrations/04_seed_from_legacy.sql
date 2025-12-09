/*
    Seed the new bilingual schema directly from dbo.Kosten_Kodi_spreadsheet.
    - GO-free, idempotent.
    - Builds EN/NL codes from legacy NL/EN columns.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @LegacyTable sysname = 'dbo.Kosten_Kodi_spreadsheet';

IF OBJECT_ID(@LegacyTable) IS NULL
BEGIN
    RAISERROR('Legacy table %s not found.', 16, 1, @LegacyTable);
    RETURN;
END;

/* Helpers to trim/normalize */
WITH Legacy AS (
    SELECT
        AC_CD,
        AC_ENG_CD,
        AC_Subcategorie_CD,
        AC_Subcategorie_ENG_CD,
        AC_Subcategorie_Opdracht_CD,
        AC_Subcategorie_Opdracht_ENG_CD,
        Jaartal_Prijs,
        Euro_Prijs_Werken_Unit,
        Euro_Prijs_Werken_Min,
        Euro_Prijs_Werken_Max,
        Euro_Prijs_Materiaal_Unit,
        Euro_Prijs_Materiaal_Min,
        Euro_Prijs_Materiaal_Max
    FROM dbo.Kosten_Kodi_spreadsheet
),
Norm AS (
    SELECT DISTINCT
        ISNULL(NULLIF(LTRIM(RTRIM(AC_CD)), ''), 'UNKNOWN') AS cat_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_ENG_CD)), ''), 'UNKNOWN') AS cat_en,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_CD)), ''), 'Unknown NL') AS cat_name_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_ENG_CD)), ''), 'Unknown EN') AS cat_name_en,

        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_CD)), ''), 'GENERAL') AS type_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_ENG_CD)), ''), 'GENERAL') AS type_en,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_CD)), ''), 'General NL') AS type_name_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_ENG_CD)), ''), 'General EN') AS type_name_en,

        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_Opdracht_CD)), ''), 'NONE') AS act_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_Opdracht_ENG_CD)), ''), 'NONE') AS act_en,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_Opdracht_CD)), ''), 'Activity NL') AS act_name_nl,
        ISNULL(NULLIF(LTRIM(RTRIM(AC_Subcategorie_Opdracht_ENG_CD)), ''), 'Activity EN') AS act_name_en,

        Jaartal_Prijs,
        Euro_Prijs_Werken_Unit,
        Euro_Prijs_Werken_Min,
        Euro_Prijs_Werken_Max,
        Euro_Prijs_Materiaal_Unit,
        Euro_Prijs_Materiaal_Min,
        Euro_Prijs_Materiaal_Max
    FROM Legacy
)

/* Units */
INSERT INTO Damage.Unit (symbol, description, measurement_kind)
SELECT DISTINCT
    u.symbol,
    CONCAT('Imported from legacy: ', u.symbol),
    'custom'
FROM (
    SELECT Euro_Prijs_Werken_Unit AS symbol FROM Norm
    UNION
    SELECT Euro_Prijs_Materiaal_Unit FROM Norm
) u
WHERE u.symbol IS NOT NULL AND u.symbol <> ''
  AND NOT EXISTS (SELECT 1 FROM Damage.Unit du WHERE du.symbol = u.symbol);

/* Price books */
INSERT INTO Damage.PriceBookVersion (year_label, valid_from, source_reference)
SELECT DISTINCT
    n.Jaartal_Prijs,
    DATEFROMPARTS(n.Jaartal_Prijs, 1, 1),
    'Imported from Kosten_Kodi_spreadsheet'
FROM Norm n
WHERE n.Jaartal_Prijs IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM Damage.PriceBookVersion pb WHERE pb.year_label = n.Jaartal_Prijs);

/* Categories */
INSERT INTO Damage.DamageCategory (code_nl, code_en, name_nl, name_en, description)
SELECT DISTINCT
    n.cat_nl,
    n.cat_en,
    n.cat_name_nl,
    n.cat_name_en,
    'Imported from legacy'
FROM Norm n
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.DamageCategory c
    WHERE c.code_nl = n.cat_nl OR c.code_en = n.cat_en
);

/* Damage types */
INSERT INTO Damage.DamageType (category_id, code_nl, code_en, name_nl, name_en, description)
SELECT DISTINCT
    c.category_id,
    CONCAT(n.cat_nl, '_', n.type_nl),
    CONCAT(n.cat_en, '_', n.type_en),
    n.type_name_nl,
    n.type_name_en,
    'Imported from legacy'
FROM Norm n
JOIN Damage.DamageCategory c
  ON c.code_nl = n.cat_nl OR c.code_en = n.cat_en
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.DamageType dt
    WHERE dt.code_nl = CONCAT(n.cat_nl, '_', n.type_nl)
       OR dt.code_en = CONCAT(n.cat_en, '_', n.type_en)
);

/* Activities */
INSERT INTO Damage.Activity (code_nl, code_en, name_nl, name_en, description)
SELECT DISTINCT
    CONCAT(n.type_nl, '_', n.act_nl),
    CONCAT(n.type_en, '_', n.act_en),
    n.act_name_nl,
    n.act_name_en,
    'Imported from legacy'
FROM Norm n
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.Activity a
    WHERE a.code_nl = CONCAT(n.type_nl, '_', n.act_nl)
       OR a.code_en = CONCAT(n.type_en, '_', n.act_en)
);

/* Default severity band per damage type */
INSERT INTO Damage.SeverityBand (damage_type_id, band_label, unit_id, range_min, range_max)
SELECT
    dt.damage_type_id,
    'default',
    COALESCE(u.unit_id, (SELECT TOP 1 unit_id FROM Damage.Unit ORDER BY unit_id)),
    0,
    999999
FROM Damage.DamageType dt
OUTER APPLY (
    SELECT TOP 1 u.unit_id
    FROM Damage.Unit u
    WHERE u.symbol IN ('m2', 'm³', 'm')
    ORDER BY CASE u.symbol WHEN 'm2' THEN 1 ELSE 99 END
) u
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.SeverityBand sb WHERE sb.damage_type_id = dt.damage_type_id
);

/* Bridge */
INSERT INTO Damage.DamageTypeActivity (damage_type_id, activity_id, is_required, sequence_order, notes)
SELECT DISTINCT
    dt.damage_type_id,
    a.activity_id,
    1,
    NULL,
    'Imported from legacy'
FROM Norm n
JOIN Damage.DamageType dt
  ON dt.code_nl = CONCAT(n.cat_nl, '_', n.type_nl)
  OR dt.code_en = CONCAT(n.cat_en, '_', n.type_en)
JOIN Damage.Activity a
  ON a.code_nl = CONCAT(n.type_nl, '_', n.act_nl)
  OR a.code_en = CONCAT(n.type_en, '_', n.act_en)
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.DamageTypeActivity dta
    WHERE dta.damage_type_id = dt.damage_type_id
      AND dta.activity_id = a.activity_id
);

/* Costs */
INSERT INTO Damage.ActivityCost (
    activity_id, price_book_id, severity_band_id,
    labor_unit_id, material_unit_id,
    labor_unit_cost, labor_cost_min, labor_cost_max,
    material_unit_cost, material_cost_min, material_cost_max,
    notes
)
SELECT
    a.activity_id,
    pb.price_book_id,
    sb.severity_band_id,
    ul.unit_id,
    um.unit_id,
    NULL,
    n.Euro_Prijs_Werken_Min,
    n.Euro_Prijs_Werken_Max,
    NULL,
    n.Euro_Prijs_Materiaal_Min,
    n.Euro_Prijs_Materiaal_Max,
    'Imported from legacy'
FROM Norm n
JOIN Damage.DamageType dt
  ON dt.code_nl = CONCAT(n.cat_nl, '_', n.type_nl)
  OR dt.code_en = CONCAT(n.cat_en, '_', n.type_en)
JOIN Damage.Activity a
  ON a.code_nl = CONCAT(n.type_nl, '_', n.act_nl)
  OR a.code_en = CONCAT(n.type_en, '_', n.act_en)
JOIN Damage.PriceBookVersion pb ON pb.year_label = n.Jaartal_Prijs
JOIN Damage.SeverityBand sb ON sb.damage_type_id = dt.damage_type_id AND sb.band_label = 'default'
LEFT JOIN Damage.Unit ul ON ul.symbol = n.Euro_Prijs_Werken_Unit
LEFT JOIN Damage.Unit um ON um.symbol = n.Euro_Prijs_Materiaal_Unit
WHERE NOT EXISTS (
    SELECT 1 FROM Damage.ActivityCost ac
    WHERE ac.activity_id = a.activity_id
      AND ac.price_book_id = pb.price_book_id
      AND ac.severity_band_id = sb.severity_band_id
);

PRINT '04_seed_from_legacy completed.';

