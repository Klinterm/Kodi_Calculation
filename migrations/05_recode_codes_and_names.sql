/*
    Recode DamageType and Activity to use human-friendly names and tighter codes.
    - Codes become short (type/activity names), with category suffix only when needed to keep uniqueness.
    - Names become descriptive ("Category - Type", "Type - Activity").
    GO-free; safe to rerun (idempotent-ish; re-applies the same rules).
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

/* Recode DamageType */
;WITH dt_base AS (
    SELECT
        dt.damage_type_id,
        dt.name_nl     AS base_name_nl,
        dt.name_en     AS base_name_en,
        dc.code_nl     AS cat_code_nl,
        dc.code_en     AS cat_code_en,
        dc.name_nl     AS cat_name_nl,
        dc.name_en     AS cat_name_en
    FROM Damage.DamageType dt
    JOIN Damage.DamageCategory dc ON dc.category_id = dt.category_id
),
dup_nl AS (
    SELECT base_name_nl, COUNT(*) AS cnt
    FROM dt_base
    GROUP BY base_name_nl
),
dup_en AS (
    SELECT base_name_en, COUNT(*) AS cnt
    FROM dt_base
    GROUP BY base_name_en
)
UPDATE dt
SET
    code_nl = CASE WHEN dnl.cnt > 1 THEN CONCAT(db.base_name_nl, '_', db.cat_code_nl) ELSE db.base_name_nl END,
    code_en = CASE WHEN den.cnt > 1 THEN CONCAT(db.base_name_en, '_', db.cat_code_en) ELSE db.base_name_en END,
    name_nl = CONCAT(db.cat_name_nl, ' - ', db.base_name_nl),
    name_en = CONCAT(db.cat_name_en, ' - ', db.base_name_en)
FROM Damage.DamageType dt
JOIN dt_base db ON db.damage_type_id = dt.damage_type_id
LEFT JOIN dup_nl dnl ON dnl.base_name_nl = db.base_name_nl
LEFT JOIN dup_en den ON den.base_name_en = db.base_name_en;

/* Recode Activity */
;WITH act_base AS (
    SELECT
        a.activity_id,
        a.name_nl       AS base_name_nl,
        a.name_en       AS base_name_en,
        dt.name_nl      AS type_name_nl,
        dt.name_en      AS type_name_en,
        dt.code_nl      AS type_code_nl,
        dt.code_en      AS type_code_en
    FROM Damage.Activity a
    LEFT JOIN Damage.DamageTypeActivity dta ON dta.activity_id = a.activity_id
    LEFT JOIN Damage.DamageType dt ON dt.damage_type_id = dta.damage_type_id
),
dup_act_nl AS (
    SELECT base_name_nl, COUNT(*) AS cnt
    FROM act_base
    GROUP BY base_name_nl
),
dup_act_en AS (
    SELECT base_name_en, COUNT(*) AS cnt
    FROM act_base
    GROUP BY base_name_en
)
UPDATE a
SET
    code_nl = CASE WHEN danl.cnt > 1 THEN CONCAT(ab.base_name_nl, '_', ISNULL(ab.type_code_nl, 'GEN')) ELSE ab.base_name_nl END,
    code_en = CASE WHEN daen.cnt > 1 THEN CONCAT(ab.base_name_en, '_', ISNULL(ab.type_code_en, 'GEN')) ELSE ab.base_name_en END,
    name_nl = CASE WHEN ab.type_name_nl IS NOT NULL THEN CONCAT(ab.type_name_nl, ' - ', ab.base_name_nl) ELSE ab.base_name_nl END,
    name_en = CASE WHEN ab.type_name_en IS NOT NULL THEN CONCAT(ab.type_name_en, ' - ', ab.base_name_en) ELSE ab.base_name_en END
FROM Damage.Activity a
JOIN act_base ab ON ab.activity_id = a.activity_id
LEFT JOIN dup_act_nl danl ON danl.base_name_nl = ab.base_name_nl
LEFT JOIN dup_act_en daen ON daen.base_name_en = ab.base_name_en;

PRINT '05_recode_codes_and_names completed.';

