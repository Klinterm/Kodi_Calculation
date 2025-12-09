/*
    Seed Damage.DamageTypeKeyword using existing codes/names.
    - Adds NL/EN keywords from code_nl, code_en, name_nl, name_en.
    - Idempotent: skips if the same keyword already exists for the type/lang.
    GO-free.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

;WITH kw AS (
    SELECT
        dt.damage_type_id,
        'nl' AS language_code,
        dt.code_nl AS keyword_text
    FROM Damage.DamageType dt
    UNION
    SELECT dt.damage_type_id, 'en', dt.code_en FROM Damage.DamageType dt
    UNION
    SELECT dt.damage_type_id, 'nl', dt.name_nl FROM Damage.DamageType dt
    UNION
    SELECT dt.damage_type_id, 'en', dt.name_en FROM Damage.DamageType dt
)
INSERT INTO Damage.DamageTypeKeyword (damage_type_id, language_code, keyword_text)
SELECT k.damage_type_id, k.language_code, k.keyword_text
FROM kw k
WHERE k.keyword_text IS NOT NULL AND k.keyword_text <> ''
  AND NOT EXISTS (
        SELECT 1 FROM Damage.DamageTypeKeyword dtk
        WHERE dtk.damage_type_id = k.damage_type_id
          AND dtk.language_code = k.language_code
          AND dtk.keyword_text = k.keyword_text
  );

PRINT '06_seed_keywords completed.';

