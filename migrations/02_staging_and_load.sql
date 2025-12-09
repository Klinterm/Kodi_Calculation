/*
    Staging + load helpers for bilingual codes/names.
    - Creates staging table for CSV.
    - Bulk-load proc.
    - Upsert proc to push staging into Damage tables.
    GO-free; safe to rerun.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

/* Staging table */
IF OBJECT_ID('dbo.StgDamageCost') IS NULL
BEGIN
    CREATE TABLE dbo.StgDamageCost (
        category_code_nl        nvarchar(100) NULL,
        category_code_en        nvarchar(100) NULL,
        category_name_nl        nvarchar(150) NULL,
        category_name_en        nvarchar(150) NULL,
        damage_type_code_nl     nvarchar(150) NULL,
        damage_type_code_en     nvarchar(150) NULL,
        damage_type_name_nl     nvarchar(200) NULL,
        damage_type_name_en     nvarchar(200) NULL,
        activity_code_nl        nvarchar(255) NULL,
        activity_code_en        nvarchar(255) NULL,
        activity_name_nl        nvarchar(255) NULL,
        activity_name_en        nvarchar(255) NULL,
        price_year              smallint NULL,
        severity_band_label     nvarchar(50) NULL,
        severity_min            decimal(18,4) NULL,
        severity_max            decimal(18,4) NULL,
        labor_unit              nvarchar(20) NULL,
        labor_min               decimal(18,2) NULL,
        labor_max               decimal(18,2) NULL,
        labor_unit_cost         decimal(18,2) NULL,
        material_unit           nvarchar(20) NULL,
        material_min            decimal(18,2) NULL,
        material_max            decimal(18,2) NULL,
        material_unit_cost      decimal(18,2) NULL
    );
END;

/* Bulk load from CSV into staging (path must be accessible to SQL Server) */
IF OBJECT_ID('Damage.LoadCsvToStaging') IS NOT NULL
    DROP PROCEDURE Damage.LoadCsvToStaging;
GO
CREATE PROCEDURE Damage.LoadCsvToStaging
    @FilePath nvarchar(4000),
    @FirstRow int = 2 -- skip header
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sql nvarchar(max) = N'
        BULK INSERT dbo.StgDamageCost
        FROM ''' + REPLACE(@FilePath, '''', '''''') + '''
        WITH (
            FIRSTROW = ' + CAST(@FirstRow AS nvarchar(20)) + ',
            FIELDTERMINATOR='','',
            ROWTERMINATOR=''\n'',
            TABLOCK,
            CODEPAGE = ''65001'',
            KEEPNULLS
        );';
    EXEC (@sql);
END;
GO

/* Upsert from staging into normalized tables */
IF OBJECT_ID('Damage.UpsertFromStaging') IS NOT NULL
    DROP PROCEDURE Damage.UpsertFromStaging;
GO
CREATE PROCEDURE Damage.UpsertFromStaging
    @Language char(2) = 'nl'  -- 'nl' or 'en' determines which code to match on fallback
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Lang char(2) = LOWER(@Language);

    /* Categories */
    INSERT INTO Damage.DamageCategory (code_nl, code_en, name_nl, name_en, description)
    SELECT DISTINCT
        COALESCE(NULLIF(category_code_nl, ''), NULLIF(category_name_nl, ''), 'UNKNOWN'),
        COALESCE(NULLIF(category_code_en, ''), NULLIF(category_name_en, ''), 'UNKNOWN'),
        COALESCE(NULLIF(category_name_nl, ''), NULLIF(category_code_nl, ''), 'Unknown'),
        COALESCE(NULLIF(category_name_en, ''), NULLIF(category_code_en, ''), 'Unknown'),
        'Imported via staging'
    FROM dbo.StgDamageCost s
    WHERE NOT EXISTS (
        SELECT 1 FROM Damage.DamageCategory c
        WHERE c.code_nl = COALESCE(NULLIF(s.category_code_nl, ''), NULLIF(s.category_name_nl, ''), 'UNKNOWN')
           OR c.code_en = COALESCE(NULLIF(s.category_code_en, ''), NULLIF(s.category_name_en, ''), 'UNKNOWN')
    );

    /* Damage types */
    INSERT INTO Damage.DamageType (category_id, code_nl, code_en, name_nl, name_en, description)
    SELECT DISTINCT
        c.category_id,
        COALESCE(NULLIF(s.damage_type_code_nl, ''), NULLIF(s.damage_type_name_nl, ''), 'UNKNOWN'),
        COALESCE(NULLIF(s.damage_type_code_en, ''), NULLIF(s.damage_type_name_en, ''), 'UNKNOWN'),
        COALESCE(NULLIF(s.damage_type_name_nl, ''), NULLIF(s.damage_type_code_nl, ''), 'Unknown'),
        COALESCE(NULLIF(s.damage_type_name_en, ''), NULLIF(s.damage_type_code_en, ''), 'Unknown'),
        'Imported via staging'
    FROM dbo.StgDamageCost s
    JOIN Damage.DamageCategory c
      ON c.code_nl = COALESCE(NULLIF(s.category_code_nl, ''), NULLIF(s.category_name_nl, ''), 'UNKNOWN')
      OR c.code_en = COALESCE(NULLIF(s.category_code_en, ''), NULLIF(s.category_name_en, ''), 'UNKNOWN')
    WHERE NOT EXISTS (
        SELECT 1 FROM Damage.DamageType dt
        WHERE dt.code_nl = COALESCE(NULLIF(s.damage_type_code_nl, ''), NULLIF(s.damage_type_name_nl, ''), 'UNKNOWN')
           OR dt.code_en = COALESCE(NULLIF(s.damage_type_code_en, ''), NULLIF(s.damage_type_name_en, ''), 'UNKNOWN')
    );

    /* Units */
    INSERT INTO Damage.Unit (symbol, description, measurement_kind)
    SELECT DISTINCT
        u.symbol,
        CONCAT('Imported unit ', u.symbol),
        'custom'
    FROM (
        SELECT labor_unit AS symbol FROM dbo.StgDamageCost
        UNION
        SELECT material_unit FROM dbo.StgDamageCost
    ) u
    WHERE u.symbol IS NOT NULL AND u.symbol <> ''
      AND NOT EXISTS (SELECT 1 FROM Damage.Unit du WHERE du.symbol = u.symbol);

    /* Price books */
    INSERT INTO Damage.PriceBookVersion (year_label, valid_from, source_reference)
    SELECT DISTINCT
        s.price_year,
        DATEFROMPARTS(s.price_year, 1, 1),
        'Imported via staging'
    FROM dbo.StgDamageCost s
    WHERE s.price_year IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM Damage.PriceBookVersion pb WHERE pb.year_label = s.price_year);

    /* Severity bands */
    INSERT INTO Damage.SeverityBand (damage_type_id, band_label, unit_id, range_min, range_max)
    SELECT DISTINCT
        dt.damage_type_id,
        ISNULL(NULLIF(s.severity_band_label, ''), 'default'),
        COALESCE(u.unit_id, (SELECT TOP 1 unit_id FROM Damage.Unit ORDER BY unit_id)),
        ISNULL(s.severity_min, 0),
        ISNULL(s.severity_max, 999999)
    FROM dbo.StgDamageCost s
    JOIN Damage.DamageType dt
      ON (@Lang = 'en' AND dt.code_en = ISNULL(NULLIF(s.damage_type_code_en, ''), ISNULL(s.damage_type_code_nl, 'UNKNOWN')))
      OR (@Lang = 'nl' AND dt.code_nl = ISNULL(NULLIF(s.damage_type_code_nl, ''), ISNULL(s.damage_type_code_en, 'UNKNOWN')))
    LEFT JOIN Damage.Unit u ON u.symbol = COALESCE(s.labor_unit, s.material_unit)
    WHERE NOT EXISTS (
        SELECT 1 FROM Damage.SeverityBand sb
        WHERE sb.damage_type_id = dt.damage_type_id
          AND sb.band_label = ISNULL(NULLIF(s.severity_band_label, ''), 'default')
    );

    /* Activities */
    INSERT INTO Damage.Activity (code_nl, code_en, name_nl, name_en, description)
    SELECT DISTINCT
        COALESCE(NULLIF(activity_code_nl, ''), NULLIF(activity_name_nl, ''), 'UNKNOWN'),
        COALESCE(NULLIF(activity_code_en, ''), NULLIF(activity_name_en, ''), 'UNKNOWN'),
        COALESCE(NULLIF(activity_name_nl, ''), NULLIF(activity_code_nl, ''), 'Unknown'),
        COALESCE(NULLIF(activity_name_en, ''), NULLIF(activity_code_en, ''), 'Unknown'),
        'Imported via staging'
    FROM dbo.StgDamageCost s
    WHERE NOT EXISTS (
        SELECT 1 FROM Damage.Activity a
        WHERE a.code_nl = COALESCE(NULLIF(s.activity_code_nl, ''), NULLIF(s.activity_name_nl, ''), 'UNKNOWN')
           OR a.code_en = COALESCE(NULLIF(s.activity_code_en, ''), NULLIF(s.activity_name_en, ''), 'UNKNOWN')
    );

    /* Bridge */
    INSERT INTO Damage.DamageTypeActivity (damage_type_id, activity_id, is_required, sequence_order, notes)
    SELECT DISTINCT
        dt.damage_type_id,
        a.activity_id,
        1,
        NULL,
        'Imported via staging'
    FROM dbo.StgDamageCost s
    JOIN Damage.DamageType dt
      ON (@Lang = 'en' AND dt.code_en = ISNULL(NULLIF(s.damage_type_code_en, ''), ISNULL(s.damage_type_code_nl, 'UNKNOWN')))
      OR (@Lang = 'nl' AND dt.code_nl = ISNULL(NULLIF(s.damage_type_code_nl, ''), ISNULL(s.damage_type_code_en, 'UNKNOWN')))
    JOIN Damage.Activity a
      ON (@Lang = 'en' AND a.code_en = ISNULL(NULLIF(s.activity_code_en, ''), ISNULL(s.activity_code_nl, 'UNKNOWN')))
      OR (@Lang = 'nl' AND a.code_nl = ISNULL(NULLIF(s.activity_code_nl, ''), ISNULL(s.activity_code_en, 'UNKNOWN')))
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
        s.labor_unit_cost,
        s.labor_min,
        s.labor_max,
        s.material_unit_cost,
        s.material_min,
        s.material_max,
        'Imported via staging'
    FROM dbo.StgDamageCost s
    JOIN Damage.DamageType dt
      ON (@Lang = 'en' AND dt.code_en = ISNULL(NULLIF(s.damage_type_code_en, ''), ISNULL(s.damage_type_code_nl, 'UNKNOWN')))
      OR (@Lang = 'nl' AND dt.code_nl = ISNULL(NULLIF(s.damage_type_code_nl, ''), ISNULL(s.damage_type_code_en, 'UNKNOWN')))
    JOIN Damage.Activity a
      ON (@Lang = 'en' AND a.code_en = ISNULL(NULLIF(s.activity_code_en, ''), ISNULL(s.activity_code_nl, 'UNKNOWN')))
      OR (@Lang = 'nl' AND a.code_nl = ISNULL(NULLIF(s.activity_code_nl, ''), ISNULL(s.activity_code_en, 'UNKNOWN')))
    JOIN Damage.SeverityBand sb
      ON sb.damage_type_id = dt.damage_type_id
     AND sb.band_label = ISNULL(NULLIF(s.severity_band_label, ''), 'default')
    JOIN Damage.PriceBookVersion pb ON pb.year_label = s.price_year
    LEFT JOIN Damage.Unit ul ON ul.symbol = s.labor_unit
    LEFT JOIN Damage.Unit um ON um.symbol = s.material_unit
    WHERE NOT EXISTS (
        SELECT 1 FROM Damage.ActivityCost ac
        WHERE ac.activity_id = a.activity_id
          AND ac.price_book_id = pb.price_book_id
          AND ac.severity_band_id = sb.severity_band_id
    );
END;
GO

PRINT '02_staging_and_load completed.';

