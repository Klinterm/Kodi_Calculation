/*
    View + estimator proc with language-aware code matching.
    GO-free; safe to rerun.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

/* Drop before create to simplify */
IF OBJECT_ID('Damage.vActivityCostFull') IS NOT NULL
    DROP VIEW Damage.vActivityCostFull;

IF OBJECT_ID('Damage.EstimateDamageCost') IS NOT NULL
    DROP PROCEDURE Damage.EstimateDamageCost;

/* Full join view */
CREATE VIEW Damage.vActivityCostFull AS
SELECT
    dc.category_id,
    dc.code_nl   AS category_code_nl,
    dc.code_en   AS category_code_en,
    dc.name_nl   AS category_name_nl,
    dc.name_en   AS category_name_en,
    dt.damage_type_id,
    dt.code_nl   AS damage_type_code_nl,
    dt.code_en   AS damage_type_code_en,
    dt.name_nl   AS damage_type_name_nl,
    dt.name_en   AS damage_type_name_en,
    sb.severity_band_id,
    sb.band_label,
    sb.range_min,
    sb.range_max,
    u_sb.symbol  AS severity_unit,
    a.activity_id,
    a.code_nl    AS activity_code_nl,
    a.code_en    AS activity_code_en,
    a.name_nl    AS activity_name_nl,
    a.name_en    AS activity_name_en,
    dta.is_required,
    dta.sequence_order,
    ac.activity_cost_id,
    ac.labor_cost_min,
    ac.labor_cost_max,
    ac.material_cost_min,
    ac.material_cost_max,
    ac.labor_unit_cost,
    ac.material_unit_cost,
    ul.symbol    AS labor_unit,
    um.symbol    AS material_unit,
    pb.year_label AS price_year
FROM Damage.DamageType dt
JOIN Damage.DamageCategory dc ON dc.category_id = dt.category_id
LEFT JOIN Damage.SeverityBand sb ON sb.damage_type_id = dt.damage_type_id
LEFT JOIN Damage.Unit u_sb ON u_sb.unit_id = sb.unit_id
JOIN Damage.DamageTypeActivity dta ON dta.damage_type_id = dt.damage_type_id
JOIN Damage.Activity a ON a.activity_id = dta.activity_id
JOIN Damage.ActivityCost ac
  ON ac.activity_id = a.activity_id
 AND (ac.severity_band_id = sb.severity_band_id OR (ac.severity_band_id IS NULL AND sb.severity_band_id IS NULL))
JOIN Damage.PriceBookVersion pb ON pb.price_book_id = ac.price_book_id
LEFT JOIN Damage.Unit ul ON ul.unit_id = ac.labor_unit_id
LEFT JOIN Damage.Unit um ON um.unit_id = ac.material_unit_id;

/* Estimator proc */
CREATE PROCEDURE Damage.EstimateDamageCost
    @DamageCode     nvarchar(150),
    @Language       char(2) = 'nl',  -- 'nl' or 'en'
    @SizeValue      decimal(18,4),
    @UnitSymbol     nvarchar(20),
    @PriceYear      smallint,
    @Verbose        bit = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Lang char(2) = LOWER(@Language);
    DECLARE @DamageTypeId int;

    SELECT TOP 1 @DamageTypeId = dt.damage_type_id
    FROM Damage.DamageType dt
    WHERE (@Lang = 'en' AND dt.code_en = @DamageCode)
       OR (@Lang = 'nl' AND dt.code_nl = @DamageCode)
       OR dt.code_nl = @DamageCode
       OR dt.code_en = @DamageCode;

    IF @DamageTypeId IS NULL
    BEGIN
        RAISERROR('Damage type not found for code %s.', 16, 1, @DamageCode);
        RETURN;
    END;

    DECLARE @PriceBookId int;
    SELECT @PriceBookId = price_book_id FROM Damage.PriceBookVersion WHERE year_label = @PriceYear;
    IF @PriceBookId IS NULL
    BEGIN
        RAISERROR('Price book for year %d not found.', 16, 1, @PriceYear);
        RETURN;
    END;

    DECLARE @InputUnitId int;
    DECLARE @ConversionToBase decimal(18,6) = 1;
    DECLARE @BaseSymbol nvarchar(20);

    SELECT
        @InputUnitId = unit_id,
        @ConversionToBase = conversion_to_base,
        @BaseSymbol = COALESCE(base_symbol, symbol)
    FROM Damage.Unit
    WHERE symbol = @UnitSymbol;

    IF @InputUnitId IS NULL
    BEGIN
        RAISERROR('Unit %s not found.', 16, 1, @UnitSymbol);
        RETURN;
    END;

    DECLARE @SizeInBase decimal(18,4) = @SizeValue * @ConversionToBase;

    DECLARE @SeverityBandId int;
    SELECT TOP(1)
        @SeverityBandId = severity_band_id
    FROM Damage.SeverityBand
    WHERE damage_type_id = @DamageTypeId
    ORDER BY
        CASE WHEN @SizeInBase BETWEEN range_min AND range_max THEN 0 ELSE 1 END,
        ABS((range_min + range_max) / 2.0 - @SizeInBase);

    IF @SeverityBandId IS NULL
    BEGIN
        RAISERROR('No severity band configured for damage type %s.', 16, 1, @DamageCode);
        RETURN;
    END;

    IF @Verbose = 1
    BEGIN
        SELECT sb.severity_band_id, sb.band_label, sb.range_min, sb.range_max
        FROM Damage.SeverityBand sb
        WHERE sb.severity_band_id = @SeverityBandId;
    END;

    ;WITH SelectedCosts AS (
        SELECT
            a.activity_id,
            CASE WHEN @Lang = 'en' THEN a.code_en ELSE a.code_nl END AS activity_code,
            CASE WHEN @Lang = 'en' THEN a.name_en ELSE a.name_nl END AS activity_name,
            ac.labor_unit_cost,
            ac.labor_cost_min,
            ac.labor_cost_max,
            ac.material_unit_cost,
            ac.material_cost_min,
            ac.material_cost_max,
            dta.sequence_order,
            dta.is_required,
            ul.symbol AS labor_unit,
            um.symbol AS material_unit
        FROM Damage.DamageTypeActivity dta
        JOIN Damage.Activity a ON a.activity_id = dta.activity_id
        JOIN Damage.ActivityCost ac ON ac.activity_id = a.activity_id
        LEFT JOIN Damage.Unit ul ON ul.unit_id = ac.labor_unit_id
        LEFT JOIN Damage.Unit um ON um.unit_id = ac.material_unit_id
        WHERE dta.damage_type_id = @DamageTypeId
          AND ac.price_book_id = @PriceBookId
          AND (ac.severity_band_id IS NULL OR ac.severity_band_id = @SeverityBandId)
    )
    SELECT
        activity_code,
        activity_name,
        is_required,
        sequence_order,
        labor_unit,
        material_unit,
        labor_cost_min,
        labor_cost_max,
        material_cost_min,
        material_cost_max,
        labor_unit_cost,
        material_unit_cost,
        EstimatedLabor = CASE
            WHEN labor_unit_cost IS NOT NULL THEN labor_unit_cost * @SizeInBase
            WHEN labor_cost_min IS NOT NULL AND labor_cost_max IS NOT NULL THEN (labor_cost_min + labor_cost_max) / 2.0
            ELSE labor_cost_min
        END,
        EstimatedMaterial = CASE
            WHEN material_unit_cost IS NOT NULL THEN material_unit_cost * @SizeInBase
            WHEN material_cost_min IS NOT NULL AND material_cost_max IS NOT NULL THEN (material_cost_min + material_cost_max) / 2.0
            ELSE material_cost_min
        END
    FROM SelectedCosts
    ORDER BY sequence_order, activity_code;
END;

PRINT '03_view_and_proc created.';

