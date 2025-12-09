/*
    Idempotent schema setup for the Damage model (bilingual codes/names).
    - GO-free for pyodbc execution.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

/* Ensure schema */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Damage')
    EXEC('CREATE SCHEMA Damage AUTHORIZATION dbo;');

/* Price books */
IF OBJECT_ID('Damage.PriceBookVersion') IS NULL
BEGIN
    CREATE TABLE Damage.PriceBookVersion (
        price_book_id      int IDENTITY(1,1) PRIMARY KEY,
        year_label         smallint NOT NULL,
        valid_from         date     NOT NULL,
        valid_to           date     NULL,
        source_reference   nvarchar(200) NULL,
        CONSTRAINT UQ_PriceBookVersion_Year UNIQUE (year_label)
    );
END;

/* Units */
IF OBJECT_ID('Damage.Unit') IS NULL
BEGIN
    CREATE TABLE Damage.Unit (
        unit_id                int IDENTITY(1,1) PRIMARY KEY,
        symbol                 nvarchar(20) NOT NULL UNIQUE,
        description            nvarchar(100) NULL,
        measurement_kind       nvarchar(20) NOT NULL DEFAULT('custom'),
        conversion_to_base     decimal(18,6) NOT NULL DEFAULT(1),
        base_symbol            nvarchar(20) NULL
    );
END;

/* Categories (bilingual codes/names) */
IF OBJECT_ID('Damage.DamageCategory') IS NULL
BEGIN
    CREATE TABLE Damage.DamageCategory (
        category_id        int IDENTITY(1,1) PRIMARY KEY,
        code_nl            nvarchar(100) NOT NULL UNIQUE,
        code_en            nvarchar(100) NOT NULL UNIQUE,
        name_nl            nvarchar(150) NOT NULL,
        name_en            nvarchar(150) NOT NULL,
        description        nvarchar(400) NULL,
        created_at         datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

/* Damage types (bilingual codes/names) */
IF OBJECT_ID('Damage.DamageType') IS NULL
BEGIN
    CREATE TABLE Damage.DamageType (
        damage_type_id         int IDENTITY(1,1) PRIMARY KEY,
        category_id            int NOT NULL CONSTRAINT FK_DamageType_Category REFERENCES Damage.DamageCategory(category_id),
        code_nl                nvarchar(150) NOT NULL UNIQUE,
        code_en                nvarchar(150) NOT NULL UNIQUE,
        name_nl                nvarchar(200) NOT NULL,
        name_en                nvarchar(200) NOT NULL,
        description            nvarchar(500) NULL,
        created_at             datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

/* Type keywords (language-aware) */
IF OBJECT_ID('Damage.DamageTypeKeyword') IS NULL
BEGIN
    CREATE TABLE Damage.DamageTypeKeyword (
        keyword_id        int IDENTITY(1,1) PRIMARY KEY,
        damage_type_id    int NOT NULL CONSTRAINT FK_DamageTypeKeyword_Type REFERENCES Damage.DamageType(damage_type_id),
        language_code     char(2) NOT NULL,
        keyword_text      nvarchar(150) NOT NULL
    );
END;

/* Severity bands */
IF OBJECT_ID('Damage.SeverityBand') IS NULL
BEGIN
    CREATE TABLE Damage.SeverityBand (
        severity_band_id   int IDENTITY(1,1) PRIMARY KEY,
        damage_type_id     int NOT NULL CONSTRAINT FK_SeverityBand_DamageType REFERENCES Damage.DamageType(damage_type_id),
        band_label         nvarchar(50) NOT NULL,
        unit_id            int NOT NULL CONSTRAINT FK_SeverityBand_Unit REFERENCES Damage.Unit(unit_id),
        range_min          decimal(18,4) NOT NULL,
        range_max          decimal(18,4) NOT NULL,
        CONSTRAINT CK_SeverityBand_Range CHECK (range_min <= range_max)
    );

    CREATE UNIQUE INDEX UX_SeverityBand_TypeRange
        ON Damage.SeverityBand (damage_type_id, range_min, range_max);
END;

/* Activities (bilingual codes/names) */
IF OBJECT_ID('Damage.Activity') IS NULL
BEGIN
    CREATE TABLE Damage.Activity (
        activity_id        int IDENTITY(1,1) PRIMARY KEY,
        code_nl            nvarchar(255) NOT NULL UNIQUE,
        code_en            nvarchar(255) NOT NULL UNIQUE,
        name_nl            nvarchar(255) NOT NULL,
        name_en            nvarchar(255) NOT NULL,
        description        nvarchar(800) NULL,
        default_unit_id    int NULL CONSTRAINT FK_Activity_Unit REFERENCES Damage.Unit(unit_id)
    );
END;

/* Bridge */
IF OBJECT_ID('Damage.DamageTypeActivity') IS NULL
BEGIN
    CREATE TABLE Damage.DamageTypeActivity (
        damage_type_activity_id int IDENTITY(1,1) PRIMARY KEY,
        damage_type_id          int NOT NULL CONSTRAINT FK_TypeActivity_DamageType REFERENCES Damage.DamageType(damage_type_id),
        activity_id             int NOT NULL CONSTRAINT FK_TypeActivity_Activity REFERENCES Damage.Activity(activity_id),
        is_required             bit NOT NULL DEFAULT 1,
        sequence_order          tinyint NULL,
        notes                   nvarchar(200) NULL,
        CONSTRAINT UQ_TypeActivity UNIQUE (damage_type_id, activity_id)
    );
END;

/* Activity costs with computed key for severity */
IF OBJECT_ID('Damage.ActivityCost') IS NULL
BEGIN
    CREATE TABLE Damage.ActivityCost (
        activity_cost_id   int IDENTITY(1,1) PRIMARY KEY,
        activity_id        int NOT NULL CONSTRAINT FK_ActivityCost_Activity REFERENCES Damage.Activity(activity_id),
        price_book_id      int NOT NULL CONSTRAINT FK_ActivityCost_PriceBook REFERENCES Damage.PriceBookVersion(price_book_id),
        severity_band_id   int NULL CONSTRAINT FK_ActivityCost_Severity REFERENCES Damage.SeverityBand(severity_band_id),
        severity_band_key  AS (ISNULL(severity_band_id, -1)) PERSISTED,
        labor_unit_id      int NULL CONSTRAINT FK_ActivityCost_LaborUnit REFERENCES Damage.Unit(unit_id),
        material_unit_id   int NULL CONSTRAINT FK_ActivityCost_MaterialUnit REFERENCES Damage.Unit(unit_id),
        labor_unit_cost    decimal(18,2) NULL,
        labor_cost_min     decimal(18,2) NULL,
        labor_cost_max     decimal(18,2) NULL,
        material_unit_cost decimal(18,2) NULL,
        material_cost_min  decimal(18,2) NULL,
        material_cost_max  decimal(18,2) NULL,
        notes              nvarchar(200) NULL,
        CONSTRAINT CK_ActivityCost_MinMax_Labor CHECK (
            (labor_cost_min IS NULL AND labor_cost_max IS NULL)
            OR (labor_cost_min <= labor_cost_max)
        ),
        CONSTRAINT CK_ActivityCost_MinMax_Material CHECK (
            (material_cost_min IS NULL AND material_cost_max IS NULL)
            OR (material_cost_min <= material_cost_max)
        )
    );
END;
ELSE
BEGIN
    IF COL_LENGTH('Damage.ActivityCost', 'severity_band_key') IS NULL
        ALTER TABLE Damage.ActivityCost ADD severity_band_key AS (ISNULL(severity_band_id, -1)) PERSISTED;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UX_ActivityCost_Key'
      AND object_id = OBJECT_ID('Damage.ActivityCost')
)
BEGIN
    CREATE UNIQUE INDEX UX_ActivityCost_Key
        ON Damage.ActivityCost (activity_id, price_book_id, severity_band_key);
END;

PRINT '01_schema_up completed (bilingual codes/names).';

