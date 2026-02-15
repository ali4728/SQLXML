/* ============================================================
   XSD -> SQL Generator Metadata Store
   - Tracks XSD schema sets (one "main" XSD + dependent/includes/imports)
   - Stores generated CREATE TABLE scripts
   - Stores execution "runs" (versioning / auditing)
   - Logs XML load events + errors

   Drop order respects foreign-key dependencies (children first).
   ============================================================ */

-- Drop in reverse dependency order
IF OBJECT_ID('dbo.SQLXML_PipelineErrorLog',      'U') IS NOT NULL DROP TABLE dbo.SQLXML_PipelineErrorLog;
GO
IF OBJECT_ID('dbo.SQLXML_XmlLoadRunTableLog',     'U') IS NOT NULL DROP TABLE dbo.SQLXML_XmlLoadRunTableLog;
GO
IF OBJECT_ID('dbo.SQLXML_XmlLoadRun',             'U') IS NOT NULL DROP TABLE dbo.SQLXML_XmlLoadRun;
GO
IF OBJECT_ID('dbo.SQLXML_SqlGeneratedScript',      'U') IS NOT NULL DROP TABLE dbo.SQLXML_SqlGeneratedScript;
GO
IF OBJECT_ID('dbo.SQLXML_XsdGenerationRun',        'U') IS NOT NULL DROP TABLE dbo.SQLXML_XsdGenerationRun;
GO
IF OBJECT_ID('dbo.SQLXML_XsdSchemaFile',           'U') IS NOT NULL DROP TABLE dbo.SQLXML_XsdSchemaFile;
GO
IF OBJECT_ID('dbo.SQLXML_XsdSchemaSet',            'U') IS NOT NULL DROP TABLE dbo.SQLXML_XsdSchemaSet;
GO

/* =========================
   1) Schema Set Registry
   ========================= */

CREATE TABLE dbo.SQLXML_XsdSchemaSet
(
    SchemaSetId          BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_XsdSchemaSet PRIMARY KEY,
    SchemaSetKey         NVARCHAR(200) NOT NULL,   -- your stable key (ex: 'CCDA_CDA', 'MyVendor_2026Q1')
    DisplayName          NVARCHAR(200) NULL,
    Description          NVARCHAR(1000) NULL,

    -- Versioning (your choice: semantic version, vendor version, date-based)
    VersionLabel         NVARCHAR(50)  NOT NULL,   -- ex: '1.0.0', '2026-02-14', 'v3'
    IsActive             BIT NOT NULL CONSTRAINT DF_SQLXML_XsdSchemaSet_IsActive DEFAULT (1),

    -- Optional: identify the “root” file (main XSD)
    RootXsdFileName      NVARCHAR(260) NULL,
    RootTargetNamespace  NVARCHAR(500) NULL,

    -- Content fingerprinting
    CombinedSha256       CHAR(64) NULL,            -- hash of canonicalized/concatenated content if you do that
    CreatedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_XsdSchemaSet_CreatedUtc DEFAULT (SYSUTCDATETIME()),
    CreatedBy            NVARCHAR(128) NULL,
    CONSTRAINT UQ_SQLXML_XsdSchemaSet_KeyVersion UNIQUE (SchemaSetKey, VersionLabel)
);
GO

/* =========================
   2) Individual XSD Files under a Schema Set
   ========================= */

CREATE TABLE dbo.SQLXML_XsdSchemaFile
(
    SchemaFileId         BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_XsdSchemaFile PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SQLXML_XsdSchemaFile_SchemaSet
                         REFERENCES dbo.SQLXML_XsdSchemaSet(SchemaSetId),

    FileRole             NVARCHAR(30) NOT NULL,    -- 'Root' | 'Include' | 'Import' | 'Redefine' | 'Other'
    FileName             NVARCHAR(260) NOT NULL,
    FilePathOrUri        NVARCHAR(1000) NULL,      -- where it came from (disk path, blob uri, etc.)
    TargetNamespace      NVARCHAR(500) NULL,

    ContentXml           XML NULL,                 -- if you store raw XML
    FileSha256           CHAR(64) NULL,

    ImportedUtc          DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_XsdSchemaFile_ImportedUtc DEFAULT (SYSUTCDATETIME()),
    ImportedBy           NVARCHAR(128) NULL,

    CONSTRAINT UQ_SQLXML_XsdSchemaFile_UniquePerSet UNIQUE (SchemaSetId, FileName)
);
GO

/* =========================
   3) Generator Runs (Execution Versioning)
   One row per “generation run” for a schema set
   ========================= */


CREATE TABLE dbo.SQLXML_XsdGenerationRun
(
    GenerationRunId      BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_XsdGenerationRun PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SQLXML_XsdGenerationRun_SchemaSet
                         REFERENCES dbo.SQLXML_XsdSchemaSet(SchemaSetId),

    -- This is your “execution versioning”
    RunVersion           INT NOT NULL,             -- increment per SchemaSetId
    ToolName             NVARCHAR(100) NULL,       -- ex: 'Xsd2SqlConsole'
    ToolVersion          NVARCHAR(50)  NULL,       -- ex: git tag/commit
    ConfigJson           NVARCHAR(MAX) NULL,       -- flattening rules, naming rules, etc.

    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_XsdGenerationRun_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_SQLXML_XsdGenerationRun_Status DEFAULT ('Running'),
    Message              NVARCHAR(2000) NULL
);
GO

-- Optional helper to enforce RunVersion increments per SchemaSetId (app-side or trigger; draft keeps it simple)


/* =========================
   4) Generated SQL Scripts (CREATE TABLE, indexes, constraints, etc.)
   ========================= */

CREATE TABLE dbo.SQLXML_SqlGeneratedScript
(
    ScriptId             BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_SqlGeneratedScript PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SQLXML_SqlGeneratedScript_SchemaSet
                         REFERENCES dbo.SQLXML_XsdSchemaSet(SchemaSetId),
    GenerationRunId      BIGINT NOT NULL CONSTRAINT FK_SQLXML_SqlGeneratedScript_Run
                         REFERENCES dbo.SQLXML_XsdGenerationRun(GenerationRunId),

    ScriptType           NVARCHAR(30) NOT NULL,    -- 'CreateTable' | 'AlterTable' | 'CreateIndex' | 'CreateSchema' | etc.
    TargetSchemaName     SYSNAME NULL,             -- ex: 'dbo', 'stg', etc.
    TargetObjectName     SYSNAME NULL,             -- table name, index name, etc.
    TargetObjectType     NVARCHAR(20) NULL,        -- 'TABLE' | 'INDEX' | 'VIEW' ...
    ScriptText           NVARCHAR(MAX) NOT NULL,
    ScriptSha256         CHAR(64) NULL,

    -- If you want “apply ordering”
    ApplyOrder           INT NULL,                 -- e.g., schemas first, tables next, FKs last

    CreatedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_SqlGeneratedScript_CreatedUtc DEFAULT (SYSUTCDATETIME())
);
GO

CREATE INDEX IX_SQLXML_SqlGeneratedScript_SchemaSet ON dbo.SQLXML_SqlGeneratedScript(SchemaSetId, GenerationRunId);
GO
CREATE INDEX IX_SQLXML_SqlGeneratedScript_Target ON dbo.SQLXML_SqlGeneratedScript(TargetSchemaName, TargetObjectName);
GO



/* =========================
   6) XML Load Runs (optional but usually needed)
   Track each inbound file/batch load using a given schema set.
   ========================= */

CREATE TABLE dbo.SQLXML_XmlLoadRun
(
    LoadRunId            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_XmlLoadRun PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SQLXML_XmlLoadRun_SchemaSet
                         REFERENCES dbo.SQLXML_XsdSchemaSet(SchemaSetId),

    SourceSystem         NVARCHAR(100) NULL,
    SourceFileName       NVARCHAR(260) NULL,
    SourceUri            NVARCHAR(1000) NULL,
    SourceFileSha256     CHAR(64) NULL,

    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_XmlLoadRun_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_SQLXML_XmlLoadRun_Status DEFAULT ('Running'),

    RowsInserted         BIGINT NULL,
    RowsUpdated          BIGINT NULL,
    RowsRejected         BIGINT NULL,

    Message              NVARCHAR(2000) NULL
);
GO

/* Row-level or table-level load logging (keep it table-level for sanity) */
CREATE TABLE dbo.SQLXML_XmlLoadRunTableLog
(
    LoadRunTableLogId    BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_XmlLoadRunTableLog PRIMARY KEY,
    LoadRunId            BIGINT NOT NULL CONSTRAINT FK_SQLXML_XmlLoadRunTableLog_LoadRun
                         REFERENCES dbo.SQLXML_XmlLoadRun(LoadRunId),

    TargetSchemaName     SYSNAME NOT NULL,
    TargetTableName      SYSNAME NOT NULL,
    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_XmlLoadRunTableLog_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_SQLXML_XmlLoadRunTableLog_Status DEFAULT ('Running'),

    RowsInserted         BIGINT NULL,
    RowsRejected         BIGINT NULL,
    ErrorMessage         NVARCHAR(4000) NULL
);
GO

CREATE INDEX IX_SQLXML_XmlLoadRunTableLog_LoadRun ON dbo.SQLXML_XmlLoadRunTableLog(LoadRunId, Status);
GO


/* =========================
   7) Central Error Log (optional but handy)
   ========================= */

CREATE TABLE dbo.SQLXML_PipelineErrorLog
(
    ErrorLogId           BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SQLXML_PipelineErrorLog PRIMARY KEY,
    OccurredUtc          DATETIME2(3) NOT NULL CONSTRAINT DF_SQLXML_PipelineErrorLog_OccurredUtc DEFAULT (SYSUTCDATETIME()),
    Area                 NVARCHAR(50) NOT NULL,     -- 'Generate' | 'Apply' | 'Load'
    SchemaSetId          BIGINT NULL,
    GenerationRunId      BIGINT NULL,
    LoadRunId            BIGINT NULL,

    ErrorNumber          INT NULL,
    ErrorMessage         NVARCHAR(4000) NOT NULL,
    ContextJson          NVARCHAR(MAX) NULL
);
GO
