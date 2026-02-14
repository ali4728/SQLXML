/* ============================================================
   XSD -> SQL Generator Metadata Store (First Draft)
   - Tracks XSD schema sets (one “main” XSD + dependent/includes/imports)
   - Stores generated CREATE TABLE scripts
   - Stores execution “runs” (versioning / auditing)
   - Logs apply/load events + errors
   ============================================================ */

-- Optional: put these in a dedicated schema
-- CREATE SCHEMA Meta AUTHORIZATION dbo;
-- GO

/* =========================
   1) Schema Set Registry
   ========================= */

IF OBJECT_ID('dbo.XsdSchemaSet', 'U') IS NOT NULL DROP TABLE dbo.XsdSchemaSet;
GO
CREATE TABLE dbo.XsdSchemaSet
(
    SchemaSetId          BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XsdSchemaSet PRIMARY KEY,
    SchemaSetKey         NVARCHAR(200) NOT NULL,   -- your stable key (ex: 'CCDA_CDA', 'MyVendor_2026Q1')
    DisplayName          NVARCHAR(200) NULL,
    Description          NVARCHAR(1000) NULL,

    -- Versioning (your choice: semantic version, vendor version, date-based)
    VersionLabel         NVARCHAR(50)  NOT NULL,   -- ex: '1.0.0', '2026-02-14', 'v3'
    IsActive             BIT NOT NULL CONSTRAINT DF_XsdSchemaSet_IsActive DEFAULT (1),

    -- Optional: identify the “root” file (main XSD)
    RootXsdFileName      NVARCHAR(260) NULL,
    RootTargetNamespace  NVARCHAR(500) NULL,

    -- Content fingerprinting
    CombinedSha256       CHAR(64) NULL,            -- hash of canonicalized/concatenated content if you do that
    CreatedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_XsdSchemaSet_CreatedUtc DEFAULT (SYSUTCDATETIME()),
    CreatedBy            NVARCHAR(128) NULL,
    Notes                NVARCHAR(2000) NULL,

    CONSTRAINT UQ_XsdSchemaSet_KeyVersion UNIQUE (SchemaSetKey, VersionLabel)
);
GO

/* =========================
   2) Individual XSD Files under a Schema Set
   ========================= */

IF OBJECT_ID('dbo.XsdSchemaFile', 'U') IS NOT NULL DROP TABLE dbo.XsdSchemaFile;
GO
CREATE TABLE dbo.XsdSchemaFile
(
    SchemaFileId         BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XsdSchemaFile PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_XsdSchemaFile_SchemaSet
                         REFERENCES dbo.XsdSchemaSet(SchemaSetId),

    FileRole             NVARCHAR(30) NOT NULL,    -- 'Root' | 'Include' | 'Import' | 'Redefine' | 'Other'
    FileName             NVARCHAR(260) NOT NULL,
    FilePathOrUri        NVARCHAR(1000) NULL,      -- where it came from (disk path, blob uri, etc.)
    TargetNamespace      NVARCHAR(500) NULL,
    LocationHint         NVARCHAR(1000) NULL,      -- schemaLocation/include path seen in XSD (optional)

    ContentXml           XML NULL,                 -- if you store raw XML
    ContentText          NVARCHAR(MAX) NULL,       -- or store text (pick one; you can keep both in draft)
    FileSha256           CHAR(64) NULL,

    ImportedUtc          DATETIME2(3) NOT NULL CONSTRAINT DF_XsdSchemaFile_ImportedUtc DEFAULT (SYSUTCDATETIME()),
    ImportedBy           NVARCHAR(128) NULL,

    CONSTRAINT UQ_XsdSchemaFile_UniquePerSet UNIQUE (SchemaSetId, FileName)
);
GO

/* =========================
   3) Generator Runs (Execution Versioning)
   One row per “generation run” for a schema set
   ========================= */

IF OBJECT_ID('dbo.XsdGenerationRun', 'U') IS NOT NULL DROP TABLE dbo.XsdGenerationRun;
GO
CREATE TABLE dbo.XsdGenerationRun
(
    GenerationRunId      BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XsdGenerationRun PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_XsdGenerationRun_SchemaSet
                         REFERENCES dbo.XsdSchemaSet(SchemaSetId),

    -- This is your “execution versioning”
    RunVersion           INT NOT NULL,             -- increment per SchemaSetId
    ToolName             NVARCHAR(100) NULL,       -- ex: 'Xsd2SqlConsole'
    ToolVersion          NVARCHAR(50)  NULL,       -- ex: git tag/commit
    ConfigJson           NVARCHAR(MAX) NULL,       -- flattening rules, naming rules, etc.

    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_XsdGenerationRun_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_XsdGenerationRun_Status DEFAULT ('Running'),
    Message              NVARCHAR(2000) NULL
);
GO

-- Optional helper to enforce RunVersion increments per SchemaSetId (app-side or trigger; draft keeps it simple)


/* =========================
   4) Generated SQL Scripts (CREATE TABLE, indexes, constraints, etc.)
   ========================= */

IF OBJECT_ID('dbo.SqlGeneratedScript', 'U') IS NOT NULL DROP TABLE dbo.SqlGeneratedScript;
GO
CREATE TABLE dbo.SqlGeneratedScript
(
    ScriptId             BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SqlGeneratedScript PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SqlGeneratedScript_SchemaSet
                         REFERENCES dbo.XsdSchemaSet(SchemaSetId),
    GenerationRunId      BIGINT NOT NULL CONSTRAINT FK_SqlGeneratedScript_Run
                         REFERENCES dbo.XsdGenerationRun(GenerationRunId),

    ScriptType           NVARCHAR(30) NOT NULL,    -- 'CreateTable' | 'AlterTable' | 'CreateIndex' | 'CreateSchema' | etc.
    TargetSchemaName     SYSNAME NULL,             -- ex: 'dbo', 'stg', etc.
    TargetObjectName     SYSNAME NULL,             -- table name, index name, etc.
    TargetObjectType     NVARCHAR(20) NULL,        -- 'TABLE' | 'INDEX' | 'VIEW' ...
    ScriptText           NVARCHAR(MAX) NOT NULL,
    ScriptSha256         CHAR(64) NULL,

    -- If you want “apply ordering”
    ApplyOrder           INT NULL,                 -- e.g., schemas first, tables next, FKs last

    CreatedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SqlGeneratedScript_CreatedUtc DEFAULT (SYSUTCDATETIME())
);
GO

CREATE INDEX IX_SqlGeneratedScript_SchemaSet ON dbo.SqlGeneratedScript(SchemaSetId, GenerationRunId);
GO
CREATE INDEX IX_SqlGeneratedScript_Target ON dbo.SqlGeneratedScript(TargetSchemaName, TargetObjectName);
GO


/* =========================
   5) Apply Scripts to a Database (DDL apply/audit)
   One row per apply attempt.
   ========================= */

IF OBJECT_ID('dbo.SqlApplyRun', 'U') IS NOT NULL DROP TABLE dbo.SqlApplyRun;
GO
CREATE TABLE dbo.SqlApplyRun
(
    ApplyRunId           BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SqlApplyRun PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_SqlApplyRun_SchemaSet
                         REFERENCES dbo.XsdSchemaSet(SchemaSetId),
    GenerationRunId      BIGINT NULL CONSTRAINT FK_SqlApplyRun_GenerationRun
                         REFERENCES dbo.XsdGenerationRun(GenerationRunId),

    TargetDatabaseName   SYSNAME NOT NULL,
    TargetServerName     NVARCHAR(200) NULL,       -- optional
    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SqlApplyRun_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_SqlApplyRun_Status DEFAULT ('Running'),

    AppliedBy            NVARCHAR(128) NULL,
    Message              NVARCHAR(2000) NULL
);
GO

/* Detail rows: which scripts were applied, and outcomes */
IF OBJECT_ID('dbo.SqlApplyRunDetail', 'U') IS NOT NULL DROP TABLE dbo.SqlApplyRunDetail;
GO
CREATE TABLE dbo.SqlApplyRunDetail
(
    ApplyRunDetailId     BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SqlApplyRunDetail PRIMARY KEY,
    ApplyRunId           BIGINT NOT NULL CONSTRAINT FK_SqlApplyRunDetail_ApplyRun
                         REFERENCES dbo.SqlApplyRun(ApplyRunId),
    ScriptId             BIGINT NOT NULL CONSTRAINT FK_SqlApplyRunDetail_Script
                         REFERENCES dbo.SqlGeneratedScript(ScriptId),

    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_SqlApplyRunDetail_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_SqlApplyRunDetail_Status DEFAULT ('Running'),
    ErrorNumber          INT NULL,
    ErrorMessage         NVARCHAR(4000) NULL
);
GO

CREATE INDEX IX_SqlApplyRunDetail_ApplyRun ON dbo.SqlApplyRunDetail(ApplyRunId, Status);
GO


/* =========================
   6) XML Load Runs (optional but usually needed)
   Track each inbound file/batch load using a given schema set.
   ========================= */

IF OBJECT_ID('dbo.XmlLoadRun', 'U') IS NOT NULL DROP TABLE dbo.XmlLoadRun;
GO
CREATE TABLE dbo.XmlLoadRun
(
    LoadRunId            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XmlLoadRun PRIMARY KEY,
    SchemaSetId          BIGINT NOT NULL CONSTRAINT FK_XmlLoadRun_SchemaSet
                         REFERENCES dbo.XsdSchemaSet(SchemaSetId),

    SourceSystem         NVARCHAR(100) NULL,
    SourceFileName       NVARCHAR(260) NULL,
    SourceUri            NVARCHAR(1000) NULL,
    SourceFileSha256     CHAR(64) NULL,

    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_XmlLoadRun_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_XmlLoadRun_Status DEFAULT ('Running'),

    RowsInserted         BIGINT NULL,
    RowsUpdated          BIGINT NULL,
    RowsRejected         BIGINT NULL,

    Message              NVARCHAR(2000) NULL
);
GO

/* Row-level or table-level load logging (keep it table-level for sanity) */
IF OBJECT_ID('dbo.XmlLoadRunTableLog', 'U') IS NOT NULL DROP TABLE dbo.XmlLoadRunTableLog;
GO
CREATE TABLE dbo.XmlLoadRunTableLog
(
    LoadRunTableLogId    BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XmlLoadRunTableLog PRIMARY KEY,
    LoadRunId            BIGINT NOT NULL CONSTRAINT FK_XmlLoadRunTableLog_LoadRun
                         REFERENCES dbo.XmlLoadRun(LoadRunId),

    TargetSchemaName     SYSNAME NOT NULL,
    TargetTableName      SYSNAME NOT NULL,
    StartedUtc           DATETIME2(3) NOT NULL CONSTRAINT DF_XmlLoadRunTableLog_StartedUtc DEFAULT (SYSUTCDATETIME()),
    FinishedUtc          DATETIME2(3) NULL,
    Status               NVARCHAR(20) NOT NULL CONSTRAINT DF_XmlLoadRunTableLog_Status DEFAULT ('Running'),

    RowsInserted         BIGINT NULL,
    RowsRejected         BIGINT NULL,
    ErrorMessage         NVARCHAR(4000) NULL
);
GO

CREATE INDEX IX_XmlLoadRunTableLog_LoadRun ON dbo.XmlLoadRunTableLog(LoadRunId, Status);
GO


/* =========================
   7) Central Error Log (optional but handy)
   ========================= */

IF OBJECT_ID('dbo.PipelineErrorLog', 'U') IS NOT NULL DROP TABLE dbo.PipelineErrorLog;
GO
CREATE TABLE dbo.PipelineErrorLog
(
    ErrorLogId           BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PipelineErrorLog PRIMARY KEY,
    OccurredUtc          DATETIME2(3) NOT NULL CONSTRAINT DF_PipelineErrorLog_OccurredUtc DEFAULT (SYSUTCDATETIME()),
    Area                 NVARCHAR(50) NOT NULL,     -- 'Generate' | 'Apply' | 'Load'
    SchemaSetId          BIGINT NULL,
    GenerationRunId      BIGINT NULL,
    ApplyRunId           BIGINT NULL,
    LoadRunId            BIGINT NULL,

    ErrorNumber          INT NULL,
    ErrorMessage         NVARCHAR(4000) NOT NULL,
    ContextJson          NVARCHAR(MAX) NULL
);
GO
