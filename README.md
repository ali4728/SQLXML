# SQLXML

A .NET 8 console application that converts **XSD schemas into SQL Server tables** and **loads XML data into those tables**. Works with any XSD-defined XML structure.

## Features

- **Schema Registration** — Store XSD files (with all imports/includes) in a metadata database for versioned, repeatable use.
- **Schema Generation** — Generate SQL Server `CREATE TABLE` DDL scripts with primary keys, foreign keys, and proper column types from a registered XSD.
- **XML Data Loading** — Process a folder of XML files and insert data into SQL Server, respecting parent-child relationships and insert ordering.
- **Smart Flattening** — Singleton complex types are flattened into wide tables to minimize table count; repeating elements get their own child tables with `RepeatIndex` for ordering.
- **Column Overflow Handling** — Tables exceeding 300 columns are automatically split into `_Ext` extension tables.
- **Identifier Shortening** — Column and table names exceeding SQL Server's 128-character limit are automatically abbreviated using domain-aware rules.
- **Per-File Transactions** — Each XML file is processed in its own transaction; failures are isolated and do not affect prior successful inserts.
- **Metadata Tracking** — Schema sets, generation runs, and load metrics are recorded in a metadata database for auditing and lineage.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- SQL Server (local or remote) for data loading and metadata storage

## Build

```bash
dotnet build
```

## Configuration

Connection strings can be provided via `appsettings.json` or as command-line arguments. Command-line arguments take precedence.

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=MyData;Trusted_Connection=True;TrustServerCertificate=True",
    "MetadataConnection": "Server=localhost;Database=SQLXMLMeta;Trusted_Connection=True;TrustServerCertificate=True"
  }
}
```

- **DefaultConnection** — Target database for business tables and data loading (used by `process-file` and `process-sql`).
- **MetadataConnection** — Database for `SQLXML_*` metadata tables (used by all commands).

## Usage

The tool uses a three-step workflow: **register** an XSD, **generate DDL** from it, then **process** XML data (from files or a SQL table).

### 1. Register XSD Files

Store XSD files (including all imports/includes) in the metadata database.

```bash
SQLXML register --xsd <path-to-xsd> [--schema-name <name>] [--version <label>]
                [--source-config <json-file>]
                [--metadata-connection-string <conn-str>]
```

- `--schema-name` defaults to the root XSD filename (without extension).
- `--version` defaults to a hash-based label derived from file contents.
- `--source-config` (optional) stores a JSON file with source table settings for use with `process-sql`.
- If the same schema name + version already exists, registration is skipped.

**Examples:**

```bash
# Register XSD only
SQLXML register --xsd Schemas/MySchema.xsd --schema-name MySchema --version v1

# Register XSD with source config for process-sql
SQLXML register --xsd Schemas/MySchema.xsd --schema-name MySchema --version v1 \
  --source-config source-config.json
```

### 2. Generate SQL Schema from Registered XSD

Generate DDL from a previously registered schema set.

```bash
SQLXML generateddl --schema-name <name> --version <ver> [--output <output.sql>]
                    [--metadata-connection-string <conn-str>]
```

**Examples:**

```bash
# Print DDL to console
SQLXML generateddl --schema-name MySchema --version v1

# Write DDL to a file
SQLXML generateddl --schema-name MySchema --version v1 --output output/Tables.sql
```

### 3. Process XML Files and Load into SQL Server

Parse XML files from a folder and insert data into the target database using a registered schema.

```bash
SQLXML process-file --schema-name <name> --version <ver> --input <xml-folder>
                    --connection-string <conn-str> [--metadata-connection-string <conn-str>]
```

**Example:**

```bash
SQLXML process-file \
  --schema-name MySchema --version v1 \
  --input SampleFiles/xml \
  --connection-string "Server=localhost;Database=MyData;Trusted_Connection=True;TrustServerCertificate=True"
```

### 4. Process XML from a SQL Table

Read XML content from rows in a SQL table and load them into the target database. The source table connection and query are defined in a **source config** JSON file that must be registered first via `register --source-config`.

```bash
SQLXML process-sql --schema-name <name> --version <ver>
                   --connection-string <conn-str> [--metadata-connection-string <conn-str>]
```

**Source config JSON format:**

```json
{
  "sourceConnectionString": "Server=YOUR_SERVER;Database=YOUR_DB;Trusted_Connection=True;TrustServerCertificate=True;",
  "sourceQuery": "SELECT Id, HL7XML FROM dbo.InboundMessages WHERE ProcessedFlag = 0",
  "sourceIdColumn": "Id",
  "sourceXmlColumn": "HL7XML",
  "sourceUpdateQuery": "UPDATE dbo.InboundMessages SET ProcessedFlag = 1 WHERE Id = @Id"
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `sourceConnectionString` | Yes | — | Connection string to the source database containing XML rows |
| `sourceQuery` | Yes | — | Any valid `SELECT` query; extra `WHERE` conditions are fine |
| `sourceIdColumn` | No | `"Id"` | Column name used as the row identifier |
| `sourceXmlColumn` | No | `"HL7XML"` | Column name containing the XML content |
| `sourceUpdateQuery` | No | — | `UPDATE` statement with an `@Id` parameter; executed per row after **all** rows succeed |

**Example end-to-end workflow:**

```bash
# 1. Register XSD with source config
SQLXML register --xsd Schemas/ADT_A01.xsd --schema-name ADT_A01 --version v1 \
  --source-config source-config.json

# 2. Generate and apply DDL
SQLXML generateddl --schema-name ADT_A01 --version v1 --output Tables.sql

# 3. Process XML rows from the source table
SQLXML process-sql --schema-name ADT_A01 --version v1 \
  --connection-string "Server=localhost;Database=MyData;Trusted_Connection=True;TrustServerCertificate=True"
```

### Options

| Option | Commands | Description |
|---|---|---|
| `--xsd <path>` | `register` | Path to the root XSD schema file |
| `--schema-name <name>` | `register`, `generateddl`, `process-file`, `process-sql` | Logical name for the schema set (defaults to XSD filename) |
| `--version <label>` | `register`, `generateddl`, `process-file`, `process-sql` | Version label for the schema set |
| `--source-config <json-file>` | `register` | JSON file with source table settings for `process-sql` |
| `--output <path>` | `generateddl` | Output path for generated SQL script |
| `--input <folder>` | `process-file` | Folder containing XML files to process |
| `--connection-string <conn-str>` | `process-file`, `process-sql` | SQL Server connection string for the target database (or set via `DefaultConnection` in appsettings.json) |
| `--metadata-connection-string <conn-str>` | all | SQL Server connection string for the metadata database (or set via `MetadataConnection` in appsettings.json) |

## How It Works

### Registration

The `register` command loads the root XSD and all referenced files (`xs:import` / `xs:include`), normalizes schema locations, and stores the full content in the metadata database. Each registration is identified by a schema name + version pair.

### Schema Generation

1. The XSD is loaded from the metadata database and all type references are resolved into a complete type dictionary.
2. Each direct child of the message root with a complex type becomes its own SQL table with an identity PK and a foreign key to the root table.
3. Sub-element singleton complex types are **flattened** into their parent table as columns (up to a configurable depth).
4. Repeating elements (`maxOccurs > 1` or `unbounded`) get their own child tables with a `RepeatIndex` column.
5. Column names are derived from the XML path, joined by underscores.

### XML Processing

1. XML files are discovered in the input folder (`process-file`) or read from a SQL table (`process-sql`) and processed sequentially.
2. Each XML document is parsed into a tree of `RowData` objects matching the generated table structure.
3. Rows are inserted in dependency order (parent before child) within a single transaction per document.
4. Results are logged to the console with per-table row counts and error details.
5. For `process-sql`, if all rows succeed and a `sourceUpdateQuery` is configured, each source row is marked as processed.

## Project Structure

```
SQLXML/
├── Program.cs                  # CLI entry point (register, generateddl, process-file, process-sql)
├── Parsing/
│   ├── XsdParser.cs            # XSD parsing, type resolution, table/column generation
│   └── XsdLoader.cs            # XSD loading from disk or metadata DB content
├── Generation/
│   └── SqlGenerator.cs         # SQL DDL script generation from table definitions
├── Processing/
│   ├── XmlProcessor.cs         # XML-to-row-tree mapping using message structure
│   └── SqlServerLoader.cs      # SQL Server insertion with transaction support
├── Models/
│   ├── TableDefinition.cs      # Table, column, and foreign key models
│   ├── MessageStructure.cs     # Message slot definitions for XML processing
│   ├── ProcessingResult.cs     # Per-file processing result model
│   ├── LoadedXsdFileInfo.cs    # XSD file metadata model (disk loading)
│   └── SchemaFileRecord.cs     # XSD file metadata model (DB storage)
├── MetaData/
│   ├── MetadataRepository.cs   # Metadata/audit database operations
│   └── Sql/
│       └── CreateTables.sql    # DDL for metadata tables
├── Schemas/                    # Sample XSD schemas
└── SampleFiles/                # Sample XML files and test data
```

## License

This project is provided as-is. See repository for license details.
