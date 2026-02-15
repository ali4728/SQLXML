# SQLXML

A .NET 8 console application that converts **XSD schemas into SQL Server tables** and **loads XML data into those tables**. Built for working with HL7 v2 messages represented as XML, but applicable to any XSD-defined XML structure.

## Features

- **Schema Generation** — Parse an XSD file and generate SQL Server `CREATE TABLE` DDL scripts with primary keys, foreign keys, and proper column types.
- **XML Data Loading** — Process a folder of XML files and insert data into SQL Server, respecting parent-child relationships and insert ordering.
- **Smart Flattening** — Singleton complex types are flattened into wide tables to minimize table count; repeating elements get their own child tables with `RepeatIndex` for ordering.
- **Column Overflow Handling** — Tables exceeding 300 columns are automatically split into `_Ext` extension tables.
- **Identifier Shortening** — Column and table names exceeding SQL Server's 128-character limit are automatically abbreviated using HL7/medical domain-aware rules.
- **Per-File Transactions** — Each XML file is processed in its own transaction; failures are isolated and do not affect prior successful inserts.
- **Optional Metadata Tracking** — Record schema sets, generation runs, and load metrics in a separate metadata database for auditing and lineage.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- SQL Server (local or remote) for data loading

## Build

```bash
dotnet build
```

## Usage

### 1. Generate SQL Schema from XSD

```bash
SQLXML schema --xsd <path-to-xsd> [--output <output.sql>] [--metadata-connection-string <conn-str>]
```

**Examples:**

```bash
# Print DDL to console
SQLXML schema --xsd Schemas/ADT/ADT_A01_26_GLO_DEF_CUSTOM_HCA.xsd

# Write DDL to a file
SQLXML schema --xsd Schemas/ADT/ADT_A01_26_GLO_DEF_CUSTOM_HCA.xsd --output output/ADT_Tables.sql
```

### 2. Process XML Files and Load into SQL Server

```bash
SQLXML process --xsd <path-to-xsd> --input <xml-folder> --connection-string <conn-str> [--metadata-connection-string <conn-str>]
```

**Example:**

```bash
SQLXML process \
  --xsd Schemas/ADT/ADT_A01_26_GLO_DEF_CUSTOM_HCA.xsd \
  --input SampleFiles/xml \
  --connection-string "Server=localhost;Database=HL7Data;Trusted_Connection=True;TrustServerCertificate=True"
```

### Options

| Option | Description |
|---|---|
| `--xsd <path>` | Path to the root XSD schema file (required) |
| `--output <path>` | Output path for generated SQL script (schema mode only) |
| `--input <folder>` | Folder containing XML files to process (process mode only) |
| `--connection-string <conn-str>` | SQL Server connection string for the target database (process mode only) |
| `--metadata-connection-string <conn-str>` | SQL Server connection string for metadata/audit database (optional, both modes) |

## How It Works

### Schema Generation

1. The XSD parser loads the root XSD and follows all `xs:import` / `xs:include` references to build a complete type dictionary.
2. Each HL7 segment (direct child of the message root) becomes its own SQL table with an identity PK and a foreign key to the root message table.
3. Sub-segment singleton complex types are **flattened** into their parent table as columns (up to a configurable depth).
4. Repeating elements (`maxOccurs > 1` or `unbounded`) get their own child tables with a `RepeatIndex` column.
5. Column names are derived from the XML path (e.g., `PID_11_PatientAddress_XAD_3_City`).

### XML Processing

1. XML files are discovered in the input folder and processed sequentially.
2. Each file is parsed into a tree of `RowData` objects matching the generated table structure.
3. Rows are inserted in dependency order (parent before child) within a single transaction per file.
4. Results are logged to the console with per-table row counts and error details.

## Project Structure

```
SQLXML/
├── Program.cs                  # CLI entry point with schema and process commands
├── Parsing/
│   └── XsdParser.cs            # XSD parsing, type resolution, table/column generation
├── Generation/
│   └── SqlGenerator.cs         # SQL DDL script generation from table definitions
├── Processing/
│   ├── XmlProcessor.cs         # XML-to-row-tree mapping using message structure
│   └── SqlServerLoader.cs      # SQL Server insertion with transaction support
├── Models/
│   ├── TableDefinition.cs      # Table, column, and foreign key models
│   ├── MessageStructure.cs     # Message slot definitions for XML processing
│   ├── ProcessingResult.cs     # Per-file processing result model
│   └── LoadedXsdFileInfo.cs    # XSD file metadata model
├── MetaData/
│   ├── MetadataRepository.cs   # Metadata/audit database operations
│   └── Sql/
│       └── CreateTables.sql    # DDL for metadata tables
├── Schemas/                    # Sample XSD schemas (HL7 v2.6 ADT)
└── SampleFiles/                # Sample XML files and test data
```

## License

This project is provided as-is. See repository for license details.
