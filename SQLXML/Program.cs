using System.Xml.Linq;
using Microsoft.Extensions.Configuration;
using SQLXML.Generation;
using SQLXML.MetaData;
using SQLXML.Models;
using SQLXML.Parsing;
using SQLXML.Processing;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}.json", optional: true)
    .Build();

if (args.Length < 1)
{
    PrintUsage();
    return 1;
}

var command = args[0];

// ═══════════════════════════════════════════════════════════════════
// REGISTER — save XSD files to metadata DB
// ═══════════════════════════════════════════════════════════════════
if (command == "register")
{
    string? xsdPath = null;
    string? schemaName = null;
    string? version = null;
    string? metadataConnectionString = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--xsd" when i + 1 < args.Length:
                xsdPath = args[++i];
                break;
            case "--schema-name" when i + 1 < args.Length:
                schemaName = args[++i];
                break;
            case "--version" when i + 1 < args.Length:
                version = args[++i];
                break;
            case "--metadata-connection-string" when i + 1 < args.Length:
                metadataConnectionString = args[++i];
                break;
        }
    }

    metadataConnectionString ??= configuration.GetConnectionString("MetadataConnection");

    if (xsdPath == null || metadataConnectionString == null)
    {
        Console.Error.WriteLine("Error: --xsd and --metadata-connection-string are required.");
        PrintUsage();
        return 1;
    }

    if (!File.Exists(xsdPath))
    {
        Console.Error.WriteLine($"Error: XSD file not found: {xsdPath}");
        return 1;
    }

    // Load XSD chain from disk (with normalized schemaLocation + ContentXml)
    var (files, _, _) = XsdLoader.LoadFromDisk(xsdPath);

    var rootFile = files.FirstOrDefault(f => f.FileRole == "Root");
    var rootFileName = rootFile?.FileName ?? Path.GetFileName(xsdPath);

    // Auto-derive schemaName from root XSD filename when not provided
    schemaName ??= Path.GetFileNameWithoutExtension(rootFileName);
    schemaName = NormalizeSchemaName(schemaName);

    // Compute per-file and combined SHA-256
    var combinedSha = MetadataRepository.ComputeCombinedSha256(
        files.Select(f => f.FilePath));
    var versionLabel = version ?? combinedSha[..12];

    using var meta = new MetadataRepository(metadataConnectionString);
    meta.EnsureMetadataTables();

    // Skip re-registration if same key+version already exists
    var existingId = meta.FindActiveSchemaSet(schemaName, versionLabel);
    if (existingId != null)
    {
        Console.WriteLine($"Schema set '{schemaName}' version '{versionLabel}' already registered (SchemaSetId={existingId}).");
        return 0;
    }

    var schemaSetId = meta.InsertSchemaSet(
        schemaSetKey: schemaName,
        versionLabel: versionLabel,
        rootXsdFileName: rootFileName,
        rootTargetNamespace: rootFile?.TargetNamespace,
        displayName: rootFileName,
        combinedSha256: combinedSha,
        createdBy: Environment.UserName);

    foreach (var file in files)
    {
        var sha = MetadataRepository.ComputeFileSha256(file.FilePath);
        meta.InsertSchemaFile(
            schemaSetId, file.FileRole, file.FileName,
            file.FilePath, file.TargetNamespace, sha,
            contentXml: file.ContentXml,
            importedBy: Environment.UserName);
    }

    Console.WriteLine($"Registered schema set '{schemaName}' version '{versionLabel}' (SchemaSetId={schemaSetId}).");
    Console.WriteLine($"  {files.Count} XSD file(s) stored.");
    return 0;
}

// ═══════════════════════════════════════════════════════════════════
// SCHEMA — generate DDL from XSD stored in metadata DB
// ═══════════════════════════════════════════════════════════════════
if (command == "generateddl")
{
    string? schemaName = null;
    string? version = null;
    string? outputPath = null;
    string? metadataConnectionString = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--schema-name" when i + 1 < args.Length:
                schemaName = args[++i];
                break;
            case "--version" when i + 1 < args.Length:
                version = args[++i];
                break;
            case "--output" when i + 1 < args.Length:
                outputPath = args[++i];
                break;
            case "--metadata-connection-string" when i + 1 < args.Length:
                metadataConnectionString = args[++i];
                break;
        }
    }

    metadataConnectionString ??= configuration.GetConnectionString("MetadataConnection");

    if (schemaName != null) schemaName = NormalizeSchemaName(schemaName);

    if (schemaName == null || version == null || metadataConnectionString == null)
    {
        Console.Error.WriteLine("Error: --schema-name, --version, and --metadata-connection-string are required.");
        PrintUsage();
        return 1;
    }

    // Load XSD content from metadata DB
    using var meta = new MetadataRepository(metadataConnectionString);
    meta.EnsureMetadataTables();

    var schemaSetId = meta.FindActiveSchemaSet(schemaName, version);
    if (schemaSetId == null)
    {
        Console.Error.WriteLine($"Error: schema set '{schemaName}' version '{version}' not found.");
        return 1;
    }

    var schemaFiles = meta.GetSchemaFiles(schemaSetId.Value);
    if (schemaFiles.Count == 0)
    {
        Console.Error.WriteLine("Error: no XSD files found for this schema set.");
        return 1;
    }

    var rootFile = schemaFiles.FirstOrDefault(f => f.FileRole == "Root");
    if (rootFile == null)
    {
        Console.Error.WriteLine("Error: no root XSD file found in schema set.");
        return 1;
    }

    var xsdContentByFileName = schemaFiles.ToDictionary(f => f.FileName, f => f.ContentXml);
    var (docs, prefixMaps) = XsdLoader.LoadFromContent(xsdContentByFileName, rootFile.FileName);

    var parser = new XsdParser();
    var (tables, _) = parser.Parse(docs, prefixMaps);
    var sql = SqlGenerator.Generate(tables);

    if (outputPath != null)
    {
        File.WriteAllText(outputPath, sql);
        Console.WriteLine($"Schema written to {outputPath}");
        Console.WriteLine($"Generated {tables.Count} tables.");
    }
    else
    {
        Console.Write(sql);
    }

    // Record generation run in metadata
    var runId = meta.InsertGenerationRun(
        schemaSetId.Value,
        toolName: "SQLXML",
        toolVersion: "1.0.0");

    try
    {
        int order = 0;
        foreach (var table in tables.OrderBy(t => t.SortOrder))
        {
            var tableScript = SqlGenerator.Generate(new List<TableDefinition> { table });
            meta.InsertGeneratedScript(
                schemaSetId.Value, runId,
                scriptType: "CreateTable",
                targetSchemaName: "dbo",
                targetObjectName: table.TableName,
                targetObjectType: "TABLE",
                scriptText: tableScript,
                applyOrder: order++);
        }

        meta.CompleteGenerationRun(runId, "Completed",
            $"Generated {tables.Count} tables.");
        Console.WriteLine($"Metadata recorded: SchemaSetId={schemaSetId}, GenerationRunId={runId}");
    }
    catch (Exception ex)
    {
        meta.CompleteGenerationRun(runId, "Failed", ex.Message);
        meta.LogPipelineError("Generate", ex.Message,
            schemaSetId: schemaSetId, generationRunId: runId);
        Console.Error.WriteLine($"Warning: metadata recording failed: {ex.Message}");
    }

    return 0;
}

// ═══════════════════════════════════════════════════════════════════
// PROCESS — load XML data using XSD from metadata DB
// ═══════════════════════════════════════════════════════════════════
if (command == "process")
{
    string? schemaName = null;
    string? version = null;
    string? inputFolder = null;
    string? connectionString = null;
    string? metadataConnectionString = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--schema-name" when i + 1 < args.Length:
                schemaName = args[++i];
                break;
            case "--version" when i + 1 < args.Length:
                version = args[++i];
                break;
            case "--input" when i + 1 < args.Length:
                inputFolder = args[++i];
                break;
            case "--connection-string" when i + 1 < args.Length:
                connectionString = args[++i];
                break;
            case "--metadata-connection-string" when i + 1 < args.Length:
                metadataConnectionString = args[++i];
                break;
        }
    }

    connectionString ??= configuration.GetConnectionString("DefaultConnection");
    metadataConnectionString ??= configuration.GetConnectionString("MetadataConnection");

    if (schemaName != null) schemaName = NormalizeSchemaName(schemaName);

    if (schemaName == null || version == null || inputFolder == null
        || connectionString == null || metadataConnectionString == null)
    {
        Console.Error.WriteLine("Error: --schema-name, --version, --input, --connection-string, and --metadata-connection-string are required.");
        PrintUsage();
        return 1;
    }

    if (!Directory.Exists(inputFolder))
    {
        Console.Error.WriteLine($"Error: Input folder not found: {inputFolder}");
        return 1;
    }

    // Load XSD content from metadata DB
    using var meta = new MetadataRepository(metadataConnectionString);
    meta.EnsureMetadataTables();

    var schemaSetId = meta.FindActiveSchemaSet(schemaName, version);
    if (schemaSetId == null)
    {
        Console.Error.WriteLine($"Error: schema set '{schemaName}' version '{version}' not found.");
        return 1;
    }

    var schemaFiles = meta.GetSchemaFiles(schemaSetId.Value);
    if (schemaFiles.Count == 0)
    {
        Console.Error.WriteLine("Error: no XSD files found for this schema set.");
        return 1;
    }

    var rootFile = schemaFiles.FirstOrDefault(f => f.FileRole == "Root");
    if (rootFile == null)
    {
        Console.Error.WriteLine("Error: no root XSD file found in schema set.");
        return 1;
    }

    var xsdContentByFileName = schemaFiles.ToDictionary(f => f.FileName, f => f.ContentXml);
    var (docs, prefixMaps) = XsdLoader.LoadFromContent(xsdContentByFileName, rootFile.FileName);

    var parser = new XsdParser();
    var (tables, structure) = parser.Parse(docs, prefixMaps);

    Console.WriteLine($"Parsed XSD: {tables.Count} tables, {structure.Slots.Count} message slots.");

    var xmlFiles = Directory.GetFiles(inputFolder, "*.xml");
    if (xmlFiles.Length == 0)
    {
        Console.WriteLine("No XML files found in input folder.");
        return 0;
    }

    Console.WriteLine($"Found {xmlFiles.Length} XML file(s) to process.");

    var results = new List<ProcessingResult>();
    var processor = new XmlProcessor(tables, structure);

    using var loader = new SqlServerLoader(connectionString, tables);

    foreach (var xmlFile in xmlFiles)
    {
        var result = new ProcessingResult { FileName = Path.GetFileName(xmlFile) };
        long loadRunId = 0;

        try
        {
            var fileSha = MetadataRepository.ComputeFileSha256(xmlFile);
            loadRunId = meta.InsertXmlLoadRun(
                schemaSetId.Value,
                sourceFileName: Path.GetFileName(xmlFile),
                sourceUri: Path.GetFullPath(xmlFile),
                sourceFileSha256: fileSha);

            var xml = XDocument.Load(xmlFile);
            var rowTree = processor.ProcessFile(xml);

            loader.BeginTransaction();
            var rowCounts = loader.InsertRowTree(rowTree);
            loader.Commit();

            foreach (var kv in rowCounts)
                result.RowCounts[kv.Key] = kv.Value;

            result.Success = true;

            long totalRows = 0;
            foreach (var kv in rowCounts)
            {
                var logId = meta.InsertXmlLoadRunTableLog(loadRunId, "dbo", kv.Key);
                meta.CompleteXmlLoadRunTableLog(logId, "Completed", rowsInserted: kv.Value);
                totalRows += kv.Value;
            }
            meta.CompleteXmlLoadRun(loadRunId, "Completed", rowsInserted: totalRows);
        }
        catch (Exception ex)
        {
            try { loader.Rollback(); } catch { /* ignore rollback errors */ }
            result.Success = false;
            result.ErrorMessage = ex.Message;

            if (loadRunId > 0)
            {
                meta.CompleteXmlLoadRun(loadRunId, "Failed", message: ex.Message);
                meta.LogPipelineError("Load", ex.Message,
                    schemaSetId: schemaSetId, loadRunId: loadRunId);
            }
        }

        results.Add(result);
    }

    // Print results
    Console.WriteLine();
    Console.WriteLine("Processing Results:");
    Console.WriteLine(new string('-', 60));

    foreach (var result in results)
    {
        var status = result.Success ? "Success" : "FAILED";
        Console.WriteLine($"  {result.FileName}: {status}");

        if (result.Success)
        {
            foreach (var kv in result.RowCounts.OrderBy(kv => kv.Key))
            {
                Console.WriteLine($"    {kv.Key}: {kv.Value} row(s)");
            }
        }
        else
        {
            Console.WriteLine($"    Error: {result.ErrorMessage}");
        }
    }

    return results.All(r => r.Success) ? 0 : 1;
}

Console.Error.WriteLine($"Unknown command: {command}");
PrintUsage();
return 1;

static void PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML register --xsd <path> [--schema-name <name>] [--version <label>]");
    Console.Error.WriteLine("                  [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML generateddl --schema-name <name> --version <ver> [--output <sql-file>]");
    Console.Error.WriteLine("                [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML process --schema-name <name> --version <ver> --input <xml-folder>");
    Console.Error.WriteLine("                 --connection-string <conn-str> [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Options:");
    Console.Error.WriteLine("  --connection-string           Target database for business tables / data loading");
    Console.Error.WriteLine("  --metadata-connection-string  Database for SQLXML_* metadata tables (can be a different server)");
}

static string NormalizeSchemaName(string name)
{
    if (name.EndsWith(".xsd", StringComparison.OrdinalIgnoreCase))
        return name[..^4];
    return name;
}
