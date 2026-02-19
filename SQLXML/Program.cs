using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SQLXML.Generation;
using SQLXML.MetaData;
using SQLXML.Models;
using SQLXML.Parsing;
using SQLXML;
using SQLXML.Inference;
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
    string? sourceConfigPath = null;

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
            case "--source-config" when i + 1 < args.Length:
                sourceConfigPath = args[++i];
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

    string? sourceConfigJson = null;
    if (sourceConfigPath != null)
    {
        if (!File.Exists(sourceConfigPath))
        {
            Console.Error.WriteLine($"Error: source config file not found: {sourceConfigPath}");
            return 1;
        }
        sourceConfigJson = File.ReadAllText(sourceConfigPath);
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
        createdBy: Environment.UserName,
        sourceConfigJson: sourceConfigJson);

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
    string? tablePrefix = null;

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
            case "--table-prefix" when i + 1 < args.Length:
                tablePrefix = args[++i];
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

    if (!string.IsNullOrWhiteSpace(tablePrefix))
        TablePrefixHelper.ApplyPrefix(tables, null, tablePrefix);

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

        if (!string.IsNullOrWhiteSpace(tablePrefix))
            meta.SaveTablePrefix(schemaSetId.Value, TablePrefixHelper.NormalizePrefix(tablePrefix));

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
// PROCESS-FILE — load XML data from files using XSD from metadata DB
// ═══════════════════════════════════════════════════════════════════
if (command == "process-file")
{
    string? schemaName = null;
    string? version = null;
    string? inputFolder = null;
    string? connectionString = null;
    string? metadataConnectionString = null;
    string? deleteSourceFiles = null;

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
            case "--delete-source-files" when i + 1 < args.Length:
                deleteSourceFiles = args[++i];
                break;
        }
    }

    var shouldDeleteSourceFiles = string.Equals(deleteSourceFiles, "Y", StringComparison.OrdinalIgnoreCase);

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

    var (tables, structure, meta, schemaSetId) = LoadSchemaSet(schemaName, version, metadataConnectionString);
    if (tables == null) return 1;

    Console.WriteLine($"Parsed XSD: {tables.Count} tables, {structure!.Slots.Count} message slots.");

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
        var fileSha = MetadataRepository.ComputeFileSha256(xmlFile);
        var xml = XDocument.Load(xmlFile);
        var result = ProcessSingleXml(
            xml,
            sourceName: Path.GetFileName(xmlFile),
            sourceUri: Path.GetFullPath(xmlFile),
            sourceSha256: fileSha,
            externalId: null,
            processor, loader, meta!, schemaSetId!.Value);
        results.Add(result);

        if (shouldDeleteSourceFiles && result.Success)
        {
            File.Delete(xmlFile);
            Console.WriteLine($"  Deleted source file: {Path.GetFileName(xmlFile)}");
        }
    }

    var verboseOutput = string.Equals(configuration["Processing:VerboseOutput"], "true", StringComparison.OrdinalIgnoreCase);
    PrintResults(results, verboseOutput);
    return results.All(r => r.Success) ? 0 : 1;
}

// ═══════════════════════════════════════════════════════════════════
// PROCESS-SQL — load XML data from a SQL table using XSD from metadata DB
// ═══════════════════════════════════════════════════════════════════
if (command == "process-sql")
{
    string? schemaName = null;
    string? version = null;
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

    if (schemaName == null || version == null || connectionString == null || metadataConnectionString == null)
    {
        Console.Error.WriteLine("Error: --schema-name, --version, --connection-string, and --metadata-connection-string are required.");
        PrintUsage();
        return 1;
    }

    var (tables, structure, meta, schemaSetId) = LoadSchemaSet(schemaName, version, metadataConnectionString);
    if (tables == null) return 1;

    // Load source config from metadata
    var sourceConfigJsonRaw = meta!.GetSourceConfig(schemaSetId!.Value);
    if (string.IsNullOrWhiteSpace(sourceConfigJsonRaw))
    {
        Console.Error.WriteLine("Error: No source config registered for this schema set. Use `register --source-config` to provide one.");
        return 1;
    }

    var sourceConfig = System.Text.Json.JsonSerializer.Deserialize<SourceConfig>(
        sourceConfigJsonRaw,
        new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });

    if (sourceConfig == null || string.IsNullOrWhiteSpace(sourceConfig.SourceConnectionString)
        || string.IsNullOrWhiteSpace(sourceConfig.SourceQuery))
    {
        Console.Error.WriteLine("Error: source config must contain 'sourceConnectionString' and 'sourceQuery'.");
        return 1;
    }

    Console.WriteLine($"Parsed XSD: {tables.Count} tables, {structure!.Slots.Count} message slots.");

    // Read source rows from the external SQL table
    var sourceRows = new List<(string id, string xmlContent)>();
    using (var srcConn = new SqlConnection(sourceConfig.SourceConnectionString))
    {
        srcConn.Open();
        using var srcCmd = new SqlCommand(sourceConfig.SourceQuery, srcConn);
        srcCmd.CommandTimeout = 120;
        using var reader = srcCmd.ExecuteReader();
        while (reader.Read())
        {
            var idValue = reader[sourceConfig.SourceIdColumn]?.ToString() ?? "";
            var xmlValue = reader[sourceConfig.SourceXmlColumn]?.ToString() ?? "";
            sourceRows.Add((idValue, xmlValue));
        }
    }

    if (sourceRows.Count == 0)
    {
        Console.WriteLine("No rows returned by source query.");
        return 0;
    }

    Console.WriteLine($"Found {sourceRows.Count} row(s) to process from source query.");

    var results = new List<ProcessingResult>();
    var processor = new XmlProcessor(tables, structure);

    using var loader = new SqlServerLoader(connectionString, tables);

    foreach (var (id, xmlContent) in sourceRows)
    {
        var xml = XDocument.Parse(xmlContent);
        long.TryParse(id, out var externalIdValue);
        var result = ProcessSingleXml(
            xml,
            sourceName: id,
            sourceUri: $"sql://{sourceConfig.SourceIdColumn}/{id}",
            sourceSha256: null,
            externalId: externalIdValue != 0 ? externalIdValue : null,
            processor, loader, meta, schemaSetId.Value);
        results.Add(result);
    }

    var verboseOutput = string.Equals(configuration["Processing:VerboseOutput"], "true", StringComparison.OrdinalIgnoreCase);
    PrintResults(results, verboseOutput);

    // If all rows succeeded and a source update query was provided, mark source rows as processed
    if (results.All(r => r.Success) && sourceConfig.SourceUpdateQuery != null)
    {
        try
        {
            int updatedCount = 0;
            using var updateConn = new SqlConnection(sourceConfig.SourceConnectionString);
            updateConn.Open();
            foreach (var (id, _) in sourceRows)
            {
                using var updateCmd = new SqlCommand(sourceConfig.SourceUpdateQuery, updateConn);
                updateCmd.Parameters.AddWithValue("@Id", id);
                updatedCount += updateCmd.ExecuteNonQuery();
            }
            Console.WriteLine($"Source update: {updatedCount} row(s) updated.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: source update query failed: {ex.Message}");
        }
    }

    return results.All(r => r.Success) ? 0 : 1;
}

// ═══════════════════════════════════════════════════════════════════
// INFER-XSD — infer XSD schema from sample XML files
// ═══════════════════════════════════════════════════════════════════
if (command == "infer-xsd")
{
    string? inputFolder = null;
    string? outputPath = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--input" when i + 1 < args.Length:
                inputFolder = args[++i];
                break;
            case "--output" when i + 1 < args.Length:
                outputPath = args[++i];
                break;
        }
    }

    if (inputFolder == null || outputPath == null)
    {
        Console.Error.WriteLine("Error: --input and --output are required.");
        PrintUsage();
        return 1;
    }

    if (!Directory.Exists(inputFolder))
    {
        Console.Error.WriteLine($"Error: Input folder not found: {inputFolder}");
        return 1;
    }

    var xmlFiles = Directory.GetFiles(inputFolder, "*.xml");
    if (xmlFiles.Length == 0)
    {
        Console.Error.WriteLine("Error: No XML files found in input folder.");
        return 1;
    }

    var inferrer = new XsdInferrer();
    int merged = 0;
    int failed = 0;

    foreach (var xmlFile in xmlFiles)
    {
        try
        {
            var doc = XDocument.Load(xmlFile);
            inferrer.MergeDocument(doc);
            merged++;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: skipping {Path.GetFileName(xmlFile)}: {ex.Message}");
            failed++;
        }
    }

    if (merged == 0)
    {
        Console.Error.WriteLine("Error: No XML files could be parsed.");
        return 1;
    }

    var xsd = inferrer.GenerateXsd();
    xsd.Save(outputPath);

    Console.WriteLine($"Inferred XSD written to {outputPath}");
    Console.WriteLine($"  {merged} file(s) merged, {failed} skipped.");
    return 0;
}

Console.Error.WriteLine($"Unknown command: {command}");
PrintUsage();
return 1;

// ═══════════════════════════════════════════════════════════════════
// Shared helpers
// ═══════════════════════════════════════════════════════════════════

static (List<TableDefinition>? tables, MessageStructure? structure, MetadataRepository? meta, long? schemaSetId) LoadSchemaSet(
    string schemaName, string version, string metadataConnectionString)
{
    var meta = new MetadataRepository(metadataConnectionString);
    meta.EnsureMetadataTables();

    var schemaSetId = meta.FindActiveSchemaSet(schemaName, version);
    if (schemaSetId == null)
    {
        Console.Error.WriteLine($"Error: schema set '{schemaName}' version '{version}' not found.");
        return (null, null, null, null);
    }

    var schemaFiles = meta.GetSchemaFiles(schemaSetId.Value);
    if (schemaFiles.Count == 0)
    {
        Console.Error.WriteLine("Error: no XSD files found for this schema set.");
        return (null, null, null, null);
    }

    var rootFile = schemaFiles.FirstOrDefault(f => f.FileRole == "Root");
    if (rootFile == null)
    {
        Console.Error.WriteLine("Error: no root XSD file found in schema set.");
        return (null, null, null, null);
    }

    var xsdContentByFileName = schemaFiles.ToDictionary(f => f.FileName, f => f.ContentXml);
    var (docs, prefixMaps) = XsdLoader.LoadFromContent(xsdContentByFileName, rootFile.FileName);

    var parser = new XsdParser();
    var (tables, structure) = parser.Parse(docs, prefixMaps);

    // Apply table prefix if one was saved during generateddl
    var savedPrefix = meta.GetTablePrefix(schemaSetId.Value);
    if (!string.IsNullOrWhiteSpace(savedPrefix))
        TablePrefixHelper.ApplyPrefix(tables, structure, savedPrefix);

    return (tables, structure, meta, schemaSetId.Value);
}

static ProcessingResult ProcessSingleXml(
    XDocument xml, string sourceName, string sourceUri, string? sourceSha256,
    long? externalId,
    XmlProcessor processor, SqlServerLoader loader,
    MetadataRepository meta, long schemaSetId)
{
    var result = new ProcessingResult { FileName = sourceName };
    long loadRunId = 0;

    try
    {
        loadRunId = meta.InsertXmlLoadRun(
            schemaSetId,
            sourceFileName: sourceName,
            sourceUri: sourceUri,
            sourceFileSha256: sourceSha256);

        var rowTree = processor.ProcessFile(xml);

        // Set ExternalId on the root row if provided (process-sql)
        if (externalId.HasValue)
        {
            rowTree.Values["ExternalId"] = externalId.Value.ToString();
        }

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

    return result;
}

static void PrintResults(List<ProcessingResult> results, bool verbose)
{
    Console.WriteLine();
    Console.WriteLine("Processing Results:");
    Console.WriteLine(new string('-', 60));

    if (verbose)
    {
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

        Console.WriteLine();
        Console.WriteLine(new string('-', 60));
    }

    var succeeded = results.Count(r => r.Success);
    var failed = results.Count(r => !r.Success);
    Console.WriteLine($"  {results.Count} file(s) processed: {succeeded} succeeded, {failed} failed.");

    // Aggregate row counts across all files
    var aggregated = new Dictionary<string, long>();
    foreach (var result in results.Where(r => r.Success))
    {
        foreach (var kv in result.RowCounts)
        {
            if (aggregated.ContainsKey(kv.Key))
                aggregated[kv.Key] += kv.Value;
            else
                aggregated[kv.Key] = kv.Value;
        }
    }

    if (aggregated.Count > 0)
    {
        Console.WriteLine();
        Console.WriteLine("  Row totals:");
        long grandTotal = 0;
        foreach (var kv in aggregated.OrderBy(kv => kv.Key))
        {
            Console.WriteLine($"    {kv.Key}: {kv.Value} row(s)");
            grandTotal += kv.Value;
        }
        Console.WriteLine($"  Total: {grandTotal} row(s)");
    }

    // List failed files
    var failures = results.Where(r => !r.Success).ToList();
    if (failures.Count > 0)
    {
        Console.WriteLine();
        Console.WriteLine("  Failed files:");
        foreach (var f in failures)
        {
            Console.WriteLine($"    {f.FileName}: {f.ErrorMessage}");
        }
    }
}

static void PrintUsage()
{
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML register --xsd <path> [--schema-name <name>] [--version <label>]");
    Console.Error.WriteLine("                  [--source-config <json-file>]");
    Console.Error.WriteLine("                  [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML generateddl --schema-name <name> --version <ver> [--output <sql-file>]");
    Console.Error.WriteLine("                [--table-prefix <prefix>] [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML process-file --schema-name <name> --version <ver> --input <xml-folder>");
    Console.Error.WriteLine("                 --connection-string <conn-str> [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine("                 [--delete-source-files <Y|N>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML process-sql --schema-name <name> --version <ver>");
    Console.Error.WriteLine("                 --connection-string <conn-str> [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  SQLXML infer-xsd --input <xml-folder> --output <xsd-file>");
    Console.Error.WriteLine();
    Console.Error.WriteLine("Options:");
    Console.Error.WriteLine("  --connection-string           Target database for business tables / data loading");
    Console.Error.WriteLine("  --metadata-connection-string  Database for SQLXML_* metadata tables (can be a different server)");
    Console.Error.WriteLine("  --source-config               JSON file with source table settings (stored in metadata at register time)");
    Console.Error.WriteLine("  --table-prefix                Prefix for all generated table names (e.g., 'marketing' → 'marketing_Orders')");
    Console.Error.WriteLine("  --delete-source-files         Delete source XML files after successful processing (Y or N, default N)");
}

static string NormalizeSchemaName(string name)
{
    if (name.EndsWith(".xsd", StringComparison.OrdinalIgnoreCase))
        return name[..^4];
    return name;
}
