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

if (command == "schema")
{
    string? xsdPath = null;
    string? outputPath = null;
    string? metadataConnectionString = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--xsd" when i + 1 < args.Length:
                xsdPath = args[++i];
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

    if (xsdPath == null)
    {
        Console.Error.WriteLine("Error: --xsd <path> is required.");
        PrintUsage();
        return 1;
    }

    if (!File.Exists(xsdPath))
    {
        Console.Error.WriteLine($"Error: XSD file not found: {xsdPath}");
        return 1;
    }

    var parser = new XsdParser();
    var (tables, _, loadedFiles) = parser.Parse(xsdPath);
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

    // ── Record metadata if metadata connection string provided ──
    if (metadataConnectionString != null)
    {
        try
        {
            using var meta = new MetadataRepository(metadataConnectionString);
            meta.EnsureMetadataTables();

            var rootFile = loadedFiles.FirstOrDefault(f => f.FileRole == "Root");
            var rootFileName = rootFile?.FileName ?? Path.GetFileName(xsdPath);
            var schemaSetKey = Path.GetFileNameWithoutExtension(rootFileName);
            var combinedSha = MetadataRepository.ComputeCombinedSha256(
                loadedFiles.Select(f => f.FilePath));
            var versionLabel = combinedSha[..12]; // short hash as version

            // Re-use existing schema set if same content
            var existingId = meta.FindActiveSchemaSet(schemaSetKey, versionLabel);
            var schemaSetId = existingId ?? meta.InsertSchemaSet(
                schemaSetKey: schemaSetKey,
                versionLabel: versionLabel,
                rootXsdFileName: rootFileName,
                rootTargetNamespace: rootFile?.TargetNamespace,
                displayName: rootFileName,
                combinedSha256: combinedSha,
                createdBy: Environment.UserName);

            // Register individual XSD files (skip if set already existed)
            if (existingId == null)
            {
                foreach (var file in loadedFiles)
                {
                    var sha = MetadataRepository.ComputeFileSha256(file.FilePath);
                    meta.InsertSchemaFile(
                        schemaSetId, file.FileRole, file.FileName,
                        file.FilePath, file.TargetNamespace, sha,
                        importedBy: Environment.UserName);
                }
            }

            // Record generation run
            var runId = meta.InsertGenerationRun(
                schemaSetId,
                toolName: "SQLXML",
                toolVersion: "1.0.0");

            try
            {
                // Record each generated CREATE TABLE as a separate script
                int order = 0;
                foreach (var table in tables.OrderBy(t => t.SortOrder))
                {
                    var tableScript = SqlGenerator.Generate(new List<TableDefinition> { table });
                    meta.InsertGeneratedScript(
                        schemaSetId, runId,
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
        }
        catch (Exception ex)
        {
            // Metadata failure should not prevent schema output
            Console.Error.WriteLine($"Warning: could not record metadata: {ex.Message}");
        }
    }

    return 0;
}

if (command == "process")
{
    string? xsdPath = null;
    string? inputFolder = null;
    string? connectionString = null;
    string? metadataConnectionString = null;

    for (int i = 1; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--xsd" when i + 1 < args.Length:
                xsdPath = args[++i];
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

    if (xsdPath == null || inputFolder == null || connectionString == null)
    {
        Console.Error.WriteLine("Error: --xsd, --input, and --connection-string are all required.");
        PrintUsage();
        return 1;
    }

    if (!File.Exists(xsdPath))
    {
        Console.Error.WriteLine($"Error: XSD file not found: {xsdPath}");
        return 1;
    }

    if (!Directory.Exists(inputFolder))
    {
        Console.Error.WriteLine($"Error: Input folder not found: {inputFolder}");
        return 1;
    }

    // Parse XSD to get table definitions and message structure
    var parser = new XsdParser();
    var (tables, structure, loadedFiles) = parser.Parse(xsdPath);

    Console.WriteLine($"Parsed XSD: {tables.Count} tables, {structure.Slots.Count} message slots.");

    // ── Set up metadata tracking (separate DB) ──
    MetadataRepository? meta = null;
    long schemaSetId = 0;
    if (metadataConnectionString != null)
    {
        try
        {
            meta = new MetadataRepository(metadataConnectionString);
            meta.EnsureMetadataTables();

            var rootFile = loadedFiles.FirstOrDefault(f => f.FileRole == "Root");
            var rootFileName = rootFile?.FileName ?? Path.GetFileName(xsdPath);
            var schemaSetKey = Path.GetFileNameWithoutExtension(rootFileName);
            var combinedSha = MetadataRepository.ComputeCombinedSha256(
                loadedFiles.Select(f => f.FilePath));
            var versionLabel = combinedSha[..12];

            var existingId = meta.FindActiveSchemaSet(schemaSetKey, versionLabel);
            schemaSetId = existingId ?? meta.InsertSchemaSet(
                schemaSetKey: schemaSetKey,
                versionLabel: versionLabel,
                rootXsdFileName: rootFileName,
                rootTargetNamespace: rootFile?.TargetNamespace,
                displayName: rootFileName,
                combinedSha256: combinedSha,
                createdBy: Environment.UserName);

            if (existingId == null)
            {
                foreach (var file in loadedFiles)
                {
                    var sha = MetadataRepository.ComputeFileSha256(file.FilePath);
                    meta.InsertSchemaFile(
                        schemaSetId, file.FileRole, file.FileName,
                        file.FilePath, file.TargetNamespace, sha,
                        importedBy: Environment.UserName);
                }
            }

            Console.WriteLine($"Metadata: SchemaSetId={schemaSetId}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: metadata init failed: {ex.Message}");
            meta?.Dispose();
            meta = null;
        }
    }

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
            // Start metadata load run
            if (meta != null)
            {
                var fileSha = MetadataRepository.ComputeFileSha256(xmlFile);
                loadRunId = meta.InsertXmlLoadRun(
                    schemaSetId,
                    sourceFileName: Path.GetFileName(xmlFile),
                    sourceUri: Path.GetFullPath(xmlFile),
                    sourceFileSha256: fileSha);
            }

            var xml = XDocument.Load(xmlFile);
            var rowTree = processor.ProcessFile(xml);

            loader.BeginTransaction();
            var rowCounts = loader.InsertRowTree(rowTree);
            loader.Commit();

            foreach (var kv in rowCounts)
                result.RowCounts[kv.Key] = kv.Value;

            result.Success = true;

            // Record table-level load metrics
            if (meta != null && loadRunId > 0)
            {
                long totalRows = 0;
                foreach (var kv in rowCounts)
                {
                    var logId = meta.InsertXmlLoadRunTableLog(loadRunId, "dbo", kv.Key);
                    meta.CompleteXmlLoadRunTableLog(logId, "Completed", rowsInserted: kv.Value);
                    totalRows += kv.Value;
                }
                meta.CompleteXmlLoadRun(loadRunId, "Completed", rowsInserted: totalRows);
            }
        }
        catch (Exception ex)
        {
            try { loader.Rollback(); } catch { /* ignore rollback errors */ }
            result.Success = false;
            result.ErrorMessage = ex.Message;

            if (meta != null && loadRunId > 0)
            {
                meta.CompleteXmlLoadRun(loadRunId, "Failed", message: ex.Message);
                meta.LogPipelineError("Load", ex.Message,
                    schemaSetId: schemaSetId, loadRunId: loadRunId);
            }
        }

        results.Add(result);
    }

    meta?.Dispose();

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
    Console.Error.WriteLine("  SQLXML schema --xsd <path> [--output <sql-file>] [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine("  SQLXML process --xsd <path> --input <xml-folder> --connection-string <conn-str>");
    Console.Error.WriteLine("                 [--metadata-connection-string <conn-str>]");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  --connection-string           Target database for business tables / data loading");
    Console.Error.WriteLine("  --metadata-connection-string  Database for SQLXML_* metadata tables (can be a different server)");
}
