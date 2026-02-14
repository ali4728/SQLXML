using System.Xml.Linq;
using SQLXML.Generation;
using SQLXML.Models;
using SQLXML.Parsing;
using SQLXML.Processing;

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
        }
    }

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
    var (tables, _) = parser.Parse(xsdPath);
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

    return 0;
}

if (command == "process")
{
    string? xsdPath = null;
    string? inputFolder = null;
    string? connectionString = null;

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
        }
    }

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
    var (tables, structure) = parser.Parse(xsdPath);

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

        try
        {
            var xml = XDocument.Load(xmlFile);
            var rowTree = processor.ProcessFile(xml);

            loader.BeginTransaction();
            var rowCounts = loader.InsertRowTree(rowTree);
            loader.Commit();

            foreach (var kv in rowCounts)
                result.RowCounts[kv.Key] = kv.Value;

            result.Success = true;
        }
        catch (Exception ex)
        {
            try { loader.Rollback(); } catch { /* ignore rollback errors */ }
            result.Success = false;
            result.ErrorMessage = ex.Message;
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
    Console.Error.WriteLine("  SQLXML schema --xsd <root-xsd-path> [--output <sql-file-path>]");
    Console.Error.WriteLine("  SQLXML process --xsd <root-xsd-path> --input <xml-folder> --connection-string <conn-str>");
}
