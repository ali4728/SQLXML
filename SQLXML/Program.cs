using SQLXML.Generation;
using SQLXML.Parsing;

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
    var tables = parser.Parse(xsdPath);
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

Console.Error.WriteLine($"Unknown command: {command}");
PrintUsage();
return 1;

static void PrintUsage()
{
    Console.Error.WriteLine("Usage: SQLXML schema --xsd <root-xsd-path> [--output <sql-file-path>]");
}
