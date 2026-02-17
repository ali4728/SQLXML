using System.Xml.Linq;
using SQLXML.Models;

namespace SQLXML.Parsing;

/// <summary>
/// Static helper for loading XSD chains from disk or from in-memory content (e.g. stored in DB).
/// </summary>
public static class XsdLoader
{
    private static readonly XNamespace Xs = "http://www.w3.org/2001/XMLSchema";

    /// <summary>
    /// Load XSD chain from disk starting at <paramref name="rootXsdPath"/>.
    /// Returns a list of <see cref="LoadedXsdFileInfo"/> with ContentXml populated,
    /// plus the parsed documents and prefix maps ready for <see cref="XsdParser"/>.
    /// SchemaLocation attributes are normalized to bare filenames in the returned ContentXml.
    /// </summary>
    public static (List<LoadedXsdFileInfo> Files,
                    List<(string Namespace, XDocument Doc)> Docs,
                    Dictionary<XDocument, Dictionary<string, string>> PrefixMaps)
        LoadFromDisk(string rootXsdPath)
    {
        var files = new List<LoadedXsdFileInfo>();
        var docs = new List<(string Namespace, XDocument Doc)>();
        var prefixMaps = new Dictionary<XDocument, Dictionary<string, string>>();

        var loaded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>();
        var rootFullPath = Path.GetFullPath(rootXsdPath);
        queue.Enqueue(rootFullPath);
        bool isFirst = true;

        while (queue.Count > 0)
        {
            var path = queue.Dequeue();
            if (!loaded.Add(path)) continue;

            var doc = XDocument.Load(path);
            var targetNs = doc.Root?.Attribute("targetNamespace")?.Value ?? "";
            docs.Add((targetNs, doc));

            // Normalize schemaLocation to bare filenames in a copy for storage
            var contentXml = NormalizeSchemaLocations(File.ReadAllText(path));

            files.Add(new LoadedXsdFileInfo
            {
                FilePath = path,
                FileName = Path.GetFileName(path),
                TargetNamespace = targetNs,
                FileRole = isFirst ? "Root" : "Import",
                ContentXml = contentXml
            });
            isFirst = false;

            // Build prefix map for this document
            var prefixMap = BuildPrefixMap(doc);
            prefixMaps[doc] = prefixMap;

            var dir = Path.GetDirectoryName(path)!;

            // Follow xs:import
            foreach (var import in doc.Descendants(Xs + "import"))
            {
                var loc = import.Attribute("schemaLocation")?.Value;
                if (loc != null)
                {
                    queue.Enqueue(Path.GetFullPath(Path.Combine(dir, loc)));
                }
            }

            // Follow xs:include (same namespace, different file)
            foreach (var include in doc.Descendants(Xs + "include"))
            {
                var loc = include.Attribute("schemaLocation")?.Value;
                if (loc != null)
                {
                    queue.Enqueue(Path.GetFullPath(Path.Combine(dir, loc)));
                }
            }
        }

        return (files, docs, prefixMaps);
    }

    /// <summary>
    /// Load XSD chain from in-memory content (e.g. retrieved from DB).
    /// The <paramref name="xsdContentByFileName"/> dictionary maps bare file names to XML text.
    /// <paramref name="rootFileName"/> identifies which file is the root XSD.
    /// Returns parsed documents and prefix maps ready for <see cref="XsdParser"/>.
    /// </summary>
    public static (List<(string Namespace, XDocument Doc)> Docs,
                    Dictionary<XDocument, Dictionary<string, string>> PrefixMaps)
        LoadFromContent(Dictionary<string, string> xsdContentByFileName, string rootFileName)
    {
        var docs = new List<(string Namespace, XDocument Doc)>();
        var prefixMaps = new Dictionary<XDocument, Dictionary<string, string>>();

        var loaded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>();
        queue.Enqueue(rootFileName);

        while (queue.Count > 0)
        {
            var fileName = queue.Dequeue();
            if (!loaded.Add(fileName)) continue;

            if (!xsdContentByFileName.TryGetValue(fileName, out var content))
            {
                // Try case-insensitive match
                var match = xsdContentByFileName.Keys
                    .FirstOrDefault(k => k.Equals(fileName, StringComparison.OrdinalIgnoreCase));
                if (match == null)
                    throw new InvalidOperationException(
                        $"XSD file '{fileName}' referenced via xs:import or xs:include but not found in schema set.");
                content = xsdContentByFileName[match];
            }

            var doc = XDocument.Parse(content);
            var targetNs = doc.Root?.Attribute("targetNamespace")?.Value ?? "";
            docs.Add((targetNs, doc));

            // Build prefix map
            var prefixMap = BuildPrefixMap(doc);
            prefixMaps[doc] = prefixMap;

            // Follow xs:import — schemaLocation should already be bare filenames
            foreach (var import in doc.Descendants(Xs + "import"))
            {
                var loc = import.Attribute("schemaLocation")?.Value;
                if (loc != null)
                {
                    // Normalize to just the filename (strip any remaining path components)
                    var importFileName = Path.GetFileName(loc);
                    queue.Enqueue(importFileName);
                }
            }

            // Follow xs:include — schemaLocation should already be bare filenames
            foreach (var include in doc.Descendants(Xs + "include"))
            {
                var loc = include.Attribute("schemaLocation")?.Value;
                if (loc != null)
                {
                    var includeFileName = Path.GetFileName(loc);
                    queue.Enqueue(includeFileName);
                }
            }
        }

        return (docs, prefixMaps);
    }

    /// <summary>
    /// Normalize schemaLocation attributes in XSD XML text to bare filenames
    /// (strip directory path components like "../common/").
    /// </summary>
    private static string NormalizeSchemaLocations(string xsdXml)
    {
        var doc = XDocument.Parse(xsdXml);
        bool modified = false;

        foreach (var import in doc.Descendants(Xs + "import"))
        {
            var loc = import.Attribute("schemaLocation");
            if (loc != null)
            {
                var normalized = Path.GetFileName(loc.Value);
                if (normalized != loc.Value)
                {
                    loc.Value = normalized;
                    modified = true;
                }
            }
        }

        foreach (var include in doc.Descendants(Xs + "include"))
        {
            var loc = include.Attribute("schemaLocation");
            if (loc != null)
            {
                var normalized = Path.GetFileName(loc.Value);
                if (normalized != loc.Value)
                {
                    loc.Value = normalized;
                    modified = true;
                }
            }
        }

        var result = modified
            ? doc.Declaration?.ToString() + Environment.NewLine + doc.ToString()
            : xsdXml;
        return StripEncodingDeclaration(result);
    }

    /// <summary>
    /// Remove encoding="..." from XML declaration for SQL Server XML column compatibility.
    /// SQL Server's XML type stores data as UTF-16 internally and rejects encoding="UTF-8".
    /// </summary>
    private static string StripEncodingDeclaration(string xml)
    {
        return System.Text.RegularExpressions.Regex.Replace(
            xml,
            @"(<\?xml\s[^?]*?)\s+encoding\s*=\s*""[^""]*""",
            "$1",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private static Dictionary<string, string> BuildPrefixMap(XDocument doc)
    {
        var prefixMap = new Dictionary<string, string>();
        if (doc.Root != null)
        {
            foreach (var attr in doc.Root.Attributes())
            {
                if (attr.IsNamespaceDeclaration)
                {
                    var prefix = attr.Name.LocalName == "xmlns" ? "" : attr.Name.LocalName;
                    prefixMap[prefix] = attr.Value;
                }
            }
        }
        return prefixMap;
    }
}
