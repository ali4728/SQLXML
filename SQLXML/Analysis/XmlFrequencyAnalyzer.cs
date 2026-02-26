using System.Text;
using System.Xml.Linq;

namespace SQLXML.Analysis;

public enum XPathNodeKind
{
    Element,
    Attribute
}

public record XPathFrequencyRecord(string XPath, XPathNodeKind Kind, int TotalCount, int DistinctFileCount);

public class XmlFrequencyAnalyzer
{
    private readonly Dictionary<string, (XPathNodeKind Kind, int TotalCount, int DistinctFileCount)> _frequencies = new();

    public void AnalyzeDocument(XDocument doc, string fileName)
    {
        if (doc.Root == null) return;

        var seenInFile = new HashSet<string>();
        TraverseElement(doc.Root, "", seenInFile);
    }

    private void TraverseElement(XElement element, string parentPath, HashSet<string> seenInFile)
    {
        var path = parentPath + "/" + element.Name.LocalName;
        RecordHit(path, XPathNodeKind.Element, seenInFile);

        foreach (var attr in element.Attributes())
        {
            if (attr.IsNamespaceDeclaration) continue;
            var attrPath = path + "/@" + attr.Name.LocalName;
            RecordHit(attrPath, XPathNodeKind.Attribute, seenInFile);
        }

        foreach (var child in element.Elements())
        {
            TraverseElement(child, path, seenInFile);
        }
    }

    private void RecordHit(string xpath, XPathNodeKind kind, HashSet<string> seenInFile)
    {
        if (_frequencies.TryGetValue(xpath, out var entry))
        {
            var newDistinct = seenInFile.Add(xpath) ? entry.DistinctFileCount + 1 : entry.DistinctFileCount;
            _frequencies[xpath] = (entry.Kind, entry.TotalCount + 1, newDistinct);
        }
        else
        {
            seenInFile.Add(xpath);
            _frequencies[xpath] = (kind, 1, 1);
        }
    }

    public List<XPathFrequencyRecord> GetResults()
    {
        return _frequencies
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new XPathFrequencyRecord(kv.Key, kv.Value.Kind, kv.Value.TotalCount, kv.Value.DistinctFileCount))
            .ToList();
    }

    public static void WriteCsv(List<XPathFrequencyRecord> records, string outputPath)
    {
        var sb = new StringBuilder();
        sb.AppendLine("XPath,Kind,TotalCount,DistinctFileCount");
        foreach (var r in records)
        {
            sb.AppendLine($"{EscapeCsv(r.XPath)},{r.Kind},{r.TotalCount},{r.DistinctFileCount}");
        }
        File.WriteAllText(outputPath, sb.ToString());
    }

    private static string EscapeCsv(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}
