using System.Globalization;
using System.Xml.Linq;

namespace SQLXML.Inference;

/// <summary>
/// Infers an XSD schema from one or more XML documents by merging their structures.
/// </summary>
public class XsdInferrer
{
    private static readonly XNamespace Xs = "http://www.w3.org/2001/XMLSchema";

    private int _totalDocuments;
    private InferredNode? _root;
    private string? _rootName;

    public void MergeDocument(XDocument doc)
    {
        var rootElement = doc.Root;
        if (rootElement == null)
            throw new InvalidOperationException("XML document has no root element.");

        var localName = rootElement.Name.LocalName;

        if (_rootName == null)
        {
            _rootName = localName;
            _root = new InferredNode(localName);
        }
        else if (_rootName != localName)
        {
            throw new InvalidOperationException(
                $"Root element mismatch: expected '{_rootName}', got '{localName}'.");
        }

        _totalDocuments++;

        var visited = new HashSet<InferredNode>();
        MergeElement(rootElement, _root!, visited);
    }

    public XDocument GenerateXsd()
    {
        if (_root == null)
            throw new InvalidOperationException("No documents have been merged.");

        var schema = new XElement(Xs + "schema",
            new XAttribute(XNamespace.Xmlns + "xs", Xs.NamespaceName));

        var rootXsElement = new XElement(Xs + "element",
            new XAttribute("name", _root.Name));

        BuildComplexType(rootXsElement, _root);
        schema.Add(rootXsElement);

        return new XDocument(new XDeclaration("1.0", "utf-8", null), schema);
    }

    private void MergeElement(XElement xmlElement, InferredNode node, HashSet<InferredNode> visited)
    {
        bool firstVisit = visited.Add(node);
        if (firstVisit)
            node.FilesWherePresent++;

        // Merge attributes
        foreach (var attr in xmlElement.Attributes())
        {
            // Skip namespace declarations
            if (attr.IsNamespaceDeclaration) continue;

            var attrName = attr.Name.LocalName;
            if (!node.Attributes.TryGetValue(attrName, out var existing))
            {
                node.Attributes[attrName] = new InferredAttribute
                {
                    Type = InferType(attr.Value),
                    FilesWherePresent = 1
                };
            }
            else
            {
                existing.Type = WidenType(existing.Type, InferType(attr.Value));
                if (firstVisit) existing.FilesWherePresent++;
            }
        }

        // Check for text content vs child elements
        var childElements = xmlElement.Elements().ToList();
        var textContent = xmlElement.Nodes()
            .OfType<XText>()
            .Select(t => t.Value.Trim())
            .Where(t => t.Length > 0)
            .FirstOrDefault();

        if (childElements.Count > 0)
        {
            node.HasChildElements = true;

            // Group children by local name, count occurrences
            var groups = childElements
                .GroupBy(e => e.Name.LocalName)
                .ToList();

            // Track child order for deterministic output
            foreach (var group in groups)
            {
                var childName = group.Key;
                var count = group.Count();

                if (!node.Children.TryGetValue(childName, out var childNode))
                {
                    childNode = new InferredNode(childName);
                    node.Children[childName] = childNode;
                }

                if (count > childNode.MaxOccurrencesUnderParent)
                    childNode.MaxOccurrencesUnderParent = count;

                foreach (var childElement in group)
                {
                    MergeElement(childElement, childNode, visited);
                }
            }
        }

        if (textContent != null)
        {
            node.HasTextContent = true;
            node.ValueType = WidenType(node.ValueType, InferType(textContent));
        }
    }

    private void BuildComplexType(XElement parentXsElement, InferredNode node)
    {
        if (!node.HasChildElements && node.Attributes.Count == 0)
        {
            // Simple leaf element — just set the type
            parentXsElement.SetAttributeValue("type", MapToXsdType(node.ValueType));
            return;
        }

        var complexType = new XElement(Xs + "complexType");

        if (node.HasChildElements)
        {
            var sequence = new XElement(Xs + "sequence");

            foreach (var (childName, childNode) in node.Children)
            {
                var childXsElement = new XElement(Xs + "element",
                    new XAttribute("name", childName));

                // Always mark as optional — sample files may not cover all variations
                childXsElement.SetAttributeValue("minOccurs", "0");

                // maxOccurs
                if (childNode.MaxOccurrencesUnderParent > 1)
                    childXsElement.SetAttributeValue("maxOccurs", "unbounded");

                BuildComplexType(childXsElement, childNode);
                sequence.Add(childXsElement);
            }

            complexType.Add(sequence);
        }

        // If the node has both text content and children/attributes, use simpleContent or mixed
        // For simplicity, if it has text content too, mark as mixed
        if (node.HasTextContent && node.HasChildElements)
            complexType.SetAttributeValue("mixed", "true");

        // Attributes
        foreach (var (attrName, attrInfo) in node.Attributes)
        {
            var xsAttr = new XElement(Xs + "attribute",
                new XAttribute("name", attrName),
                new XAttribute("type", MapToXsdType(attrInfo.Type)));

            // Always mark as optional — sample files may not cover all variations
            xsAttr.SetAttributeValue("use", "optional");

            complexType.Add(xsAttr);
        }

        // If simple element with attributes but no children, use simpleContent extension
        if (!node.HasChildElements && node.Attributes.Count > 0)
        {
            var simpleContent = new XElement(Xs + "simpleContent",
                new XElement(Xs + "extension",
                    new XAttribute("base", MapToXsdType(node.ValueType)),
                    complexType.Elements(Xs + "attribute")));
            complexType.RemoveAll();
            complexType.Add(simpleContent);
        }

        parentXsElement.Add(complexType);
    }

    private static InferredType InferType(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return InferredType.None;

        // Boolean: only literal true/false
        if (value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
            value.Equals("false", StringComparison.OrdinalIgnoreCase))
            return InferredType.Boolean;

        // Integer
        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            return InferredType.Integer;

        // Decimal
        if (decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out _))
            return InferredType.Decimal;

        // Date (yyyy-MM-dd)
        if (value.Length == 10 &&
            DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out _))
            return InferredType.Date;

        // DateTime (ISO 8601 variants)
        if (value.Length > 10 &&
            DateTime.TryParse(value, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out _))
            return InferredType.DateTime;

        return InferredType.String;
    }

    private static InferredType WidenType(InferredType existing, InferredType incoming)
    {
        if (existing == incoming) return existing;
        if (existing == InferredType.None) return incoming;
        if (incoming == InferredType.None) return existing;

        // Numeric widening: Boolean → Integer → Decimal → String
        if (IsNumericPath(existing) && IsNumericPath(incoming))
        {
            // Return the wider of the two
            return (InferredType)Math.Max((int)existing, (int)incoming);
        }

        // Temporal widening: Date → DateTime → String
        if (IsTemporal(existing) && IsTemporal(incoming))
        {
            return (InferredType)Math.Max((int)existing, (int)incoming);
        }

        // Incompatible paths → String
        return InferredType.String;
    }

    private static bool IsNumericPath(InferredType t) =>
        t == InferredType.Boolean || t == InferredType.Integer || t == InferredType.Decimal;

    private static bool IsTemporal(InferredType t) =>
        t == InferredType.Date || t == InferredType.DateTime;

    private static string MapToXsdType(InferredType t) => t switch
    {
        InferredType.Integer => "xs:integer",
        InferredType.Decimal => "xs:decimal",
        // Boolean, Date, DateTime all map to xs:string for HL7 safety
        _ => "xs:string"
    };
}

public enum InferredType
{
    None = 0,
    Boolean = 1,
    Integer = 2,
    Decimal = 3,
    Date = 4,
    DateTime = 5,
    String = 6
}

public class InferredNode
{
    public string Name { get; }
    public Dictionary<string, InferredNode> Children { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, InferredAttribute> Attributes { get; } = new(StringComparer.Ordinal);
    public InferredType ValueType { get; set; } = InferredType.None;
    public int MaxOccurrencesUnderParent { get; set; } = 1;
    public int FilesWherePresent { get; set; }
    public bool HasTextContent { get; set; }
    public bool HasChildElements { get; set; }

    public InferredNode(string name) => Name = name;
}

public class InferredAttribute
{
    public InferredType Type { get; set; } = InferredType.None;
    public int FilesWherePresent { get; set; }
}
