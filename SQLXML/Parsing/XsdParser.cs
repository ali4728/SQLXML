using System.Xml.Linq;
using SQLXML.Generation;
using SQLXML.Models;

namespace SQLXML.Parsing;

public class XsdParser
{
    private static readonly XNamespace Xs = "http://www.w3.org/2001/XMLSchema";

    // All loaded XSD documents keyed by their target namespace
    private readonly Dictionary<string, XDocument> _docs = new();

    // Type dictionary: (namespace, localName) -> XElement for complexType/simpleType
    private readonly Dictionary<(string ns, string name), XElement> _types = new();

    // Namespace prefix -> namespace URI mappings per document
    private readonly Dictionary<XDocument, Dictionary<string, string>> _prefixMaps = new();

    private readonly List<TableDefinition> _tables = new();
    private int _sortOrder;

    public List<TableDefinition> Parse(string rootXsdPath)
    {
        LoadXsdChain(rootXsdPath);
        BuildTypeDictionary();

        var rootDoc = _docs.Values.First();
        var rootElement = rootDoc.Descendants(Xs + "element")
            .FirstOrDefault(e => e.Parent?.Name == Xs + "schema"
                && e.Attribute("name")?.Value == "ADT_A01_26_GLO_DEF");

        if (rootElement == null)
            throw new InvalidOperationException("Root element ADT_A01_26_GLO_DEF not found in XSD.");

        var messageName = "ADT_A01_26_GLO_DEF";
        var messageTable = CreateTable(messageName);

        var sequence = rootElement.Element(Xs + "complexType")?.Element(Xs + "sequence");
        if (sequence == null)
            throw new InvalidOperationException("Root element has no xs:sequence.");

        WalkMessageSequence(sequence, messageTable);

        return _tables;
    }

    private void LoadXsdChain(string rootPath)
    {
        var loaded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>();
        queue.Enqueue(Path.GetFullPath(rootPath));

        while (queue.Count > 0)
        {
            var path = queue.Dequeue();
            if (!loaded.Add(path)) continue;

            var doc = XDocument.Load(path);
            var targetNs = doc.Root?.Attribute("targetNamespace")?.Value ?? "";
            _docs[targetNs] = doc;

            // Build prefix map for this document
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
            _prefixMaps[doc] = prefixMap;

            // Follow xs:import
            var dir = Path.GetDirectoryName(path)!;
            foreach (var import in doc.Descendants(Xs + "import"))
            {
                var loc = import.Attribute("schemaLocation")?.Value;
                if (loc != null)
                {
                    queue.Enqueue(Path.GetFullPath(Path.Combine(dir, loc)));
                }
            }
        }
    }

    private void BuildTypeDictionary()
    {
        foreach (var (ns, doc) in _docs)
        {
            foreach (var ct in doc.Descendants(Xs + "complexType"))
            {
                var name = ct.Attribute("name")?.Value;
                if (name != null)
                    _types[(ns, name)] = ct;
            }
            foreach (var st in doc.Descendants(Xs + "simpleType"))
            {
                var name = st.Attribute("name")?.Value;
                if (name != null)
                    _types[(ns, name)] = st;
            }
        }
    }

    private void WalkMessageSequence(XElement sequence, TableDefinition messageTable)
    {
        // First pass: collect all segment names at root level (direct elements)
        // to detect duplicates and know which names are "taken"
        var rootSegNames = new Dictionary<string, int>();
        foreach (var child in sequence.Elements())
        {
            if (child.Name == Xs + "element")
            {
                var segName = GetSegmentShortName(child);
                rootSegNames.TryGetValue(segName, out int count);
                rootSegNames[segName] = count + 1;
            }
        }

        // Collect all names used across root + groups to detect collisions
        var allUsedNames = new HashSet<string>(rootSegNames.Keys);

        // Second pass: create tables
        var segNameIndex = new Dictionary<string, int>();

        foreach (var child in sequence.Elements())
        {
            if (child.Name == Xs + "element")
            {
                var segName = GetSegmentShortName(child);
                segNameIndex.TryGetValue(segName, out int idx);
                segNameIndex[segName] = idx + 1;

                var tableName = segName;
                if (rootSegNames[segName] > 1 && idx > 0)
                {
                    tableName = $"{segName}_{idx + 1}";
                }

                var isRepeating = IsRepeating(child);
                var segTable = CreateSegmentTable(tableName, messageTable.TableName, isRepeating);
                ProcessSegmentFields(child, segTable);
            }
            else if (child.Name == Xs + "sequence")
            {
                ProcessGroup(child, messageTable.TableName, allUsedNames);
            }
        }
    }

    private void ProcessGroup(XElement groupSequence, string messageTableName, HashSet<string> allUsedNames)
    {
        var elements = groupSequence.Elements(Xs + "element").ToList();
        if (elements.Count == 0) return;

        // Lead segment gets MessageId FK
        var leadElement = elements[0];
        var leadSegName = GetSegmentShortName(leadElement);
        var leadIsRepeating = true; // Groups themselves are unbounded

        var leadTable = CreateSegmentTable(leadSegName, messageTableName, leadIsRepeating);
        ProcessSegmentFields(leadElement, leadTable);

        // Child segments in the group get both MessageId and LeadSegmentId FKs
        for (int i = 1; i < elements.Count; i++)
        {
            var childElement = elements[i];
            var childSegName = GetSegmentShortName(childElement);
            var childIsRepeating = IsRepeating(childElement);

            // Only prefix with lead segment name if the name collides with a root-level name
            var childTableName = allUsedNames.Contains(childSegName)
                ? $"{leadSegName}_{childSegName}"
                : childSegName;

            var childTable = CreateGroupChildTable(childTableName, messageTableName, leadTable.TableName, childIsRepeating);
            ProcessSegmentFields(childElement, childTable);
        }
    }

    private void ProcessSegmentFields(XElement segElement, TableDefinition segTable)
    {
        var typeName = segElement.Attribute("type")?.Value;
        if (typeName == null) return;

        var resolvedType = ResolveTypeRef(typeName, segElement);
        if (resolvedType == null) return;

        var seq = resolvedType.Element(Xs + "sequence");
        if (seq == null) return;

        foreach (var field in seq.Elements(Xs + "element"))
        {
            var fieldName = field.Attribute("name")?.Value;
            if (fieldName == null) continue;

            var isFieldRepeating = IsRepeating(field);
            var fieldTypeAttr = field.Attribute("type")?.Value;

            if (isFieldRepeating)
            {
                // Create a child table for this repeating field
                var childTableName = fieldName;
                var childTable = CreateChildFieldTable(childTableName, segTable.TableName);

                if (fieldTypeAttr != null)
                {
                    var fieldType = ResolveTypeRef(fieldTypeAttr, field);
                    if (fieldType != null && IsComplexType(fieldType))
                    {
                        FlattenComplexType(fieldType, field, childTable, fieldName);
                    }
                    else
                    {
                        // Simple repeating field
                        var sqlType = fieldTypeAttr != null ? SqlGenerator.GetSqlType(StripPrefix(fieldTypeAttr)) : "NVARCHAR(MAX)";
                        childTable.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = fieldName,
                            SqlType = sqlType,
                            IsNullable = true
                        });
                    }
                }
            }
            else
            {
                // Non-repeating field
                if (fieldTypeAttr != null)
                {
                    var fieldType = ResolveTypeRef(fieldTypeAttr, field);
                    if (fieldType != null && IsComplexType(fieldType))
                    {
                        // Flatten complex type into parent table
                        FlattenComplexType(fieldType, field, segTable, fieldName);
                    }
                    else
                    {
                        // Simple type -> single column
                        var sqlType = SqlGenerator.GetSqlType(StripPrefix(fieldTypeAttr));
                        segTable.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = fieldName,
                            SqlType = sqlType,
                            IsNullable = true
                        });
                    }
                }
                else
                {
                    // Inline anonymous complex type
                    var inlineCt = field.Element(Xs + "complexType");
                    if (inlineCt != null)
                    {
                        FlattenComplexType(inlineCt, field, segTable, fieldName);
                    }
                    else
                    {
                        segTable.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = fieldName,
                            SqlType = "NVARCHAR(MAX)",
                            IsNullable = true
                        });
                    }
                }
            }
        }
    }

    private void FlattenComplexType(XElement complexType, XElement contextElement, TableDefinition table, string prefix)
    {
        var seq = complexType.Element(Xs + "sequence");
        if (seq == null) return;

        foreach (var el in seq.Elements(Xs + "element"))
        {
            var elName = el.Attribute("name")?.Value;
            if (elName == null) continue;

            var colName = $"{prefix}_{elName}";
            var elTypeAttr = el.Attribute("type")?.Value;

            if (elTypeAttr != null)
            {
                var elType = ResolveTypeRef(elTypeAttr, el);
                if (elType != null && IsComplexType(elType))
                {
                    // Recurse to flatten nested complex type
                    FlattenComplexType(elType, el, table, colName);
                }
                else
                {
                    var sqlType = SqlGenerator.GetSqlType(StripPrefix(elTypeAttr));
                    table.Columns.Add(new ColumnDefinition
                    {
                        ColumnName = colName,
                        SqlType = sqlType,
                        IsNullable = true
                    });
                }
            }
            else
            {
                // Inline anonymous complex type
                var inlineCt = el.Element(Xs + "complexType");
                if (inlineCt != null)
                {
                    FlattenComplexType(inlineCt, el, table, colName);
                }
                else
                {
                    table.Columns.Add(new ColumnDefinition
                    {
                        ColumnName = colName,
                        SqlType = "NVARCHAR(MAX)",
                        IsNullable = true
                    });
                }
            }
        }
    }

    private XElement? ResolveTypeRef(string typeRef, XElement context)
    {
        var localName = StripPrefix(typeRef);
        var ns = ResolveNamespace(typeRef, context);

        if (_types.TryGetValue((ns, localName), out var typeEl))
            return typeEl;

        // Try all namespaces as fallback
        foreach (var key in _types.Keys)
        {
            if (key.name == localName)
                return _types[key];
        }

        return null;
    }

    private string ResolveNamespace(string typeRef, XElement context)
    {
        if (!typeRef.Contains(':'))
        {
            // No prefix - use the targetNamespace of the document containing this element
            var doc = context.Document;
            if (doc != null)
                return doc.Root?.Attribute("targetNamespace")?.Value ?? "";
            return "";
        }

        var prefix = typeRef.Split(':')[0];
        var doc2 = context.Document;
        if (doc2 != null && _prefixMaps.TryGetValue(doc2, out var map))
        {
            if (map.TryGetValue(prefix, out var ns))
                return ns;
        }

        return "";
    }

    private static string StripPrefix(string typeRef)
    {
        var idx = typeRef.IndexOf(':');
        return idx >= 0 ? typeRef[(idx + 1)..] : typeRef;
    }

    private static bool IsComplexType(XElement typeElement)
    {
        return typeElement.Name.LocalName == "complexType";
    }

    private static bool IsRepeating(XElement element)
    {
        var maxOccurs = element.Attribute("maxOccurs")?.Value;
        return maxOccurs == "unbounded" || (int.TryParse(maxOccurs, out var m) && m > 1);
    }

    private static string GetSegmentShortName(XElement element)
    {
        var name = element.Attribute("name")?.Value ?? "Unknown";
        // Extract segment code: e.g. "EVN_EventType" -> "EVN", "PID_PatientIdentification" -> "PID"
        var underscoreIdx = name.IndexOf('_');
        return underscoreIdx > 0 ? name[..underscoreIdx] : name;
    }

    private TableDefinition CreateTable(string tableName)
    {
        var table = new TableDefinition
        {
            TableName = tableName,
            SortOrder = _sortOrder++
        };
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = "Id",
            SqlType = "BIGINT",
            IsIdentity = true,
            IsPrimaryKey = true,
            IsNullable = false
        });
        _tables.Add(table);
        return table;
    }

    private TableDefinition CreateSegmentTable(string tableName, string messageTableName, bool isRepeating)
    {
        var table = CreateTable(tableName);

        // FK to message table
        var fkCol = $"{messageTableName}Id";
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = fkCol,
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.ForeignKeys.Add(new ForeignKeyDefinition
        {
            ConstraintName = $"FK_{tableName}_{messageTableName}",
            ColumnName = fkCol,
            ReferencedTable = messageTableName,
            ReferencedColumn = "Id"
        });

        if (isRepeating)
        {
            table.Columns.Add(new ColumnDefinition
            {
                ColumnName = "RepeatIndex",
                SqlType = "INT",
                IsNullable = false
            });
        }

        return table;
    }

    private TableDefinition CreateGroupChildTable(string tableName, string messageTableName, string leadTableName, bool isRepeating)
    {
        var table = CreateTable(tableName);

        // FK to message table
        var msgFkCol = $"{messageTableName}Id";
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = msgFkCol,
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.ForeignKeys.Add(new ForeignKeyDefinition
        {
            ConstraintName = $"FK_{tableName}_{messageTableName}",
            ColumnName = msgFkCol,
            ReferencedTable = messageTableName,
            ReferencedColumn = "Id"
        });

        // FK to lead segment table
        var leadFkCol = $"{leadTableName}Id";
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = leadFkCol,
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.ForeignKeys.Add(new ForeignKeyDefinition
        {
            ConstraintName = $"FK_{tableName}_{leadTableName}",
            ColumnName = leadFkCol,
            ReferencedTable = leadTableName,
            ReferencedColumn = "Id"
        });

        if (isRepeating)
        {
            table.Columns.Add(new ColumnDefinition
            {
                ColumnName = "RepeatIndex",
                SqlType = "INT",
                IsNullable = false
            });
        }

        return table;
    }

    private TableDefinition CreateChildFieldTable(string tableName, string parentTableName)
    {
        var table = CreateTable(tableName);

        var fkCol = $"{parentTableName}Id";
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = fkCol,
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.ForeignKeys.Add(new ForeignKeyDefinition
        {
            ConstraintName = $"FK_{tableName}_{parentTableName}",
            ColumnName = fkCol,
            ReferencedTable = parentTableName,
            ReferencedColumn = "Id"
        });

        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = "RepeatIndex",
            SqlType = "INT",
            IsNullable = false
        });

        return table;
    }
}
