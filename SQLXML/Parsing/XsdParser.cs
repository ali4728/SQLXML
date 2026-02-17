using System.Xml.Linq;
using SQLXML.Generation;
using SQLXML.Models;

namespace SQLXML.Parsing;

public class XsdParser
{
    private static readonly XNamespace Xs = "http://www.w3.org/2001/XMLSchema";

    private const int MaxColumnsPerTable = 300;

    // All loaded XSD documents as (namespace, document) tuples (supports multiple docs per namespace via xs:include)
    private readonly List<(string Namespace, XDocument Doc)> _docs = new();

    // Type dictionary: (namespace, localName) -> XElement for complexType/simpleType
    private readonly Dictionary<(string ns, string name), XElement> _types = new();

    // Namespace prefix -> namespace URI mappings per document
    private readonly Dictionary<XDocument, Dictionary<string, string>> _prefixMaps = new();

    private readonly List<TableDefinition> _tables = new();
    private readonly MessageStructure _messageStructure = new();
    private readonly HashSet<string> _createdTableNames = new(StringComparer.OrdinalIgnoreCase);
    private int _sortOrder;

    /// <summary>
    /// Convenience overload: loads XSD from disk, then parses.
    /// </summary>
    public (List<TableDefinition> Tables, MessageStructure Structure, List<LoadedXsdFileInfo> LoadedFiles) Parse(string rootXsdPath)
    {
        var (files, docs, prefixMaps) = XsdLoader.LoadFromDisk(rootXsdPath);
        var (tables, structure) = Parse(docs, prefixMaps);
        return (tables, structure, files);
    }

    /// <summary>
    /// Core parse entry point: accepts pre-loaded XSD documents (from DB or disk).
    /// </summary>
    public (List<TableDefinition> Tables, MessageStructure Structure) Parse(
        List<(string Namespace, XDocument Doc)> docs,
        Dictionary<XDocument, Dictionary<string, string>> prefixMaps)
    {
        foreach (var entry in docs)
            _docs.Add(entry);
        foreach (var (doc, map) in prefixMaps)
            _prefixMaps[doc] = map;

        BuildTypeDictionary();

        // Dynamic root element discovery: find the first top-level xs:element
        var rootDoc = _docs[0].Doc;
        var rootElement = rootDoc.Descendants(Xs + "element")
            .FirstOrDefault(e => e.Parent?.Name == Xs + "schema");

        if (rootElement == null)
            throw new InvalidOperationException("No top-level xs:element found in XSD.");

        var rootName = rootElement.Attribute("name")?.Value
            ?? throw new InvalidOperationException("Root element has no name attribute.");

        var rootTable = CreateTable(rootName);
        rootTable.XmlElementName = rootName;

        // Add ExternalId column for linking to external source rows (e.g. process-sql)
        rootTable.Columns.Add(new ColumnDefinition
        {
            ColumnName = "ExternalId",
            SqlType = "BIGINT",
            IsNullable = true,
            XmlPath = new List<string>()
        });

        // Get the complex type (inline or referenced) and process its children
        var complexType = GetComplexType(rootElement);
        if (complexType != null)
        {
            ProcessComplexTypeChildren(complexType, rootElement, rootTable, createTablesForSingletons: true);
        }

        // Handle column overflow on all tables
        HandleColumnOverflow();

        // Shorten column names that exceed SQL Server's 128-char identifier limit
        ShortenLongIdentifiers();

        return (_tables, _messageStructure);
    }

    private XElement? GetComplexType(XElement element)
    {
        // Check for inline complexType first
        var inline = element.Element(Xs + "complexType");
        if (inline != null) return inline;

        // Check for referenced type via type= attribute
        var typeAttr = element.Attribute("type")?.Value;
        if (typeAttr != null)
        {
            var resolved = ResolveTypeRef(typeAttr, element);
            if (resolved != null && IsComplexType(resolved))
                return resolved;
        }

        return null;
    }

    private void ProcessComplexTypeChildren(XElement complexType, XElement contextElement, TableDefinition currentTable, bool createTablesForSingletons = false)
    {
        // Handle xs:sequence
        var sequence = complexType.Element(Xs + "sequence");
        if (sequence != null)
        {
            ProcessSequenceChildren(sequence, contextElement, currentTable, createTablesForSingletons);
        }

        // Handle xs:attribute elements
        foreach (var attr in complexType.Elements(Xs + "attribute"))
        {
            var attrName = attr.Attribute("name")?.Value;
            if (attrName == null) continue;

            var attrType = attr.Attribute("type")?.Value;
            var sqlType = attrType != null ? SqlGenerator.GetSqlType(StripPrefix(attrType)) : "NVARCHAR(MAX)";

            currentTable.Columns.Add(new ColumnDefinition
            {
                ColumnName = UniqueColumnName(currentTable, attrName),
                SqlType = sqlType,
                IsNullable = attr.Attribute("use")?.Value != "required",
                XmlPath = new List<string> { "@" + attrName }
            });
        }
    }

    private void ProcessSequenceChildren(XElement sequence, XElement contextElement, TableDefinition parentTable, bool createTablesForSingletons = false)
    {
        // First pass: count duplicate element names for disambiguation
        var nameCountMap = new Dictionary<string, int>();
        foreach (var child in sequence.Elements())
        {
            if (child.Name == Xs + "element")
            {
                var (_, resolvedName) = ResolveElement(child);
                nameCountMap.TryGetValue(resolvedName, out int count);
                nameCountMap[resolvedName] = count + 1;
            }
        }

        // Collect all element names for group collision detection
        var allUsedNames = new HashSet<string>(nameCountMap.Keys);

        var nameIndexMap = new Dictionary<string, int>();

        foreach (var child in sequence.Elements())
        {
            if (child.Name == Xs + "element")
            {
                var (resolvedChild, name) = ResolveElement(child);
                var isRepeating = IsRepeating(child);
                var complexType = GetComplexType(resolvedChild);

                // Resolve table name (handle duplicates like ARV×2 in HL7)
                nameIndexMap.TryGetValue(name, out int idx);
                nameIndexMap[name] = idx + 1;

                var tableName = name;
                if (nameCountMap.TryGetValue(name, out int total) && total > 1 && idx > 0)
                {
                    tableName = $"{name}_{idx + 1}";
                }

                if (isRepeating)
                {
                    // CREATE CHILD TABLE
                    var childTable = CreateChildTable(tableName, parentTable.TableName, isRepeating: true);
                    childTable.XmlElementName = name;
                    childTable.ParentTableName = parentTable.TableName;
                    childTable.ParentXmlFieldName = name;

                    if (complexType != null)
                    {
                        ProcessComplexTypeChildren(complexType, resolvedChild, childTable);
                    }
                    else
                    {
                        // Simple repeating element - add a Value column
                        var typeAttr = resolvedChild.Attribute("type")?.Value;
                        var sqlType = typeAttr != null ? SqlGenerator.GetSqlType(StripPrefix(typeAttr)) : "NVARCHAR(MAX)";
                        childTable.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = "Value",
                            SqlType = sqlType,
                            IsNullable = true,
                            XmlPath = new List<string> { name }
                        });
                    }

                    // Only add to root-level message structure, not for nested tables
                    if (createTablesForSingletons)
                    {
                        _messageStructure.Slots.Add(new MessageSlot
                        {
                            XmlElementName = name,
                            TableName = tableName,
                            IsRepeating = true
                        });
                    }
                }
                else if (complexType != null)
                {
                    if (createTablesForSingletons)
                    {
                        // Create 1:1 child table for singleton segment
                        var childTable = CreateChildTable(tableName, parentTable.TableName, isRepeating: false);
                        childTable.XmlElementName = name;
                        childTable.ParentTableName = parentTable.TableName;
                        childTable.ParentXmlFieldName = name;
                        ProcessComplexTypeChildren(complexType, resolvedChild, childTable);

                        _messageStructure.Slots.Add(new MessageSlot
                        {
                            XmlElementName = name,
                            TableName = tableName,
                            IsRepeating = false
                        });
                    }
                    else
                    {
                        // FLATTEN singleton complex type into parent table
                        FlattenComplexType(complexType, resolvedChild, parentTable, name, new List<string> { name }, new List<string> { name });
                    }
                }
                else
                {
                    // SIMPLE COLUMN
                    var typeAttr = resolvedChild.Attribute("type")?.Value;
                    var sqlType = typeAttr != null ? SqlGenerator.GetSqlType(StripPrefix(typeAttr)) : "NVARCHAR(MAX)";
                    parentTable.Columns.Add(new ColumnDefinition
                    {
                        ColumnName = UniqueColumnName(parentTable, name),
                        SqlType = sqlType,
                        IsNullable = true,
                        XmlPath = new List<string> { name }
                    });
                }
            }
            else if (child.Name == Xs + "sequence")
            {
                // Nested xs:sequence (group)
                if (IsRepeating(child))
                {
                    var groupSlot = ProcessGroup(child, parentTable.TableName, allUsedNames);
                    if (groupSlot != null)
                        _messageStructure.Slots.Add(groupSlot);
                }
                else
                {
                    // Non-repeating nested sequence - just process children inline
                    ProcessSequenceChildren(child, contextElement, parentTable);
                }
            }
        }
    }

    private void FlattenComplexType(XElement complexType, XElement contextElement, TableDefinition table, string prefix, List<string> xmlPath, List<string>? containerPath = null)
    {
        var currentContainerPath = containerPath ?? new List<string>();
        var seq = complexType.Element(Xs + "sequence");
        if (seq != null)
        {
            foreach (var el in seq.Elements(Xs + "element"))
            {
                var (resolvedEl, elName) = ResolveElement(el);
                if (elName == "Unknown") continue;

                var colName = $"{prefix}_{elName}";
                var childXmlPath = new List<string>(xmlPath) { elName };

                var isRepeating = IsRepeating(el);
                var elComplexType = GetComplexType(resolvedEl);

                if (isRepeating)
                {
                    // Repeating element inside a flattened scope — must create a child table
                    var childTable = CreateChildTable(elName, table.TableName, isRepeating: true);
                    childTable.XmlElementName = elName;
                    childTable.ParentTableName = table.TableName;
                    childTable.ParentXmlFieldName = elName;
                    childTable.XmlContainerPath = new List<string>(currentContainerPath);

                    if (elComplexType != null)
                    {
                        ProcessComplexTypeChildren(elComplexType, resolvedEl, childTable);
                    }
                    else
                    {
                        var elTypeAttr = resolvedEl.Attribute("type")?.Value;
                        var sqlType = elTypeAttr != null ? SqlGenerator.GetSqlType(StripPrefix(elTypeAttr)) : "NVARCHAR(MAX)";
                        childTable.Columns.Add(new ColumnDefinition
                        {
                            ColumnName = "Value",
                            SqlType = sqlType,
                            IsNullable = true,
                            XmlPath = childXmlPath
                        });
                    }

                    // No slot addition here — child field tables are discovered
                    // via _childFieldTables in XmlProcessor.ExtractSegmentRow
                }
                else if (elComplexType != null)
                {
                    // Singleton complex type — continue flattening, tracking the container path
                    var nestedContainerPath = new List<string>(currentContainerPath) { elName };
                    FlattenComplexType(elComplexType, resolvedEl, table, colName, childXmlPath, nestedContainerPath);
                }
                else
                {
                    var elTypeAttr = resolvedEl.Attribute("type")?.Value;
                    var sqlType = elTypeAttr != null ? SqlGenerator.GetSqlType(StripPrefix(elTypeAttr)) : "NVARCHAR(MAX)";
                    table.Columns.Add(new ColumnDefinition
                    {
                        ColumnName = UniqueColumnName(table, colName),
                        SqlType = sqlType,
                        IsNullable = true,
                        XmlPath = childXmlPath
                    });
                }
            }
        }

        // Handle xs:attribute elements within the complex type
        foreach (var attr in complexType.Elements(Xs + "attribute"))
        {
            var attrName = attr.Attribute("name")?.Value;
            if (attrName == null) continue;

            var attrType = attr.Attribute("type")?.Value;
            var sqlType = attrType != null ? SqlGenerator.GetSqlType(StripPrefix(attrType)) : "NVARCHAR(MAX)";
            var colName = $"{prefix}_{attrName}";

            table.Columns.Add(new ColumnDefinition
            {
                ColumnName = UniqueColumnName(table, colName),
                SqlType = sqlType,
                IsNullable = attr.Attribute("use")?.Value != "required",
                XmlPath = new List<string>(xmlPath) { "@" + attrName }
            });
        }
    }

    private MessageSlot? ProcessGroup(XElement groupSequence, string parentTableName, HashSet<string> allUsedNames)
    {
        var elements = groupSequence.Elements(Xs + "element").ToList();
        if (elements.Count == 0) return null;

        // Lead segment gets parent FK
        var leadElement = elements[0];
        var (resolvedLead, leadName) = ResolveElement(leadElement);
        var leadIsRepeating = true; // Groups themselves are unbounded

        var leadTable = CreateChildTable(leadName, parentTableName, leadIsRepeating);
        leadTable.XmlElementName = leadName;

        var leadComplexType = GetComplexType(resolvedLead);
        if (leadComplexType != null)
        {
            ProcessComplexTypeChildren(leadComplexType, resolvedLead, leadTable);
        }

        var groupSlot = new MessageSlot
        {
            XmlElementName = leadName,
            TableName = leadName,
            IsRepeating = true,
            IsGroup = true,
            GroupChildren = new List<MessageSlot>()
        };

        // Child segments in the group get both parent and lead segment FKs
        for (int i = 1; i < elements.Count; i++)
        {
            var childElement = elements[i];
            var (resolvedGroupChild, childName) = ResolveElement(childElement);
            var childIsRepeating = IsRepeating(childElement);

            // Only prefix with lead name if the name collides with a used name
            var childTableName = allUsedNames.Contains(childName)
                ? $"{leadName}_{childName}"
                : childName;

            var childTable = CreateGroupChildTable(childTableName, parentTableName, leadTable.TableName, childIsRepeating);
            childTable.XmlElementName = childName;

            var childComplexType = GetComplexType(resolvedGroupChild);
            if (childComplexType != null)
            {
                ProcessComplexTypeChildren(childComplexType, resolvedGroupChild, childTable);
            }

            groupSlot.GroupChildren.Add(new MessageSlot
            {
                XmlElementName = childName,
                TableName = childTableName,
                IsRepeating = childIsRepeating
            });
        }

        return groupSlot;
    }

    private void HandleColumnOverflow()
    {
        var tablesToAdd = new List<TableDefinition>();

        foreach (var table in _tables.ToList())
        {
            if (table.Columns.Count <= MaxColumnsPerTable) continue;

            // Move overflow columns (keep system columns: Id, FKs, RepeatIndex)
            var systemColCount = table.Columns.Count(c => c.IsPrimaryKey || c.ColumnName == "RepeatIndex"
                || table.ForeignKeys.Any(fk => fk.ColumnName == c.ColumnName));
            var dataColumns = table.Columns.Where(c => !c.IsPrimaryKey && c.ColumnName != "RepeatIndex"
                && !table.ForeignKeys.Any(fk => fk.ColumnName == c.ColumnName)).ToList();

            var keepCount = MaxColumnsPerTable - systemColCount;
            var overflowColumns = dataColumns.Skip(keepCount).ToList();

            // Remove all overflow columns from the original table
            foreach (var col in overflowColumns)
            {
                table.Columns.Remove(col);
            }

            // Split overflow into chunks that each fit within the column limit
            // Each extension table has 1 system column (Id)
            var extMaxDataColumns = MaxColumnsPerTable - 1;
            var chunks = new List<List<ColumnDefinition>>();
            for (int i = 0; i < overflowColumns.Count; i += extMaxDataColumns)
            {
                chunks.Add(overflowColumns.Skip(i).Take(extMaxDataColumns).ToList());
            }

            var parentTableName = table.TableName;
            for (int c = 0; c < chunks.Count; c++)
            {
                var suffix = c == 0 ? "_Ext" : $"_Ext{c + 1}";
                var extTableName = $"{parentTableName}{suffix}";

                var extTable = new TableDefinition
                {
                    TableName = extTableName,
                    SortOrder = _sortOrder++
                };

                // PK + FK to original table
                extTable.Columns.Add(new ColumnDefinition
                {
                    ColumnName = "Id",
                    SqlType = "BIGINT",
                    IsIdentity = false,
                    IsPrimaryKey = true,
                    IsNullable = false
                });
                extTable.ForeignKeys.Add(new ForeignKeyDefinition
                {
                    ConstraintName = $"FK_{extTableName}_{parentTableName}",
                    ColumnName = "Id",
                    ReferencedTable = parentTableName,
                    ReferencedColumn = "Id"
                });

                foreach (var col in chunks[c])
                {
                    extTable.Columns.Add(col);
                }

                tablesToAdd.Add(extTable);
            }
        }

        _tables.AddRange(tablesToAdd);
    }

    // Abbreviations ordered by descending length savings for maximum shortening efficiency
    private static readonly (string Long, string Short)[] _abbreviations =
    [
        ("Identification", "Id"),
        ("Representation", "Rep"),
        ("Authentication", "Auth"),
        ("Administration", "Admin"),
        ("Administrative", "Admin"),
        ("Communication", "Comm"),
        ("Certification", "Cert"),
        ("Authorization", "Authz"),
        ("Organization", "Org"),
        ("Professional", "Prof"),
        ("Jurisdiction", "Jur"),
        ("Observation", "Obs"),
        ("Demographic", "Demo"),
        ("Information", "Info"),
        ("Instructions", "Instr"),
        ("Description", "Desc"),
        ("Restriction", "Restr"),
        ("Credential", "Cred"),
        ("Department", "Dept"),
        ("Additional", "Addl"),
        ("Expiration", "Exp"),
        ("Insurance", "Ins"),
        ("Namespace", "Ns"),
        ("Alternate", "Alt"),
        ("Effective", "Eff"),
        ("Assigning", "Asgn"),
        ("Universal", "Univ"),
        ("Facility", "Fac"),
        ("Provider", "Prov"),
        ("Patient", "Pat"),
        ("Version", "Ver"),
        ("Primary", "Prim"),
    ];

    private const int MaxIdentifierLength = 128;

    private void ShortenLongIdentifiers()
    {
        foreach (var table in _tables)
        {
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var col in table.Columns)
                usedNames.Add(col.ColumnName);

            foreach (var col in table.Columns)
            {
                if (col.ColumnName.Length <= MaxIdentifierLength) continue;

                usedNames.Remove(col.ColumnName);
                var shortened = ShortenName(col.ColumnName, MaxIdentifierLength);

                // Ensure uniqueness within the table
                var finalName = shortened;
                var suffix = 2;
                while (usedNames.Contains(finalName))
                {
                    var sfx = $"_{suffix}";
                    finalName = shortened[..Math.Min(shortened.Length, MaxIdentifierLength - sfx.Length)] + sfx;
                    suffix++;
                }

                col.ColumnName = finalName;
                usedNames.Add(finalName);
            }
        }
    }

    private static string ShortenName(string name, int maxLength)
    {
        // Apply abbreviations progressively until within limit
        var result = name;
        foreach (var (longForm, shortForm) in _abbreviations)
        {
            if (result.Length <= maxLength) break;
            result = result.Replace(longForm, shortForm);
        }

        if (result.Length <= maxLength) return result;

        // Still too long — truncate and append a stable hash for uniqueness
        var hash = GetStableShortHash(name);
        return result[..(maxLength - 9)] + "_" + hash;
    }

    private static string GetStableShortHash(string input)
    {
        unchecked
        {
            uint h = 2166136261;
            foreach (var c in input)
                h = (h ^ c) * 16777619;
            return h.ToString("x8");
        }
    }


    private void BuildTypeDictionary()
    {
        foreach (var (ns, doc) in _docs)
        {
            // Note: multiple docs may share the same namespace (via xs:include)
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

    /// <summary>
    /// Resolves an xs:element with a ref="..." attribute to the actual top-level element definition.
    /// </summary>
    private XElement? ResolveRefElement(XElement element)
    {
        var refAttr = element.Attribute("ref")?.Value;
        if (refAttr == null) return null;

        var localName = StripPrefix(refAttr);

        // Search all loaded documents for a top-level xs:element with this name
        foreach (var (_, doc) in _docs)
        {
            var resolved = doc.Descendants(Xs + "element")
                .FirstOrDefault(e => e.Parent?.Name == Xs + "schema"
                                  && e.Attribute("name")?.Value == localName);
            if (resolved != null) return resolved;
        }
        return null;
    }

    /// <summary>
    /// Given an xs:element, returns the effective element (resolving ref if present)
    /// and extracts the element name. The original element is still used for minOccurs/maxOccurs.
    /// </summary>
    private (XElement resolved, string name) ResolveElement(XElement element)
    {
        var refTarget = ResolveRefElement(element);
        if (refTarget != null)
        {
            var name = refTarget.Attribute("name")?.Value ?? "Unknown";
            return (refTarget, name);
        }
        return (element, element.Attribute("name")?.Value ?? "Unknown");
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

    /// <summary>
    /// Returns a column name that is unique within the table. If the desired name
    /// collides with an existing column (e.g. a system PK or FK column), appends
    /// "_Value", then "_2", "_3", etc.
    /// </summary>
    private static string UniqueColumnName(TableDefinition table, string desired)
    {
        var existing = new HashSet<string>(table.Columns.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);
        if (!existing.Contains(desired)) return desired;

        // First attempt: append _Value (semantic hint that this is the XML value)
        var candidate = desired + "_Value";
        if (!existing.Contains(candidate)) return candidate;

        // Fallback: numeric suffix
        var suffix = 2;
        while (existing.Contains($"{desired}_{suffix}"))
            suffix++;
        return $"{desired}_{suffix}";
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

    private TableDefinition CreateTable(string tableName)
    {
        // Auto-disambiguate if this table name was already used globally
        var finalName = tableName;
        var suffix = 2;
        while (!_createdTableNames.Add(finalName))
        {
            finalName = $"{tableName}_{suffix}";
            suffix++;
        }

        var table = new TableDefinition
        {
            TableName = finalName,
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

    private TableDefinition CreateChildTable(string tableName, string parentTableName, bool isRepeating)
    {
        var table = CreateTable(tableName);

        // FK to parent table
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

    private TableDefinition CreateGroupChildTable(string tableName, string parentTableName, string leadTableName, bool isRepeating)
    {
        var table = CreateTable(tableName);

        // FK to parent table
        var parentFkCol = $"{parentTableName}Id";
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = parentFkCol,
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.ForeignKeys.Add(new ForeignKeyDefinition
        {
            ConstraintName = $"FK_{tableName}_{parentTableName}",
            ColumnName = parentFkCol,
            ReferencedTable = parentTableName,
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
}
