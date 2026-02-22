using System.Xml.Linq;
using SQLXML.Generation;
using SQLXML.Models;

namespace SQLXML.Parsing;

public class XsdParser
{
    private static readonly XNamespace Xs = "http://www.w3.org/2001/XMLSchema";

    private const int MaxRecursionDepth = 50;

    private int _recursionDepth;

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

    // Reference counting for shared/reused complex type detection
    private readonly Dictionary<string, int> _refCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TableDefinition> _sharedTables = new(StringComparer.OrdinalIgnoreCase);

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
        CountReferences();

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

    private void ProcessComplexTypeChildren(XElement complexType, XElement contextElement, TableDefinition currentTable, bool createTablesForSingletons = false, HashSet<string>? visitedTypes = null)
    {
        _recursionDepth++;
        try
        {
        if (_recursionDepth > MaxRecursionDepth)
            return;

        var typeName = complexType.Attribute("name")?.Value;
        if (typeName != null)
        {
            visitedTypes ??= new HashSet<string>();
            if (visitedTypes.Contains(typeName))
                return; // cycle — skip entirely
            visitedTypes = new HashSet<string>(visitedTypes) { typeName };
        }

        var (sequence, choice, attrSources) = GetEffectiveContent(complexType);

        // Handle xs:complexContent > xs:extension base type
        var complexContent = complexType.Element(Xs + "complexContent");
        if (complexContent != null)
        {
            var extension = complexContent.Element(Xs + "extension");
            if (extension != null)
                ProcessBaseType(extension, contextElement, currentTable, createTablesForSingletons, visitedTypes);
        }

        // Handle xs:sequence
        if (sequence != null)
        {
            ProcessSequenceChildren(sequence, contextElement, currentTable, createTablesForSingletons, visitedTypes);
        }

        // Handle xs:choice
        if (choice != null)
        {
            ProcessSequenceChildren(choice, contextElement, currentTable, false, visitedTypes);
        }

        // Handle xs:attribute elements from all sources (complexType itself + derived element)
        foreach (var source in attrSources)
        {
            foreach (var attr in source.Elements(Xs + "attribute"))
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

            // Handle xs:attributeGroup ref
            foreach (var attrGroup in source.Elements(Xs + "attributeGroup"))
            {
                var refAttr = attrGroup.Attribute("ref")?.Value;
                if (refAttr == null) continue;
                var resolved = ResolveAttributeGroup(StripPrefix(refAttr));
                if (resolved == null) continue;

                foreach (var attr in resolved.Elements(Xs + "attribute"))
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
        }
        }
        finally
        {
            _recursionDepth--;
        }
    }

    private void ProcessSequenceChildren(XElement sequence, XElement contextElement, TableDefinition parentTable, bool createTablesForSingletons = false, HashSet<string>? visitedTypes = null)
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
                var isRepeating = IsRepeating(child) || IsRepeating(sequence);
                var complexType = GetComplexType(resolvedChild);

                // Resolve table name (handle duplicates like ARV×2 in HL7)
                nameIndexMap.TryGetValue(name, out int idx);
                nameIndexMap[name] = idx + 1;

                var tableName = name;
                if (nameCountMap.TryGetValue(name, out int total) && total > 1 && idx > 0)
                {
                    tableName = $"{name}_{idx + 1}";
                }

                // Cycle detection: if this complex type is already in the visited set, store as XML column
                var childTypeName = complexType?.Attribute("name")?.Value;
                if (complexType != null && childTypeName != null && visitedTypes != null && visitedTypes.Contains(childTypeName))
                {
                    // Store recursive type reference as XML column
                    parentTable.Columns.Add(new ColumnDefinition
                    {
                        ColumnName = UniqueColumnName(parentTable, name),
                        SqlType = "NVARCHAR(MAX)",
                        IsNullable = true,
                        XmlPath = new List<string> { name }
                    });
                    continue;
                }

                if (isRepeating)
                {
                    // Check if this is a reused complex type → shared table
                    var resolvedName2 = resolvedChild.Attribute("name")?.Value ?? name;
                    var childTypeName2 = complexType?.Attribute("name")?.Value ?? resolvedName2;
                    if (complexType != null && IsReusedType(childTypeName2))
                    {
                        // Shared table path
                        TableDefinition sharedTable;
                        if (_sharedTables.TryGetValue(childTypeName2, out var existing))
                        {
                            sharedTable = existing;
                        }
                        else
                        {
                            sharedTable = CreateSharedChildTable(resolvedName2);
                            sharedTable.XmlElementName = resolvedName2;
                            ProcessComplexTypeChildren(complexType, resolvedChild, sharedTable, false, visitedTypes);
                            _sharedTables[childTypeName2] = sharedTable;
                        }

                        sharedTable.SharedParentMappings.Add(new SharedParentMapping
                        {
                            ParentTableName = parentTable.TableName,
                            ParentXmlFieldName = name
                        });

                        if (createTablesForSingletons)
                        {
                            _messageStructure.Slots.Add(new MessageSlot
                            {
                                XmlElementName = name,
                                TableName = sharedTable.TableName,
                                IsRepeating = true
                            });
                        }
                    }
                    else
                    {
                        // CREATE CHILD TABLE (non-shared path)
                        var childTable = CreateChildTable(tableName, parentTable.TableName, isRepeating: true);
                        childTable.XmlElementName = name;
                        childTable.ParentTableName = parentTable.TableName;
                        childTable.ParentXmlFieldName = name;

                        if (complexType != null)
                        {
                            ProcessComplexTypeChildren(complexType, resolvedChild, childTable, false, visitedTypes);
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
                }
                else if (complexType != null)
                {
                    // Check if this is a reused complex type → shared table (even for singletons)
                    var resolvedName2 = resolvedChild.Attribute("name")?.Value ?? name;
                    var childTypeName2 = complexType.Attribute("name")?.Value ?? resolvedName2;
                    if (IsReusedType(childTypeName2))
                    {
                        // Shared table path (same as repeating branch but without RepeatIndex)
                        TableDefinition sharedTable;
                        if (_sharedTables.TryGetValue(childTypeName2, out var existing))
                        {
                            sharedTable = existing;
                        }
                        else
                        {
                            sharedTable = CreateSharedChildTable(resolvedName2);
                            sharedTable.XmlElementName = resolvedName2;
                            ProcessComplexTypeChildren(complexType, resolvedChild, sharedTable, false, visitedTypes);
                            _sharedTables[childTypeName2] = sharedTable;
                        }

                        sharedTable.SharedParentMappings.Add(new SharedParentMapping
                        {
                            ParentTableName = parentTable.TableName,
                            ParentXmlFieldName = name
                        });

                        if (createTablesForSingletons)
                        {
                            _messageStructure.Slots.Add(new MessageSlot
                            {
                                XmlElementName = name,
                                TableName = sharedTable.TableName,
                                IsRepeating = false
                            });
                        }
                    }
                    else if (createTablesForSingletons)
                    {
                        if (IsWrapperElement(complexType))
                        {
                            // Wrapper: don't create table. Process inner children as root-level slots.
                            var wrapperSlot = new MessageSlot
                            {
                                XmlElementName = name,
                                IsWrapper = true,
                                WrapperChildren = new List<MessageSlot>()
                            };

                            var slotsBefore = _messageStructure.Slots.Count;
                            ProcessComplexTypeChildren(complexType, resolvedChild, parentTable,
                                createTablesForSingletons: true, visitedTypes);

                            // Move newly added slots into the wrapper
                            for (int s = slotsBefore; s < _messageStructure.Slots.Count; s++)
                                wrapperSlot.WrapperChildren.Add(_messageStructure.Slots[s]);
                            _messageStructure.Slots.RemoveRange(slotsBefore,
                                _messageStructure.Slots.Count - slotsBefore);
                            _messageStructure.Slots.Add(wrapperSlot);
                        }
                        else
                        {
                            // Create 1:1 child table for singleton segment
                            var childTable = CreateChildTable(tableName, parentTable.TableName, isRepeating: false);
                            childTable.XmlElementName = name;
                            childTable.ParentTableName = parentTable.TableName;
                            childTable.ParentXmlFieldName = name;

                            ProcessComplexTypeChildren(complexType, resolvedChild, childTable, false, visitedTypes);

                            _messageStructure.Slots.Add(new MessageSlot
                            {
                                XmlElementName = name,
                                TableName = tableName,
                                IsRepeating = false
                            });
                        }
                    }
                    else
                    {
                        // Create 1:1 child table for singleton complex type
                        var childTable = CreateChildTable(tableName, parentTable.TableName, isRepeating: false);
                        childTable.XmlElementName = name;
                        childTable.ParentTableName = parentTable.TableName;
                        childTable.ParentXmlFieldName = name;
                        ProcessComplexTypeChildren(complexType, resolvedChild, childTable, false, visitedTypes);
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
                    var groupSlot = ProcessGroup(child, parentTable.TableName, allUsedNames, visitedTypes);
                    if (groupSlot != null)
                        _messageStructure.Slots.Add(groupSlot);
                }
                else
                {
                    // Non-repeating nested sequence - just process children inline
                    ProcessSequenceChildren(child, contextElement, parentTable, false, visitedTypes);
                }
            }
            else if (child.Name == Xs + "choice")
            {
                // xs:choice — process children as nullable alternatives
                ProcessSequenceChildren(child, contextElement, parentTable, false, visitedTypes);
            }
        }
    }

    private MessageSlot? ProcessGroup(XElement groupSequence, string parentTableName, HashSet<string> allUsedNames, HashSet<string>? visitedTypes = null)
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
            ProcessComplexTypeChildren(leadComplexType, resolvedLead, leadTable, false, visitedTypes);
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
                ProcessComplexTypeChildren(childComplexType, resolvedGroupChild, childTable, false, visitedTypes);
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

    /// <summary>
    /// Walks all XSD docs counting how many times each named complex type is referenced
    /// via ref="..." or type="..." on xs:element nodes.
    /// </summary>
    private void CountReferences()
    {
        foreach (var (_, doc) in _docs)
        {
            foreach (var el in doc.Descendants(Xs + "element"))
            {
                // Count ref="..." usages
                var refAttr = el.Attribute("ref")?.Value;
                if (refAttr != null)
                {
                    var localName = StripPrefix(refAttr);
                    // Check if the referenced element resolves to a complex type
                    var resolved = ResolveRefElement(el);
                    if (resolved != null && GetComplexType(resolved) != null)
                    {
                        _refCounts.TryGetValue(localName, out int c);
                        _refCounts[localName] = c + 1;
                    }
                }

                // Count type="..." usages that resolve to named complex types
                var typeAttr = el.Attribute("type")?.Value;
                if (typeAttr != null)
                {
                    var localName = StripPrefix(typeAttr);
                    var resolved = ResolveTypeRef(typeAttr, el);
                    if (resolved != null && IsComplexType(resolved))
                    {
                        _refCounts.TryGetValue(localName, out int c);
                        _refCounts[localName] = c + 1;
                    }
                }
            }
        }
    }

    private bool IsReusedType(string name)
    {
        return _refCounts.TryGetValue(name, out int count) && count >= 2;
    }

    /// <summary>
    /// Creates a shared child table with ParentKey/ParentType instead of a hard FK.
    /// </summary>
    private TableDefinition CreateSharedChildTable(string tableName)
    {
        var table = CreateTable(tableName);

        table.IsSharedTable = true;

        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = "ParentKey",
            SqlType = "BIGINT",
            IsNullable = false
        });
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = "ParentType",
            SqlType = "NVARCHAR(128)",
            IsNullable = false
        });
        table.Columns.Add(new ColumnDefinition
        {
            ColumnName = "RepeatIndex",
            SqlType = "INT",
            IsNullable = false
        });

        return table;
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


    /// <summary>
    /// Resolves an xs:attributeGroup by name from loaded XSD documents.
    /// </summary>
    private XElement? ResolveAttributeGroup(string name)
    {
        foreach (var (_, doc) in _docs)
        {
            var ag = doc.Descendants(Xs + "attributeGroup")
                .FirstOrDefault(e => e.Parent?.Name == Xs + "schema"
                                  && e.Attribute("name")?.Value == name);
            if (ag != null) return ag;
        }
        return null;
    }

    /// <summary>
    /// Returns the effective sequence, choice, and attribute source from a complexType,
    /// handling xs:complexContent/extension/restriction transparently.
    /// Also processes base type content for extensions.
    /// </summary>
    private (XElement? Sequence, XElement? Choice, List<XElement> AttributeSources)
        GetEffectiveContent(XElement complexType)
    {
        var sources = new List<XElement> { complexType };
        var sequence = complexType.Element(Xs + "sequence");
        var choice = complexType.Element(Xs + "choice");

        var complexContent = complexType.Element(Xs + "complexContent");
        if (complexContent != null)
        {
            var extension = complexContent.Element(Xs + "extension");
            var restriction = complexContent.Element(Xs + "restriction");
            var derived = extension ?? restriction;
            if (derived != null)
            {
                sources.Add(derived);
                sequence ??= derived.Element(Xs + "sequence");
                choice ??= derived.Element(Xs + "choice");
            }
        }

        return (sequence, choice, sources);
    }

    /// <summary>
    /// Processes the base type of an xs:extension element, if present.
    /// </summary>
    private void ProcessBaseType(XElement extensionOrRestriction, XElement contextElement,
        TableDefinition table, bool createTablesForSingletons, HashSet<string>? visitedTypes)
    {
        var baseTypeRef = extensionOrRestriction.Attribute("base")?.Value;
        if (baseTypeRef == null) return;

        var baseType = ResolveTypeRef(baseTypeRef, extensionOrRestriction);
        if (baseType != null && IsComplexType(baseType))
        {
            var baseTypeName = baseType.Attribute("name")?.Value;
            if (baseTypeName != null && visitedTypes != null && visitedTypes.Contains(baseTypeName))
                return; // cycle detected in base type chain
            var newVisited = new HashSet<string>(visitedTypes ?? Enumerable.Empty<string>());
            if (baseTypeName != null) newVisited.Add(baseTypeName);
            ProcessComplexTypeChildren(baseType, contextElement, table, createTablesForSingletons, newVisited);
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

    /// <summary>
    /// A singleton complex element is a "wrapper" if its complex type has no attributes/attributeGroups
    /// and all its child elements are repeating. Such a wrapper table would have zero data columns.
    /// </summary>
    private bool IsWrapperElement(XElement complexType)
    {
        var (seq, choice, attrSources) = GetEffectiveContent(complexType);

        // Has attributes? Not a wrapper
        foreach (var source in attrSources)
        {
            if (source.Elements(Xs + "attribute").Any()
                || source.Elements(Xs + "attributeGroup").Any())
                return false;
        }

        var elementSource = seq ?? choice;
        if (elementSource == null) return false;

        var elements = elementSource.Elements(Xs + "element").ToList();
        if (elements.Count == 0) return false;

        // All children must be repeating
        return elements.All(e => IsRepeating(e));
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
