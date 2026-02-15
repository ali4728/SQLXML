using System.Xml.Linq;
using SQLXML.Models;

namespace SQLXML.Processing;

public class RowData
{
    public string TableName { get; set; } = string.Empty;
    public Dictionary<string, string?> Values { get; } = new();
    public int? RepeatIndex { get; set; }
    public List<RowData> ChildRows { get; } = new();
}

public class XmlProcessor
{
    private readonly List<TableDefinition> _tables;
    private readonly MessageStructure _structure;
    private readonly Dictionary<string, TableDefinition> _tablesByName;
    private readonly Dictionary<string, List<TableDefinition>> _childFieldTables; // parentTableName -> child tables

    public XmlProcessor(List<TableDefinition> tables, MessageStructure structure)
    {
        _tables = tables;
        _structure = structure;
        _tablesByName = tables.ToDictionary(t => t.TableName);
        _childFieldTables = tables
            .Where(t => t.ParentTableName != null)
            .GroupBy(t => t.ParentTableName!)
            .ToDictionary(g => g.Key, g => g.ToList());
    }

    public RowData ProcessFile(XDocument xml)
    {
        var root = xml.Root;
        if (root == null)
            throw new InvalidOperationException("XML document has no root element.");

        var rootTable = _tables.First(t => t.ParentTableName == null && t.ForeignKeys.Count == 0);
        var messageRow = new RowData { TableName = rootTable.TableName };

        // Extract root element's own column values (e.g. attributes, simple child elements)
        if (_tablesByName.TryGetValue(rootTable.TableName, out var rootTableDef))
        {
            foreach (var col in rootTableDef.Columns)
            {
                if (col.XmlPath.Count == 0) continue;
                var value = NavigateXmlPath(root, col.XmlPath);
                if (value != null)
                    messageRow.Values[col.ColumnName] = value;
            }
        }

        // Get all direct children of the root element
        var xmlChildren = root.Elements().ToList();
        int idx = 0;

        // Build a set of all slot element names so we don't skip past them
        var slotNames = new HashSet<string>(_structure.Slots.Select(s => s.XmlElementName));

        foreach (var slot in _structure.Slots)
        {
            // Skip XML children that don't match any slot, but stop if we hit another slot's element
            while (idx < xmlChildren.Count
                   && xmlChildren[idx].Name.LocalName != slot.XmlElementName
                   && !slotNames.Contains(xmlChildren[idx].Name.LocalName))
                idx++;

            if (slot.IsGroup)
            {
                ConsumeGroupInstances(xmlChildren, ref idx, slot, messageRow);
            }
            else if (slot.IsRepeating)
            {
                ConsumeRepeatingSegments(xmlChildren, ref idx, slot, messageRow);
            }
            else
            {
                ConsumeOptionalSegment(xmlChildren, ref idx, slot, messageRow);
            }
        }

        return messageRow;
    }

    private void ConsumeOptionalSegment(List<XElement> xmlChildren, ref int idx, MessageSlot slot, RowData parentRow)
    {
        if (idx >= xmlChildren.Count) return;

        var el = xmlChildren[idx];
        if (el.Name.LocalName != slot.XmlElementName) return;

        var row = ExtractSegmentRow(el, slot.TableName);
        parentRow.ChildRows.Add(row);
        idx++;
    }

    private void ConsumeRepeatingSegments(List<XElement> xmlChildren, ref int idx, MessageSlot slot, RowData parentRow)
    {
        int repeatIndex = 0;
        while (idx < xmlChildren.Count && xmlChildren[idx].Name.LocalName == slot.XmlElementName)
        {
            var row = ExtractSegmentRow(xmlChildren[idx], slot.TableName);
            row.RepeatIndex = repeatIndex++;
            parentRow.ChildRows.Add(row);
            idx++;
        }
    }

    private void ConsumeGroupInstances(List<XElement> xmlChildren, ref int idx, MessageSlot groupSlot, RowData messageRow)
    {
        // A group is led by the lead element. Consume instances until lead element stops appearing.
        int repeatIndex = 0;
        while (idx < xmlChildren.Count && xmlChildren[idx].Name.LocalName == groupSlot.XmlElementName)
        {
            // Extract lead segment
            var leadRow = ExtractSegmentRow(xmlChildren[idx], groupSlot.TableName);
            leadRow.RepeatIndex = repeatIndex++;
            messageRow.ChildRows.Add(leadRow);
            idx++;

            // Consume group children
            if (groupSlot.GroupChildren != null)
            {
                foreach (var childSlot in groupSlot.GroupChildren)
                {
                    if (childSlot.IsRepeating)
                    {
                        int childRepeatIndex = 0;
                        while (idx < xmlChildren.Count && xmlChildren[idx].Name.LocalName == childSlot.XmlElementName)
                        {
                            var childRow = ExtractSegmentRow(xmlChildren[idx], childSlot.TableName);
                            childRow.RepeatIndex = childRepeatIndex++;
                            leadRow.ChildRows.Add(childRow);
                            idx++;
                        }
                    }
                    else
                    {
                        if (idx < xmlChildren.Count && xmlChildren[idx].Name.LocalName == childSlot.XmlElementName)
                        {
                            var childRow = ExtractSegmentRow(xmlChildren[idx], childSlot.TableName);
                            leadRow.ChildRows.Add(childRow);
                            idx++;
                        }
                    }
                }
            }
        }
    }

    private RowData ExtractSegmentRow(XElement segElement, string tableName)
    {
        var row = new RowData { TableName = tableName };

        if (!_tablesByName.TryGetValue(tableName, out var tableDef))
            return row;

        // Extract values for data columns (those with XmlPath)
        foreach (var col in tableDef.Columns)
        {
            if (col.XmlPath.Count == 0) continue;

            var value = NavigateXmlPath(segElement, col.XmlPath);
            if (value != null)
            {
                row.Values[col.ColumnName] = value;
            }
        }

        // Extract child field table rows (repeating fields within this segment)
        if (_childFieldTables.TryGetValue(tableName, out var childTables))
        {
            foreach (var childTableDef in childTables)
            {
                var fieldName = childTableDef.ParentXmlFieldName;
                if (fieldName == null) continue;

                // Navigate through XmlContainerPath to find the right parent element
                var searchRoot = segElement;
                if (childTableDef.XmlContainerPath.Count > 0)
                {
                    foreach (var containerName in childTableDef.XmlContainerPath)
                    {
                        searchRoot = searchRoot.Elements()
                            .FirstOrDefault(e => e.Name.LocalName == containerName);
                        if (searchRoot == null) break;
                    }
                    if (searchRoot == null) continue;
                }

                // Find all repeating elements matching this field name
                var repeatingElements = searchRoot.Elements()
                    .Where(e => e.Name.LocalName == fieldName)
                    .ToList();

                for (int i = 0; i < repeatingElements.Count; i++)
                {
                    // Recursively extract the child row (so its own children are also processed)
                    var childRow = ExtractSegmentRow(repeatingElements[i], childTableDef.TableName);
                    childRow.RepeatIndex = i;
                    row.ChildRows.Add(childRow);
                }
            }
        }

        return row;
    }

    private static string? NavigateXmlPath(XElement element, List<string> path)
    {
        var current = element;
        for (int i = 0; i < path.Count - 1; i++)
        {
            current = current.Elements().FirstOrDefault(e => e.Name.LocalName == path[i]);
            if (current == null) return null;
        }

        var lastSegment = path[^1];

        // Handle XML attributes (paths starting with "@")
        if (lastSegment.StartsWith("@"))
        {
            var attrName = lastSegment.Substring(1);
            return current.Attribute(attrName)?.Value;
        }

        var leaf = current.Elements().FirstOrDefault(e => e.Name.LocalName == lastSegment);
        return leaf?.Value;
    }
}
