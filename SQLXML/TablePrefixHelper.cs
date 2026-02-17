using SQLXML.Models;

namespace SQLXML;

public static class TablePrefixHelper
{
    public static string NormalizePrefix(string prefix)
    {
        if (!prefix.EndsWith("_")) prefix += "_";
        return prefix;
    }

    public static void ApplyPrefix(List<TableDefinition> tables, MessageStructure? structure, string prefix)
    {
        prefix = NormalizePrefix(prefix);

        // Build old→new name map
        var nameMap = tables.ToDictionary(t => t.TableName, t => prefix + t.TableName);

        // Rename tables
        foreach (var table in tables)
        {
            if (table.ParentTableName != null && nameMap.ContainsKey(table.ParentTableName))
                table.ParentTableName = nameMap[table.ParentTableName];

            foreach (var fk in table.ForeignKeys)
            {
                if (nameMap.ContainsKey(fk.ReferencedTable))
                    fk.ReferencedTable = nameMap[fk.ReferencedTable];
                // Update constraint name: FK_<newChild>_<newParent>
                fk.ConstraintName = $"FK_{nameMap[table.TableName]}_{fk.ReferencedTable}";
            }

            table.TableName = nameMap[table.TableName];
        }

        // Rename slots in MessageStructure
        if (structure != null)
        {
            ApplyPrefixToSlots(structure.Slots, nameMap);
        }
    }

    private static void ApplyPrefixToSlots(List<MessageSlot> slots, Dictionary<string, string> nameMap)
    {
        foreach (var slot in slots)
        {
            if (nameMap.ContainsKey(slot.TableName))
                slot.TableName = nameMap[slot.TableName];

            if (slot.GroupChildren != null)
                ApplyPrefixToSlots(slot.GroupChildren, nameMap);
        }
    }
}
