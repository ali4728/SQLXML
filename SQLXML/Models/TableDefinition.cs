namespace SQLXML.Models;

public class TableDefinition
{
    public string TableName { get; set; } = string.Empty;
    public List<ColumnDefinition> Columns { get; } = new();
    public List<ForeignKeyDefinition> ForeignKeys { get; } = new();
    public int SortOrder { get; set; }
    public string XmlElementName { get; set; } = string.Empty;
    public string? ParentTableName { get; set; }
    public string? ParentXmlFieldName { get; set; }

    /// <summary>
    /// When set, indicates that this table's XML elements are nested inside a wrapper element
    /// that was eliminated from the schema. XML navigation must first enter this wrapper element.
    /// </summary>
    public string? WrapperXmlElementName { get; set; }

    /// <summary>
    /// True if this table is shared across multiple parent types (polymorphic parent via ParentKey/ParentType).
    /// </summary>
    public bool IsSharedTable { get; set; }

    /// <summary>
    /// Mappings describing each parent that references this shared table.
    /// </summary>
    public List<SharedParentMapping> SharedParentMappings { get; set; } = new();
}

public class ColumnDefinition
{
    public string ColumnName { get; set; } = string.Empty;
    public string SqlType { get; set; } = "NVARCHAR(MAX)";
    public bool IsNullable { get; set; } = true;
    public bool IsIdentity { get; set; }
    public bool IsPrimaryKey { get; set; }
    public List<string> XmlPath { get; set; } = new();
}

public class SharedParentMapping
{
    public string ParentTableName { get; set; } = string.Empty;
    public string ParentXmlFieldName { get; set; } = string.Empty;
    public string? WrapperXmlElementName { get; set; }
}

public class ForeignKeyDefinition
{
    public string ConstraintName { get; set; } = string.Empty;
    public string ColumnName { get; set; } = string.Empty;
    public string ReferencedTable { get; set; } = string.Empty;
    public string ReferencedColumn { get; set; } = string.Empty;
}
