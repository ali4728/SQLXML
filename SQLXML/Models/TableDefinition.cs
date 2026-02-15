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
    /// Path from the parent segment element down to the container of repeating child elements.
    /// E.g. for Encounter under Transaction/Encounters/Encounter, this would be ["Encounters"].
    /// Empty list means the child is a direct child of the parent element.
    /// </summary>
    public List<string> XmlContainerPath { get; set; } = new();
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

public class ForeignKeyDefinition
{
    public string ConstraintName { get; set; } = string.Empty;
    public string ColumnName { get; set; } = string.Empty;
    public string ReferencedTable { get; set; } = string.Empty;
    public string ReferencedColumn { get; set; } = string.Empty;
}
