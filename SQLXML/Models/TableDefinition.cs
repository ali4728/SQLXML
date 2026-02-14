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
