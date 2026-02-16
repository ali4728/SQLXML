namespace SQLXML.Models;

/// <summary>
/// DTO for schema file data read from the metadata database.
/// </summary>
public class SchemaFileRecord
{
    public string FileName { get; set; } = string.Empty;
    public string FileRole { get; set; } = string.Empty;
    public string TargetNamespace { get; set; } = string.Empty;
    public string ContentXml { get; set; } = string.Empty;
}
