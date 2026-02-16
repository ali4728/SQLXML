namespace SQLXML.Models;

/// <summary>
/// Information about an XSD file loaded during parsing (root + imports).
/// </summary>
public class LoadedXsdFileInfo
{
    public string FilePath { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public string TargetNamespace { get; set; } = string.Empty;
    /// <summary>'Root' | 'Import' | 'Include' | 'Redefine'</summary>
    public string FileRole { get; set; } = "Other";
    /// <summary>Raw XML text content of the XSD file (populated during disk loading for DB storage).</summary>
    public string? ContentXml { get; set; }
}
