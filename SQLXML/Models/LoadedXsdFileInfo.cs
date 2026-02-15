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
}
