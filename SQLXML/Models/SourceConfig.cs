namespace SQLXML.Models;

public class SourceConfig
{
    public string? SourceConnectionString { get; set; }
    public string? SourceQuery { get; set; }
    public string SourceIdColumn { get; set; } = "Id";
    public string SourceXmlColumn { get; set; } = "HL7XML";
    public string? SourceUpdateQuery { get; set; }
}
