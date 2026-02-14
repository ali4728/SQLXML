namespace SQLXML.Models;

public class ProcessingResult
{
    public string FileName { get; set; } = string.Empty;
    public bool Success { get; set; }
    public Dictionary<string, int> RowCounts { get; } = new();
    public string? ErrorMessage { get; set; }
}
