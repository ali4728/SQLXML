namespace SQLXML.Models;

public class MessageStructure
{
    public List<MessageSlot> Slots { get; } = new();
}

public class MessageSlot
{
    public string XmlElementName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public bool IsRepeating { get; set; }
    public bool IsGroup { get; set; }
    public List<MessageSlot>? GroupChildren { get; set; }
}
