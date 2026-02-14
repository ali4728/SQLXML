using System.Text;
using SQLXML.Models;

namespace SQLXML.Generation;

public static class SqlGenerator
{
    public static string Generate(List<TableDefinition> tables)
    {
        var sb = new StringBuilder();
        var sorted = tables.OrderBy(t => t.SortOrder).ToList();

        foreach (var table in sorted)
        {
            sb.AppendLine($"CREATE TABLE [{table.TableName}] (");

            var lines = new List<string>();

            foreach (var col in table.Columns)
            {
                var identity = col.IsIdentity ? " IDENTITY(1,1)" : "";
                var nullable = col.IsNullable ? " NULL" : " NOT NULL";
                lines.Add($"    [{col.ColumnName}] {col.SqlType}{identity}{nullable}");
            }

            // Primary key constraint
            var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
            if (pkCols.Count > 0)
            {
                var pkColNames = string.Join(", ", pkCols.Select(c => $"[{c.ColumnName}]"));
                lines.Add($"    CONSTRAINT [PK_{table.TableName}] PRIMARY KEY CLUSTERED ({pkColNames})");
            }

            // Foreign key constraints
            foreach (var fk in table.ForeignKeys)
            {
                lines.Add($"    CONSTRAINT [{fk.ConstraintName}] FOREIGN KEY ([{fk.ColumnName}]) REFERENCES [{fk.ReferencedTable}]([{fk.ReferencedColumn}])");
            }

            sb.AppendLine(string.Join(",\n", lines));
            sb.AppendLine(");");
            sb.AppendLine("GO");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    public static string GetSqlType(string typeName)
    {
        // Strip suffixes like _0, _1, _396 etc. to get the base type name
        var baseType = GetBaseTypeName(typeName);

        return baseType switch
        {
            "NM" => "NVARCHAR(50)",
            "SI" => "NVARCHAR(50)",
            "DTM" => "NVARCHAR(50)",
            "DT" => "NVARCHAR(20)",
            "TM" => "NVARCHAR(20)",
            _ => "NVARCHAR(MAX)"
        };
    }

    private static string GetBaseTypeName(string typeName)
    {
        // Types like "ST_0", "NM_6", "DTM_1", "ID_396" - extract the prefix before the first underscore+digits
        // But also handle "IS_1", "ID_2" which are base types with table references
        // The base HL7 types are: ST, NM, SI, DTM, DT, TM, ID, IS, TX, FT, GTS, varies
        string[] knownBases = ["ST", "NM", "SI", "DTM", "DT", "TM", "ID", "IS", "TX", "FT", "GTS", "varies"];

        foreach (var b in knownBases)
        {
            if (typeName == b || typeName.StartsWith(b + "_"))
                return b;
        }

        return typeName;
    }
}
