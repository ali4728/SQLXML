using Microsoft.Data.SqlClient;
using SQLXML.Models;

namespace SQLXML.Processing;

public class SqlServerLoader : IDisposable
{
    private readonly SqlConnection _conn;
    private readonly Dictionary<string, TableDefinition> _tablesByName;
    private SqlTransaction? _tx;

    public SqlServerLoader(string connectionString, List<TableDefinition> tables)
    {
        _conn = new SqlConnection(connectionString);
        _tablesByName = tables.ToDictionary(t => t.TableName);
        _conn.Open();
    }

    public void BeginTransaction()
    {
        _tx = _conn.BeginTransaction();
    }

    public void Commit()
    {
        _tx?.Commit();
        _tx = null;
    }

    public void Rollback()
    {
        _tx?.Rollback();
        _tx = null;
    }

    public Dictionary<string, int> InsertRowTree(RowData messageRow)
    {
        var rowCounts = new Dictionary<string, int>();
        var fkValues = new Dictionary<string, long>();

        // Insert root message row
        var messageParams = messageRow.Values.ToDictionary(kv => kv.Key, kv => (object?)kv.Value);
        var messageId = InsertRow(messageRow.TableName, messageParams, null, null);
        fkValues[messageRow.TableName] = messageId;
        IncrementCount(rowCounts, messageRow.TableName);

        // Insert segment rows (direct children of message)
        foreach (var segRow in messageRow.ChildRows)
        {
            InsertSegmentRow(segRow, fkValues, rowCounts);
        }

        return rowCounts;
    }

    private void InsertSegmentRow(RowData segRow, Dictionary<string, long> fkValues, Dictionary<string, int> rowCounts)
    {
        if (!_tablesByName.TryGetValue(segRow.TableName, out var tableDef))
            return;

        // Build parameter dict with FK values and data values
        var parameters = new Dictionary<string, object?>(
            segRow.Values.ToDictionary(kv => kv.Key, kv => (object?)kv.Value));

        // Add FK columns
        foreach (var fk in tableDef.ForeignKeys)
        {
            if (fkValues.TryGetValue(fk.ReferencedTable, out var fkId))
            {
                parameters[fk.ColumnName] = fkId;
            }
        }

        // Add RepeatIndex if applicable
        if (segRow.RepeatIndex.HasValue &&
            tableDef.Columns.Any(c => c.ColumnName == "RepeatIndex"))
        {
            parameters["RepeatIndex"] = segRow.RepeatIndex.Value;
        }

        var id = InsertRow(segRow.TableName, parameters, tableDef, null);
        fkValues[segRow.TableName] = id;
        IncrementCount(rowCounts, segRow.TableName);

        // Insert child rows (child field tables and group children)
        foreach (var childRow in segRow.ChildRows)
        {
            if (_tablesByName.TryGetValue(childRow.TableName, out var childTableDef))
            {
                var childParams = new Dictionary<string, object?>(
                    childRow.Values.ToDictionary(kv => kv.Key, kv => (object?)kv.Value));

                // Add FK columns for child table
                foreach (var fk in childTableDef.ForeignKeys)
                {
                    if (fkValues.TryGetValue(fk.ReferencedTable, out var fkId))
                    {
                        childParams[fk.ColumnName] = fkId;
                    }
                }

                if (childRow.RepeatIndex.HasValue &&
                    childTableDef.Columns.Any(c => c.ColumnName == "RepeatIndex"))
                {
                    childParams["RepeatIndex"] = childRow.RepeatIndex.Value;
                }

                var childId = InsertRow(childRow.TableName, childParams, childTableDef, null);
                fkValues[childRow.TableName] = childId;
                IncrementCount(rowCounts, childRow.TableName);

                // Handle nested children (group child segments with their own child field tables)
                foreach (var grandchild in childRow.ChildRows)
                {
                    InsertSegmentRow(grandchild, fkValues, rowCounts);
                }
            }
        }
    }

    private long InsertRow(string tableName, Dictionary<string, object?> parameters, TableDefinition? tableDef, SqlTransaction? explicitTx)
    {
        // Filter to only columns that have values, excluding Id (identity)
        var colsWithValues = parameters
            .Where(kv => kv.Key != "Id" && kv.Value != null)
            .ToList();

        if (colsWithValues.Count == 0)
        {
            // Insert with default values only (just identity)
            var defaultSql = $"INSERT INTO [{tableName}] DEFAULT VALUES; SELECT SCOPE_IDENTITY();";
            using var defaultCmd = new SqlCommand(defaultSql, _conn, _tx);
            var defaultResult = defaultCmd.ExecuteScalar();
            return Convert.ToInt64(defaultResult);
        }

        var columnNames = string.Join(", ", colsWithValues.Select(kv => $"[{kv.Key}]"));
        var paramNames = string.Join(", ", colsWithValues.Select(kv => $"@{SanitizeParamName(kv.Key)}"));
        var sql = $"INSERT INTO [{tableName}] ({columnNames}) VALUES ({paramNames}); SELECT SCOPE_IDENTITY();";

        using var cmd = new SqlCommand(sql, _conn, _tx);
        foreach (var kv in colsWithValues)
        {
            cmd.Parameters.AddWithValue($"@{SanitizeParamName(kv.Key)}", kv.Value ?? DBNull.Value);
        }

        var result = cmd.ExecuteScalar();
        return Convert.ToInt64(result);
    }

    private static string SanitizeParamName(string name)
    {
        // SQL parameter names can't contain certain characters
        return name.Replace(".", "_").Replace("-", "_").Replace(":", "_");
    }

    private static void IncrementCount(Dictionary<string, int> counts, string tableName)
    {
        counts.TryGetValue(tableName, out int current);
        counts[tableName] = current + 1;
    }

    public void Dispose()
    {
        _tx?.Dispose();
        _conn.Dispose();
    }
}
