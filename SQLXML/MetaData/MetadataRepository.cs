using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.SqlClient;
using SQLXML.Models;

namespace SQLXML.MetaData;

/// <summary>
/// Manages all SQLXML_* metadata tables: schema sets, generation runs,
/// generated scripts, apply runs, XML load runs, and pipeline errors.
/// </summary>
public class MetadataRepository : IDisposable
{
    private readonly SqlConnection _conn;
#pragma warning disable CS0649 // _tx reserved for future transactional metadata operations
    private SqlTransaction? _tx;
#pragma warning restore CS0649

    public MetadataRepository(string connectionString)
    {
        _conn = new SqlConnection(connectionString);
        _conn.Open();
    }

    // ── helpers ──────────────────────────────────────────────────────

    /// <summary>Ensures the metadata tables exist (idempotent).</summary>
    public void EnsureMetadataTables()
    {
        const string check = """
            IF OBJECT_ID('dbo.SQLXML_XsdSchemaSet','U') IS NULL
                SELECT 0 ELSE SELECT 1
            """;
        using var cmd = new SqlCommand(check, _conn, _tx);
        var exists = Convert.ToInt32(cmd.ExecuteScalar());
        if (exists == 1) return;

        // Read the embedded SQL script and execute it
        var scriptPath = FindCreateTablesScript();
        if (scriptPath == null)
            throw new InvalidOperationException(
                "Could not locate MetaData/Sql/CreateTables.sql relative to the application.");

        var script = File.ReadAllText(scriptPath);
        ExecuteBatches(script);
    }

    private static string? FindCreateTablesScript()
    {
        // Try relative to the running executable first, then working directory
        var candidates = new[]
        {
            Path.Combine(AppContext.BaseDirectory, "MetaData", "Sql", "CreateTables.sql"),
            Path.Combine(Directory.GetCurrentDirectory(), "MetaData", "Sql", "CreateTables.sql"),
            Path.Combine(Directory.GetCurrentDirectory(), "SQLXML", "MetaData", "Sql", "CreateTables.sql"),
        };

        foreach (var c in candidates)
            if (File.Exists(c)) return c;

        return null;
    }

    private void ExecuteBatches(string script)
    {
        // Split on GO (case-insensitive, whole line)
        var batches = System.Text.RegularExpressions.Regex.Split(
            script, @"^\s*GO\s*$",
            System.Text.RegularExpressions.RegexOptions.Multiline |
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;
            using var cmd = new SqlCommand(trimmed, _conn, _tx);
            cmd.ExecuteNonQuery();
        }
    }

    // ── 1) Schema Set ───────────────────────────────────────────────

    /// <summary>Registers a schema set and its files. Returns the new SchemaSetId.</summary>
    public long InsertSchemaSet(
        string schemaSetKey,
        string versionLabel,
        string rootXsdFileName,
        string? rootTargetNamespace,
        string? displayName = null,
        string? description = null,
        string? combinedSha256 = null,
        string? createdBy = null)
    {
        const string sql = """
            INSERT INTO dbo.SQLXML_XsdSchemaSet
                (SchemaSetKey, VersionLabel, RootXsdFileName, RootTargetNamespace,
                 DisplayName, Description, CombinedSha256, CreatedBy)
            VALUES
                (@Key, @Ver, @Root, @Ns, @Display, @Desc, @Sha, @By);
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Key", schemaSetKey);
        cmd.Parameters.AddWithValue("@Ver", versionLabel);
        cmd.Parameters.AddWithValue("@Root", rootXsdFileName);
        cmd.Parameters.AddWithValue("@Ns", (object?)rootTargetNamespace ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Display", (object?)displayName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Desc", (object?)description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Sha", (object?)combinedSha256 ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@By", (object?)createdBy ?? DBNull.Value);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── 2) Schema Files ─────────────────────────────────────────────

    public long InsertSchemaFile(
        long schemaSetId,
        string fileRole,
        string fileName,
        string? filePathOrUri,
        string? targetNamespace,
        string? fileSha256,
        string? contentXml = null,
        string? importedBy = null)
    {
        const string sql = """
            INSERT INTO dbo.SQLXML_XsdSchemaFile
                (SchemaSetId, FileRole, FileName, FilePathOrUri, TargetNamespace,
                 ContentXml, FileSha256, ImportedBy)
            VALUES
                (@SetId, @Role, @Name, @Path, @Ns, @Xml, @Sha, @By);
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@SetId", schemaSetId);
        cmd.Parameters.AddWithValue("@Role", fileRole);
        cmd.Parameters.AddWithValue("@Name", fileName);
        cmd.Parameters.AddWithValue("@Path", (object?)filePathOrUri ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Ns", (object?)targetNamespace ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Xml", (object?)contentXml ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Sha", (object?)fileSha256 ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@By", (object?)importedBy ?? DBNull.Value);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── 3) Generation Run ───────────────────────────────────────────

    public long InsertGenerationRun(
        long schemaSetId,
        string? toolName = null,
        string? toolVersion = null,
        string? configJson = null)
    {
        // Auto-increment RunVersion per SchemaSetId
        const string getNextVersion = """
            SELECT ISNULL(MAX(RunVersion), 0) + 1
            FROM dbo.SQLXML_XsdGenerationRun
            WHERE SchemaSetId = @SetId
            """;

        using var vCmd = new SqlCommand(getNextVersion, _conn, _tx);
        vCmd.Parameters.AddWithValue("@SetId", schemaSetId);
        var runVersion = Convert.ToInt32(vCmd.ExecuteScalar());

        const string sql = """
            INSERT INTO dbo.SQLXML_XsdGenerationRun
                (SchemaSetId, RunVersion, ToolName, ToolVersion, ConfigJson, Status)
            VALUES
                (@SetId, @RunVer, @Tool, @ToolVer, @Config, 'Running');
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@SetId", schemaSetId);
        cmd.Parameters.AddWithValue("@RunVer", runVersion);
        cmd.Parameters.AddWithValue("@Tool", (object?)toolName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ToolVer", (object?)toolVersion ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Config", (object?)configJson ?? DBNull.Value);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    public void CompleteGenerationRun(long generationRunId, string status, string? message = null)
    {
        const string sql = """
            UPDATE dbo.SQLXML_XsdGenerationRun
            SET FinishedUtc = SYSUTCDATETIME(),
                Status      = @Status,
                Message     = @Msg
            WHERE GenerationRunId = @Id
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Id", generationRunId);
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@Msg", (object?)message ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ── 4) Generated Scripts ────────────────────────────────────────

    public long InsertGeneratedScript(
        long schemaSetId,
        long generationRunId,
        string scriptType,
        string? targetSchemaName,
        string? targetObjectName,
        string? targetObjectType,
        string scriptText,
        int? applyOrder = null)
    {
        var sha = ComputeSha256(scriptText);

        const string sql = """
            INSERT INTO dbo.SQLXML_SqlGeneratedScript
                (SchemaSetId, GenerationRunId, ScriptType, TargetSchemaName,
                 TargetObjectName, TargetObjectType, ScriptText, ScriptSha256, ApplyOrder)
            VALUES
                (@SetId, @RunId, @Type, @Schema, @ObjName, @ObjType, @Text, @Sha, @Order);
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@SetId", schemaSetId);
        cmd.Parameters.AddWithValue("@RunId", generationRunId);
        cmd.Parameters.AddWithValue("@Type", scriptType);
        cmd.Parameters.AddWithValue("@Schema", (object?)targetSchemaName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ObjName", (object?)targetObjectName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ObjType", (object?)targetObjectType ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Text", scriptText);
        cmd.Parameters.AddWithValue("@Sha", sha);
        cmd.Parameters.AddWithValue("@Order", (object?)applyOrder ?? DBNull.Value);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── 5) XML Load Run ─────────────────────────────────────────────

    public long InsertXmlLoadRun(
        long schemaSetId,
        string? sourceSystem = null,
        string? sourceFileName = null,
        string? sourceUri = null,
        string? sourceFileSha256 = null)
    {
        const string sql = """
            INSERT INTO dbo.SQLXML_XmlLoadRun
                (SchemaSetId, SourceSystem, SourceFileName, SourceUri, SourceFileSha256)
            VALUES
                (@SetId, @Sys, @File, @Uri, @Sha);
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@SetId", schemaSetId);
        cmd.Parameters.AddWithValue("@Sys", (object?)sourceSystem ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@File", (object?)sourceFileName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Uri", (object?)sourceUri ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Sha", (object?)sourceFileSha256 ?? DBNull.Value);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    public void CompleteXmlLoadRun(
        long loadRunId, string status,
        long? rowsInserted = null, long? rowsUpdated = null, long? rowsRejected = null,
        string? message = null)
    {
        const string sql = """
            UPDATE dbo.SQLXML_XmlLoadRun
            SET FinishedUtc   = SYSUTCDATETIME(),
                Status        = @Status,
                RowsInserted  = @Ins,
                RowsUpdated   = @Upd,
                RowsRejected  = @Rej,
                Message       = @Msg
            WHERE LoadRunId = @Id
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Id", loadRunId);
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@Ins", (object?)rowsInserted ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Upd", (object?)rowsUpdated ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Rej", (object?)rowsRejected ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Msg", (object?)message ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    public long InsertXmlLoadRunTableLog(
        long loadRunId,
        string targetSchemaName,
        string targetTableName)
    {
        const string sql = """
            INSERT INTO dbo.SQLXML_XmlLoadRunTableLog
                (LoadRunId, TargetSchemaName, TargetTableName)
            VALUES
                (@RunId, @Schema, @Table);
            SELECT SCOPE_IDENTITY();
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@RunId", loadRunId);
        cmd.Parameters.AddWithValue("@Schema", targetSchemaName);
        cmd.Parameters.AddWithValue("@Table", targetTableName);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    public void CompleteXmlLoadRunTableLog(
        long logId, string status,
        long? rowsInserted = null, long? rowsRejected = null,
        string? errorMessage = null)
    {
        const string sql = """
            UPDATE dbo.SQLXML_XmlLoadRunTableLog
            SET FinishedUtc   = SYSUTCDATETIME(),
                Status        = @Status,
                RowsInserted  = @Ins,
                RowsRejected  = @Rej,
                ErrorMessage  = @ErrMsg
            WHERE LoadRunTableLogId = @Id
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Id", logId);
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@Ins", (object?)rowsInserted ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Rej", (object?)rowsRejected ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ErrMsg", (object?)errorMessage ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ── 7) Pipeline Error Log ───────────────────────────────────────

    public void LogPipelineError(
        string area,
        string errorMessage,
        long? schemaSetId = null,
        long? generationRunId = null,
        long? loadRunId = null,
        int? errorNumber = null,
        string? contextJson = null)
    {
        const string sql = """
            INSERT INTO dbo.SQLXML_PipelineErrorLog
                (Area, SchemaSetId, GenerationRunId, LoadRunId,
                 ErrorNumber, ErrorMessage, ContextJson)
            VALUES
                (@Area, @SetId, @GenId, @LoadId, @ErrNo, @ErrMsg, @Ctx)
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Area", area);
        cmd.Parameters.AddWithValue("@SetId", (object?)schemaSetId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@GenId", (object?)generationRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@LoadId", (object?)loadRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ErrNo", (object?)errorNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ErrMsg", errorMessage);
        cmd.Parameters.AddWithValue("@Ctx", (object?)contextJson ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ── Schema File Retrieval ────────────────────────────────────────

    /// <summary>
    /// Returns all XSD files for the given schema set, ordered by FileRole (Root first).
    /// </summary>
    public List<SchemaFileRecord> GetSchemaFiles(long schemaSetId)
    {
        const string sql = """
            SELECT FileName, FileRole, TargetNamespace, ContentXml
            FROM dbo.SQLXML_XsdSchemaFile
            WHERE SchemaSetId = @SetId
            ORDER BY CASE FileRole WHEN 'Root' THEN 0 ELSE 1 END, FileName
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@SetId", schemaSetId);
        using var reader = cmd.ExecuteReader();

        var results = new List<SchemaFileRecord>();
        while (reader.Read())
        {
            results.Add(new SchemaFileRecord
            {
                FileName = reader.GetString(0),
                FileRole = reader.GetString(1),
                TargetNamespace = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ContentXml = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }
        return results;
    }

    // ── Helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Look up existing active schema set by key. Returns SchemaSetId or null.
    /// </summary>
    public long? FindActiveSchemaSet(string schemaSetKey, string versionLabel)
    {
        const string sql = """
            SELECT SchemaSetId FROM dbo.SQLXML_XsdSchemaSet
            WHERE SchemaSetKey = @Key AND VersionLabel = @Ver AND IsActive = 1
            """;

        using var cmd = new SqlCommand(sql, _conn, _tx);
        cmd.Parameters.AddWithValue("@Key", schemaSetKey);
        cmd.Parameters.AddWithValue("@Ver", versionLabel);
        var result = cmd.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : Convert.ToInt64(result);
    }

    public static string ComputeSha256(string content)
    {
        var bytes = Encoding.UTF8.GetBytes(content);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static string ComputeFileSha256(string filePath)
    {
        using var stream = File.OpenRead(filePath);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Computes a combined SHA-256 over multiple files (sorted by name, concatenated).
    /// </summary>
    public static string ComputeCombinedSha256(IEnumerable<string> filePaths)
    {
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var path in filePaths.OrderBy(p => Path.GetFileName(p), StringComparer.OrdinalIgnoreCase))
        {
            var bytes = File.ReadAllBytes(path);
            sha.AppendData(bytes);
        }
        var hash = sha.GetCurrentHash();
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Computes a combined SHA-256 over in-memory content entries (sorted by name, concatenated).
    /// </summary>
    public static string ComputeCombinedSha256FromContent(IEnumerable<(string FileName, string Content)> entries)
    {
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var (_, content) in entries.OrderBy(e => e.FileName, StringComparer.OrdinalIgnoreCase))
        {
            sha.AppendData(Encoding.UTF8.GetBytes(content));
        }
        var hash = sha.GetCurrentHash();
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public void Dispose()
    {
        _tx?.Dispose();
        _conn.Dispose();
    }
}
