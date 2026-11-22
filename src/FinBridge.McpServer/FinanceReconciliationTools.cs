using System.ComponentModel;
using Dapper;
using FinBridge.Contracts;
using ModelContextProtocol.Server;
using Npgsql;

namespace FinBridge.McpServer;

[McpServerToolType]
public sealed class FinanceReconciliationTools(
    IConfiguration configuration,
    UserContext userContext,
    AuditLogger auditLogger)
{
    private readonly string _connectionString = configuration.GetConnectionString("LedgerDb")!;

    [McpServerTool, Description("Returns high-level reconciliation metrics comparing synced Dynamics invoices against unresolved DLQ discrepancies.")]
    public async Task<LedgerSummaryDto> GetPipelineHealthSummaryAsync(CancellationToken ct)
    {
        userContext.EnsureRole("FinanceAuditor");
        await auditLogger.LogToolInvocationAsync(userContext, nameof(GetPipelineHealthSummaryAsync), new { }, ct);

        await using var conn = new NpgsqlConnection(_connectionString);
        var syncedCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM processed_events");
        var syncedTotal = await conn.ExecuteScalarAsync<decimal?>("SELECT SUM(amount) FROM processed_events") ?? 0m;
        var unresolvedCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM sync_discrepancies WHERE resolved = FALSE");
        var lastSynced = await conn.ExecuteScalarAsync<DateTime?>("SELECT MAX(processed_at) FROM processed_events");

        return new LedgerSummaryDto(
            syncedCount,
            syncedTotal,
            unresolvedCount,
            lastSynced.HasValue ? new DateTimeOffset(DateTime.SpecifyKind(lastSynced.Value, DateTimeKind.Utc)) : null
        );
    }

    [McpServerTool, Description("Retrieves failed or mismatched ERP-to-Dynamics invoice sync records routed to the Dead Letter Queue.")]
    public async Task<IReadOnlyList<SyncDiscrepancyDto>> GetUnresolvedDiscrepanciesAsync(
        [Description("Optional error category filter: ValidationFailure, MappingFailure, or ALL")] string category = "ALL",
        CancellationToken ct = default)
    {
        userContext.EnsureRole("FinanceAuditor");
        await auditLogger.LogToolInvocationAsync(userContext, nameof(GetUnresolvedDiscrepanciesAsync), new { category }, ct);

        await using var conn = new NpgsqlConnection(_connectionString);

        const string allSql = """
            SELECT id AS Id, event_id AS EventId, invoice_number AS InvoiceNumber, customer_code AS CustomerCode,
                   amount AS Amount, currency AS Currency, error_category AS ErrorCategory,
                   error_message AS ErrorMessage, resolved AS Resolved, created_at AS CreatedAt
            FROM sync_discrepancies
            WHERE resolved = FALSE
            ORDER BY created_at DESC
            LIMIT 25
            """;

        const string filteredSql = """
            SELECT id AS Id, event_id AS EventId, invoice_number AS InvoiceNumber, customer_code AS CustomerCode,
                   amount AS Amount, currency AS Currency, error_category AS ErrorCategory,
                   error_message AS ErrorMessage, resolved AS Resolved, created_at AS CreatedAt
            FROM sync_discrepancies
            WHERE resolved = FALSE AND error_category = @category
            ORDER BY created_at DESC
            LIMIT 25
            """;

        var sql = category.Equals("ALL", StringComparison.OrdinalIgnoreCase) ? allSql : filteredSql;
        var rows = await conn.QueryAsync<SyncDiscrepancyRow>(sql, new { category });
        return rows.Select(row => new SyncDiscrepancyDto(
            row.Id,
            row.EventId,
            row.InvoiceNumber,
            row.CustomerCode,
            row.Amount,
            row.Currency,
            row.ErrorCategory,
            row.ErrorMessage,
            row.Resolved,
            new DateTimeOffset(DateTime.SpecifyKind(row.CreatedAt, DateTimeKind.Utc))))
            .ToList();
    }
}

internal sealed class SyncDiscrepancyRow
{
    public int Id { get; set; }
    public Guid EventId { get; set; }
    public string InvoiceNumber { get; set; } = string.Empty;
    public string CustomerCode { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
    public string ErrorCategory { get; set; } = string.Empty;
    public string ErrorMessage { get; set; } = string.Empty;
    public bool Resolved { get; set; }
    public DateTime CreatedAt { get; set; }
}
