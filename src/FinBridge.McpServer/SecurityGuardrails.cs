using System.Text.Json;
using Dapper;
using Npgsql;

namespace FinBridge.McpServer;

public sealed class UserContext(IHttpContextAccessor accessor)
{
    public string UserId => accessor.HttpContext?.Request.Headers["X-Demo-User"].FirstOrDefault() ?? "local-dev-user";
    public string Role => accessor.HttpContext?.Request.Headers["X-Demo-Role"].FirstOrDefault() ?? "FinanceAuditor";

    public void EnsureRole(string requiredRole)
    {
        if (!string.Equals(Role, requiredRole, StringComparison.OrdinalIgnoreCase))
        {
            throw new UnauthorizedAccessException($"Access Denied: Role '{requiredRole}' is required to invoke this MCP financial tool.");
        }
    }
}

public sealed class AuditLogger(IConfiguration configuration)
{
    private readonly string _connectionString = configuration.GetConnectionString("LedgerDb")!;

    public async Task LogToolInvocationAsync(UserContext user, string toolName, object parameters, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        const string sql = """
            INSERT INTO mcp_audit_logs (user_id, user_role, tool_name, parameters_json)
            VALUES (@UserId, @Role, @ToolName, @ParamsJson)
            """;
        await conn.ExecuteAsync(
            sql,
            new
            {
                user.UserId,
                user.Role,
                ToolName = toolName,
                ParamsJson = JsonSerializer.Serialize(parameters)
            });
    }
}
