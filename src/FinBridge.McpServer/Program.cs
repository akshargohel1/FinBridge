using FinBridge.McpServer;
using ModelContextProtocol.Server;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<UserContext>();
builder.Services.AddScoped<AuditLogger>();

builder.Services.AddMcpServer()
    .WithHttpTransport()
    .WithToolsFromAssembly();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    Service = "FinBridge MCP Server (.NET 10)",
    McpEndpoint = "/mcp",
    Status = "Healthy"
}));

app.MapMcp("/mcp");

app.Run();
