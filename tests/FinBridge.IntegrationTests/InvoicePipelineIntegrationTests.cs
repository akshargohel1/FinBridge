using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using FinBridge.Contracts;
using FinBridge.McpServer;
using FinBridge.SyncWorker;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Xunit;

namespace FinBridge.IntegrationTests;

public sealed class InvoicePipelineIntegrationTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("apache/kafka:3.9.0").Build();
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("finbridge_test")
        .WithUsername("finbridge")
        .WithPassword("integration-test-password")
        .Build();

    private IHost? _workerHost;
    private IConfiguration _configuration = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await _kafka.StartAsync();

        var connectionString = _postgres.GetConnectionString();
        var initScript = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "init.sql"));
        await _postgres.ExecScriptAsync(initScript);

        var settings = new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = _kafka.GetBootstrapAddress(),
            ["Kafka:GroupId"] = $"finbridge-integration-{Guid.NewGuid():N}",
            ["ConnectionStrings:LedgerDb"] = connectionString
        };

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.AddSingleton<DynamicsSyncService>();
        builder.Services.AddHostedService<InvoiceConsumerWorker>();

        _configuration = builder.Configuration;
        _workerHost = builder.Build();
        await _workerHost.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_workerHost is not null)
        {
            await _workerHost.StopAsync();
            _workerHost.Dispose();
        }

        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task WorkerAndMcpTools_HandleInvoiceOutcomesAndAuditQueries()
    {
        var duplicateEventId = Guid.NewGuid();
        var events = new[]
        {
            CreateInvoice(duplicateEventId, "INV-VALID", "CUST-DE-1001", 125m),
            CreateInvoice(duplicateEventId, "INV-VALID", "CUST-DE-1001", 125m),
            CreateInvoice(Guid.NewGuid(), "INV-NEGATIVE", "CUST-DE-1001", -10m),
            CreateInvoice(Guid.NewGuid(), "INV-UNKNOWN", "CUST-UNKNOWN", 50m)
        };

        await PublishAsync(events);
        await WaitForPipelineAsync(expectedProcessed: 1, expectedDiscrepancies: 2);

        var deadLetters = await ReadDeadLettersAsync(expectedCount: 2);
        Assert.Equal(
            new[] { "MappingFailure", "ValidationFailure" },
            deadLetters.Select(message => message.ErrorCategory).OrderBy(category => category).ToArray());

        var accessor = new HttpContextAccessor { HttpContext = new DefaultHttpContext() };
        accessor.HttpContext.Request.Headers["X-Demo-User"] = "integration-test-user";
        accessor.HttpContext.Request.Headers["X-Demo-Role"] = "FinanceAuditor";

        var auditLogger = new AuditLogger(_configuration);
        var tools = new FinanceReconciliationTools(_configuration, new UserContext(accessor), auditLogger);

        var summary = await tools.GetPipelineHealthSummaryAsync(CancellationToken.None);
        Assert.Equal(1, summary.TotalSyncedCount);
        Assert.Equal(125m, summary.TotalSyncedAmountEur);
        Assert.Equal(2, summary.UnresolvedDiscrepancyCount);
        Assert.NotNull(summary.LastSyncedAt);

        var validationDiscrepancies = await tools.GetUnresolvedDiscrepanciesAsync(
            "ValidationFailure",
            CancellationToken.None);
        Assert.Single(validationDiscrepancies);
        Assert.Equal("INV-NEGATIVE", validationDiscrepancies[0].InvoiceNumber);

        accessor.HttpContext.Request.Headers["X-Demo-Role"] = "FinanceOperator";
        var unauthorized = new FinanceReconciliationTools(
            _configuration,
            new UserContext(accessor),
            auditLogger);
        await Assert.ThrowsAsync<UnauthorizedAccessException>(
            () => unauthorized.GetPipelineHealthSummaryAsync(CancellationToken.None));

        Assert.Equal(2, await GetAuditLogCountAsync());
    }

    private async Task PublishAsync(IEnumerable<InvoiceCreatedEvent> events)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        foreach (var invoice in events)
        {
            await producer.ProduceAsync(
                KafkaTopics.InvoicesCreated,
                new Message<string, string>
                {
                    Key = "pipeline-integration",
                    Value = JsonSerializer.Serialize(invoice)
                });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private async Task WaitForPipelineAsync(int expectedProcessed, int expectedDiscrepancies)
    {
        var timeout = Stopwatch.StartNew();
        while (timeout.Elapsed < TimeSpan.FromSeconds(30))
        {
            await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
            await connection.OpenAsync();

            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT
                    (SELECT COUNT(*) FROM processed_events),
                    (SELECT COUNT(*) FROM sync_discrepancies)
                """;

            await using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync()
                && reader.GetInt64(0) == expectedProcessed
                && reader.GetInt64(1) == expectedDiscrepancies)
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException(
            $"The worker did not record {expectedProcessed} processed invoice(s) and "
            + $"{expectedDiscrepancies} discrepancy record(s) within 30 seconds.");
    }

    private async Task<IReadOnlyList<DeadLetterEnvelope>> ReadDeadLettersAsync(int expectedCount)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"finbridge-dlq-test-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(KafkaTopics.InvoicesDeadLetter);

        var messages = new List<DeadLetterEnvelope>();
        var timeout = Stopwatch.StartNew();
        while (messages.Count < expectedCount && timeout.Elapsed < TimeSpan.FromSeconds(30))
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is not null)
            {
                var envelope = JsonSerializer.Deserialize<DeadLetterEnvelope>(result.Message.Value);
                if (envelope is not null)
                {
                    messages.Add(envelope);
                }
            }
        }

        Assert.Equal(expectedCount, messages.Count);
        return messages;
    }

    private async Task<int> GetAuditLogCountAsync()
    {
        await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM mcp_audit_logs";
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static InvoiceCreatedEvent CreateInvoice(
        Guid eventId,
        string invoiceNumber,
        string customerCode,
        decimal amount)
    {
        return new InvoiceCreatedEvent(
            eventId,
            invoiceNumber,
            customerCode,
            amount,
            "EUR",
            DateTimeOffset.UtcNow);
    }
}
