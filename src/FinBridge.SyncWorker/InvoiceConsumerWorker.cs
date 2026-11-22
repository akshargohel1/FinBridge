using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using FinBridge.Contracts;
using Npgsql;

namespace FinBridge.SyncWorker;

public sealed class InvoiceConsumerWorker(
    IConfiguration configuration,
    DynamicsSyncService dynamicsService,
    ILogger<InvoiceConsumerWorker> logger) : BackgroundService
{
    private readonly string _bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
    private readonly string _groupId = configuration["Kafka:GroupId"] ?? "finbridge-dynamics-sync-group";
    private readonly string _connectionString = configuration.GetConnectionString("LedgerDb")!;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(KafkaTopics.InvoicesCreated);
        logger.LogInformation("FinBridge SyncWorker listening on topic '{Topic}'...", KafkaTopics.InvoicesCreated);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null) continue;

                var evt = JsonSerializer.Deserialize<InvoiceCreatedEvent>(result.Message.Value);
                if (evt is not null)
                {
                    await ProcessEventAsync(evt, dlqProducer, stoppingToken);
                }

                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                logger.LogWarning("Kafka consume warning (waiting for topic creation): {Reason}", ex.Error.Reason);
                await Task.Delay(2000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ProcessEventAsync(
        InvoiceCreatedEvent evt,
        IProducer<string, string> dlqProducer,
        CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);

        var alreadyProcessed = await conn.ExecuteScalarAsync<bool>(
            "SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = @EventId)",
            new { evt.EventId });

        if (alreadyProcessed)
        {
            logger.LogWarning("[IDEMPOTENCY] Duplicate EventId {EventId} ({InvoiceNumber}) skipped safely.", evt.EventId, evt.InvoiceNumber);
            return;
        }

        try
        {
            await dynamicsService.SyncToMicrosoftDynamicsAsync(evt, ct);

            const string insertSql = """
                INSERT INTO processed_events (event_id, invoice_number, customer_code, amount, currency, status)
                VALUES (@EventId, @InvoiceNumber, @CustomerCode, @Amount, @Currency, 'Synced')
                """;
            await conn.ExecuteAsync(insertSql, evt);

            logger.LogInformation("[SYNCED] Invoice {InvoiceNumber} ({Amount} {Currency}) synced to Microsoft Dynamics.",
                evt.InvoiceNumber, evt.Amount, evt.Currency);
        }
        catch (Exception ex)
        {
            var category = ex.Message.Contains(':') ? ex.Message.Split(':')[0] : "TransientError";
            logger.LogError("[DLQ ROUTE] Invoice {InvoiceNumber} failed ({Category}): {Message}",
                evt.InvoiceNumber, category, ex.Message);

            var envelope = new DeadLetterEnvelope(evt, category, ex.Message, DateTimeOffset.UtcNow);
            await dlqProducer.ProduceAsync(
                KafkaTopics.InvoicesDeadLetter,
                new Message<string, string>
                {
                    Key = evt.InvoiceNumber,
                    Value = JsonSerializer.Serialize(envelope)
                }, ct);

            const string discrepancySql = """
                INSERT INTO sync_discrepancies (event_id, invoice_number, customer_code, amount, currency, error_category, error_message)
                VALUES (@EventId, @InvoiceNumber, @CustomerCode, @Amount, @Currency, @Category, @Message)
                """;
            await conn.ExecuteAsync(
                discrepancySql,
                new
                {
                    evt.EventId,
                    evt.InvoiceNumber,
                    evt.CustomerCode,
                    evt.Amount,
                    evt.Currency,
                    Category = category,
                    Message = ex.Message
                });
        }
    }
}
