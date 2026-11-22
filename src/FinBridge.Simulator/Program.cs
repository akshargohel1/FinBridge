using System.Text.Json;
using Confluent.Kafka;
using FinBridge.Contracts;

var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();

var duplicateEventId = Guid.NewGuid();

var sampleEvents = new List<InvoiceCreatedEvent>
{
    new(duplicateEventId, "INV-2026-001", "CUST-DE-1001", 4250.00m, "EUR", DateTimeOffset.UtcNow),
    new(Guid.NewGuid(),   "INV-2026-002", "CUST-DE-1002", 1890.50m, "EUR", DateTimeOffset.UtcNow),
    new(duplicateEventId, "INV-2026-001", "CUST-DE-1001", 4250.00m, "EUR", DateTimeOffset.UtcNow),
    new(Guid.NewGuid(),   "INV-2026-003", "CUST-SE-2001", -150.00m, "EUR", DateTimeOffset.UtcNow),
    new(Guid.NewGuid(),   "INV-2026-004", "CUST-UNKNOWN-99", 7400.00m, "EUR", DateTimeOffset.UtcNow)
};

Console.WriteLine("Publishing 5 Legacy ERP Invoice Events to Kafka...\n");

foreach (var evt in sampleEvents)
{
    var payload = JsonSerializer.Serialize(evt);
    await producer.ProduceAsync(
        KafkaTopics.InvoicesCreated,
        new Message<string, string> { Key = evt.InvoiceNumber, Value = payload });

    Console.WriteLine($" -> Sent {evt.InvoiceNumber} | Customer: {evt.CustomerCode} | Amount: {evt.Amount} {evt.Currency} | EventId: {evt.EventId}");
}

producer.Flush(TimeSpan.FromSeconds(5));
Console.WriteLine("\nDone! Check your SyncWorker terminal, Kafka UI (http://localhost:8080), or Claude MCP tools.");
