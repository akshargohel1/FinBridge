using FinBridge.Contracts;
using FinBridge.SyncWorker;
using Xunit;

namespace FinBridge.UnitTests;

public sealed class DynamicsSyncServiceTests
{
    private readonly DynamicsSyncService _service = new();

    [Fact]
    public async Task SyncToMicrosoftDynamicsAsync_AcceptsPositiveInvoiceForKnownCustomer()
    {
        var invoice = CreateInvoice(amount: 125m, customerCode: "CUST-DE-1001");

        await _service.SyncToMicrosoftDynamicsAsync(invoice, CancellationToken.None);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task SyncToMicrosoftDynamicsAsync_RejectsNonPositiveAmount(decimal amount)
    {
        var invoice = CreateInvoice(amount, customerCode: "CUST-DE-1001");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _service.SyncToMicrosoftDynamicsAsync(invoice, CancellationToken.None));

        Assert.StartsWith("ValidationFailure:", exception.Message);
    }

    [Fact]
    public async Task SyncToMicrosoftDynamicsAsync_RejectsUnknownCustomer()
    {
        var invoice = CreateInvoice(amount: 125m, customerCode: "CUST-UNKNOWN");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _service.SyncToMicrosoftDynamicsAsync(invoice, CancellationToken.None));

        Assert.StartsWith("MappingFailure:", exception.Message);
    }

    private static InvoiceCreatedEvent CreateInvoice(decimal amount, string customerCode)
    {
        return new InvoiceCreatedEvent(
            Guid.NewGuid(),
            "INV-TEST-001",
            customerCode,
            amount,
            "EUR",
            DateTimeOffset.UtcNow);
    }
}
