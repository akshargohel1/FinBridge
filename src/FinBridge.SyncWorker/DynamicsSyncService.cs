using FinBridge.Contracts;

namespace FinBridge.SyncWorker;

public sealed class DynamicsSyncService
{
    private static readonly HashSet<string> ValidCustomers = ["CUST-DE-1001", "CUST-DE-1002", "CUST-SE-2001"];

    public Task SyncToMicrosoftDynamicsAsync(InvoiceCreatedEvent evt, CancellationToken ct)
    {
        if (evt.Amount <= 0)
        {
            throw new InvalidOperationException($"ValidationFailure: Invoice {evt.InvoiceNumber} has non-positive amount ({evt.Amount}).");
        }

        if (!ValidCustomers.Contains(evt.CustomerCode))
        {
            throw new InvalidOperationException($"MappingFailure: CustomerCode '{evt.CustomerCode}' does not exist in Microsoft Dynamics master data.");
        }

        return Task.Delay(50, ct);
    }
}
