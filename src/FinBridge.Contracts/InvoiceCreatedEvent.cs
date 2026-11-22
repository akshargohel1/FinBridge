namespace FinBridge.Contracts;

public sealed record InvoiceCreatedEvent(
    Guid EventId,
    string InvoiceNumber,
    string CustomerCode,
    decimal Amount,
    string Currency,
    DateTimeOffset OccurredAt
);

public sealed record DeadLetterEnvelope(
    InvoiceCreatedEvent OriginalEvent,
    string ErrorCategory,
    string ErrorMessage,
    DateTimeOffset FailedAt
);

public sealed record SyncDiscrepancyDto(
    int Id,
    Guid EventId,
    string InvoiceNumber,
    string CustomerCode,
    decimal Amount,
    string Currency,
    string ErrorCategory,
    string ErrorMessage,
    bool Resolved,
    DateTimeOffset CreatedAt
);

public sealed record LedgerSummaryDto(
    int TotalSyncedCount,
    decimal TotalSyncedAmountEur,
    int UnresolvedDiscrepancyCount,
    DateTimeOffset? LastSyncedAt
);
