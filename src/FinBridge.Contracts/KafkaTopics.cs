namespace FinBridge.Contracts;

public static class KafkaTopics
{
    public const string InvoicesCreated = "finance.invoices.created";
    public const string InvoicesDeadLetter = "finance.invoices.dlq";
}
