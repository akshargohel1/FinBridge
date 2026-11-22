# FinBridge: Governed Event-Driven & Agentic AI Middleware (.NET 10 + Kafka + MCP)

**FinBridge** is a complete, runnable enterprise reference architecture demonstrating how legacy ERP systems synchronize financial records with **Microsoft Dynamics** via **Apache Kafka**, while exposing zero-trust diagnostic tools to **Claude AI** through a custom **C# Model Context Protocol (MCP) Server**.

## Architecture Overview

```text
[FinBridge.Simulator (Legacy ERP)]
        │  1. Publishes InvoiceCreatedEvent
        ▼
  [Apache Kafka]  ──(Poison / Failed Events)──▶ [finance.invoices.dlq]
        │
        ▼  2. Idempotent .NET 10 Consumer validates & syncs
[FinBridge.SyncWorker] ──▶ [Mock Microsoft Dynamics API]
        │
        ▼  3. Persists ledger state, idempotency keys & discrepancies
[PostgreSQL Ledger]
        ▲
        │  4. Read-only, RBAC-secured MCP Tools + Audit Trail
[FinBridge.McpServer (.NET 10)] ◀──(HTTP / JSON-RPC)──▶ [Claude Desktop / AI Client]
```

## Solution Projects
1. **`FinBridge.Contracts`**: Shared event schemas (`InvoiceCreatedEvent`), DTOs, and Kafka topic constants.
2. **`FinBridge.SyncWorker`**: Background Kafka consumer implementing **idempotency checks**, business rule validation, simulated Microsoft Dynamics API synchronization, and **Dead Letter Queue (DLQ)** routing.
3. **`FinBridge.McpServer`**: ASP.NET Core MCP Server built with the official `ModelContextProtocol.AspNetCore` SDK, enforcing **RBAC role checks** (`FinanceAuditor`) and **PostgreSQL audit logging** on every AI tool invocation.
4. **`FinBridge.Simulator`**: CLI tool that seeds realistic ERP invoice events—including intentional edge cases (negative amounts, unknown customer codes, and duplicate event IDs)—to test resilience end-to-end.

## Quick Start in VS Code

### Prerequisites
- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### Step 1: Start Kafka, Kafka UI, and PostgreSQL
```bash
docker-compose up -d
```
- **Kafka UI** is available at `http://localhost:8080`
- **PostgreSQL** automatically initializes all ledger and audit tables via `init.sql`.

### Step 2: Build the Solution
```bash
dotnet build FinBridge.sln
```

### Run the Tests
```bash
dotnet test FinBridge.sln
```
The unit tests run without external services. Integration tests start disposable Kafka and PostgreSQL containers and require Docker Desktop.

### Step 3: Run the Kafka Sync Worker (Terminal 1)
```bash
dotnet run --project src/FinBridge.SyncWorker
```

### Step 4: Run the C# MCP Server (Terminal 2)
```bash
dotnet run --project src/FinBridge.McpServer
```
The MCP endpoint runs at `http://localhost:5050/mcp`.

### Step 5: Publish Sample ERP Events (Terminal 3)
```bash
dotnet run --project src/FinBridge.Simulator
```
Watch Terminal 1 process valid invoices into the ledger, skip duplicate events via idempotency keys, and route invalid records to `finance.invoices.dlq`.

### Step 6: Connect Claude Desktop to Your C# MCP Server
Add this to your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "finbridge-reconciliation": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "http://localhost:5050/mcp",
        "--header",
        "X-Demo-User: akshar.gohel",
        "--header",
        "X-Demo-Role: FinanceAuditor"
      ]
    }
  }
}
```
Ask Claude:
> *"Check our finance sync pipeline for any failed or mismatched invoices, explain why they failed, and summarize the recent audit log."*
