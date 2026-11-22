CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    invoice_number VARCHAR(64) NOT NULL,
    customer_code VARCHAR(64) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    status VARCHAR(32) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_discrepancies (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    invoice_number VARCHAR(64) NOT NULL,
    customer_code VARCHAR(64) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    error_category VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mcp_audit_logs (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(128) NOT NULL,
    user_role VARCHAR(64) NOT NULL,
    tool_name VARCHAR(128) NOT NULL,
    parameters_json TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
