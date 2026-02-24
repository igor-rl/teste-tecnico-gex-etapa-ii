CREATE TABLE IF NOT EXISTS lead_control (
  id              SERIAL PRIMARY KEY,
  order_id        VARCHAR(100) UNIQUE NOT NULL,
  correlation_id  UUID,
  email           VARCHAR(255),
  phone           VARCHAR(50),
  product_name    VARCHAR(255),
  status          VARCHAR(20) NOT NULL CHECK (status IN ('enviado', 'erro', 'descartado', 'retrying')),
  error_message   TEXT,
  email_valid     BOOLEAN DEFAULT FALSE,
  phone_valid     BOOLEAN DEFAULT FALSE,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lead_status       ON lead_control(status);
CREATE INDEX IF NOT EXISTS idx_lead_processed_at ON lead_control(processed_at);
CREATE INDEX IF NOT EXISTS idx_lead_email        ON lead_control(email);

CREATE OR REPLACE VIEW lead_summary AS
SELECT
  status,
  COUNT(*)                                    AS total,
  COUNT(*) FILTER (WHERE email_valid = false) AS invalid_email_count,
  COUNT(*) FILTER (WHERE phone_valid = false) AS invalid_phone_count,
  MIN(processed_at)                           AS first_processed,
  MAX(processed_at)                           AS last_processed
FROM lead_control
GROUP BY status;
