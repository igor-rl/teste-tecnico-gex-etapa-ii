-- ─── Tabela de Controle Principal ───────────────────────────────
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

-- Índices para queries de auditoria
CREATE INDEX IF NOT EXISTS idx_lead_control_status ON lead_control(status);
CREATE INDEX IF NOT EXISTS idx_lead_control_processed_at ON lead_control(processed_at);
CREATE INDEX IF NOT EXISTS idx_lead_control_email ON lead_control(email);

-- ─── View de resumo (usada pelo /summary) ────────────────────────
CREATE OR REPLACE VIEW lead_summary AS
SELECT
  status,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE email_valid = false) AS invalid_email_count,
  COUNT(*) FILTER (WHERE phone_valid = false) AS invalid_phone_count,
  MIN(processed_at) AS first_processed,
  MAX(processed_at) AS last_processed
FROM lead_control
GROUP BY status;

-- ─── Queries de Auditoria (Parte A do desafio conceitual) ────────

-- 1. Tempo médio entre a venda e o processamento
-- SELECT AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) AS avg_seconds
-- FROM lead_control;

-- 2. Taxa de erro
-- SELECT
--   ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'erro') / NULLIF(COUNT(*), 0), 2) AS error_rate_pct
-- FROM lead_control;

-- 3. Leads processados nos últimos 30 minutos (detectar parada)
-- SELECT COUNT(*) FROM lead_control
-- WHERE processed_at > NOW() - INTERVAL '30 minutes';

-- 4. Reconciliação: enviados vs total aprovados
-- SELECT
--   COUNT(*) FILTER (WHERE status = 'enviado')  AS enviados,
--   COUNT(*) FILTER (WHERE status = 'erro')     AS com_erro,
--   COUNT(*) FILTER (WHERE status = 'descartado') AS descartados,
--   COUNT(*)                                    AS total
-- FROM lead_control;