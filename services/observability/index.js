'use strict'

const Fastify = require('fastify')
const { Queue, QueueEvents } = require('bullmq')
const { Pool } = require('pg')
const pino = require('pino')

const logger = pino({ level: 'info', base: { service: 'observability' } })

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const db = new Pool({ connectionString: process.env.DATABASE_URL })

const queues = {
  'events.raw': new Queue('events.raw', { connection }),
  'leads.valid': new Queue('leads.valid', { connection }),
  'leads.discarded': new Queue('leads.discarded', { connection }),
  'delivery.results': new Queue('delivery.results', { connection }),
}

const app = Fastify({ logger: false })

// ─── GET /health ─────────────────────────────────────────────────
app.get('/health', async () => ({ status: 'ok', service: 'observability' }))

// ─── GET /metrics ─────────────────────────────────────────────────
app.get('/metrics', async () => {
  const queueStats = {}

  for (const [name, queue] of Object.entries(queues)) {
    const counts = await queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed')
    queueStats[name] = counts
  }

  return { queues: queueStats, ts: new Date().toISOString() }
})

// ─── GET /summary ─────────────────────────────────────────────────
// Responde: quantos enviados, com erro, descartados
app.get('/summary', async () => {
  const result = await db.query(`
    SELECT
      status,
      COUNT(*)            AS total,
      COUNT(*) FILTER (WHERE email_valid = false) AS invalid_email,
      COUNT(*) FILTER (WHERE phone_valid = false) AS invalid_phone
    FROM lead_control
    GROUP BY status
  `)

  const rows = result.rows
  const summary = {}
  for (const row of rows) {
    summary[row.status] = {
      total: parseInt(row.total),
      invalid_email: parseInt(row.invalid_email),
      invalid_phone: parseInt(row.invalid_phone),
    }
  }

  return { summary, ts: new Date().toISOString() }
})

// ─── GET /reconciliation ─────────────────────────────────────────
// Compara total de eventos recebidos vs gravados
app.get('/reconciliation', async () => {
  const [rawCount, dbCount] = await Promise.all([
    queues['events.raw'].getJobCounts('completed'),
    db.query('SELECT COUNT(*) AS total FROM lead_control'),
  ])

  return {
    raw_queue_completed: rawCount.completed,
    db_records: parseInt(dbCount.rows[0].total),
    diff: rawCount.completed - parseInt(dbCount.rows[0].total),
    ts: new Date().toISOString(),
  }
})

// ─── GET /recent-errors ──────────────────────────────────────────
app.get('/recent-errors', async () => {
  const result = await db.query(`
    SELECT order_id, email, product_name, error_message, processed_at
    FROM lead_control
    WHERE status = 'erro'
    ORDER BY processed_at DESC
    LIMIT 50
  `)
  return { errors: result.rows }
})

// ─── Start ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3005

app.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
  if (err) {
    logger.error(err)
    process.exit(1)
  }
  logger.info({ port: PORT }, 'Observability service started')
})