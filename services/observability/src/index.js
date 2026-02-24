'use strict'

const Fastify = require('fastify')
const { Queue } = require('bullmq')
const { Pool }  = require('pg')
const pino      = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
  base: { service: 'observability' },
})

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const db = new Pool({ connectionString: process.env.DATABASE_URL })

const queues = {
  'events.raw':       new Queue('events.raw',       { connection }),
  'leads.valid':      new Queue('leads.valid',       { connection }),
  'leads.discarded':  new Queue('leads.discarded',   { connection }),
  'delivery.results': new Queue('delivery.results',  { connection }),
}

const app = Fastify({ logger: false })

app.get('/health', async () => ({ status: 'ok', service: 'observability' }))

app.get('/metrics', async () => {
  const stats = {}
  for (const [name, q] of Object.entries(queues)) {
    stats[name] = await q.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed')
  }
  return { queues: stats, ts: new Date().toISOString() }
})

app.get('/summary', async () => {
  const { rows } = await db.query(`
    SELECT status,
      COUNT(*)                                       AS total,
      COUNT(*) FILTER (WHERE email_valid = false)    AS invalid_email,
      COUNT(*) FILTER (WHERE phone_valid = false)    AS invalid_phone
    FROM lead_control GROUP BY status
  `)
  const summary = {}
  for (const r of rows) {
    summary[r.status] = { total: +r.total, invalid_email: +r.invalid_email, invalid_phone: +r.invalid_phone }
  }
  return { summary, ts: new Date().toISOString() }
})

app.get('/reconciliation', async () => {
  const [qCount, { rows }] = await Promise.all([
    queues['events.raw'].getJobCounts('completed'),
    db.query('SELECT COUNT(*) AS total FROM lead_control'),
  ])
  const db_records = +rows[0].total
  return { raw_queue_completed: qCount.completed, db_records, diff: qCount.completed - db_records, ts: new Date().toISOString() }
})

app.get('/recent-errors', async () => {
  const { rows } = await db.query(`
    SELECT order_id, email, product_name, error_message, processed_at
    FROM lead_control WHERE status = 'erro'
    ORDER BY processed_at DESC LIMIT 50
  `)
  return { errors: rows }
})

const PORT = process.env.PORT || 3005
app.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
  if (err) { logger.error(err); process.exit(1) }
  logger.info({ port: PORT }, 'Observability service started')
})
