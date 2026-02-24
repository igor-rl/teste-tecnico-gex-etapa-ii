'use strict'

const { Worker } = require('bullmq')
const { Pool }   = require('pg')
const pino       = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
  base: { service: 'persistence' },
})

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const db = new Pool({ connectionString: process.env.DATABASE_URL })

// ─── Upsert ───────────────────────────────────────────────────────

async function upsertLead(data) {
  await db.query(`
    INSERT INTO lead_control
      (order_id, correlation_id, email, phone, product_name, status, error_message, email_valid, phone_valid, processed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (order_id) DO UPDATE SET
      status        = EXCLUDED.status,
      error_message = EXCLUDED.error_message,
      processed_at  = EXCLUDED.processed_at
  `, [
    data.order_id,
    data.correlation_id,
    data.email,
    data.phone,
    data.product_name,
    data.status,
    data.error_message || null,
    data.email_valid  ?? false,
    data.phone_valid  ?? false,
    data.processed_at || new Date().toISOString(),
  ])
}

// ─── Workers ─────────────────────────────────────────────────────

const deliveryWorker = new Worker('delivery.results', async (job) => {
  await upsertLead(job.data)
  logger.info({ order_id: job.data.order_id, status: job.data.status }, 'Persisted')
}, { connection, concurrency: 20 })

const discardedWorker = new Worker('leads.discarded', async (job) => {
  const lead = job.data
  await upsertLead({
    order_id:       lead.order_id,
    correlation_id: lead.correlation_id,
    email:          lead.email         || lead.customer_email,
    phone:          lead.phone         || lead.customer_phone,
    product_name:   lead.product_name,
    status:         'descartado',
    error_message:  lead.discard_reason,
    email_valid:    lead.email_valid   ?? false,
    phone_valid:    lead.phone_valid   ?? false,
    processed_at:   lead.processed_at  || new Date().toISOString(),
  })
  logger.info({ order_id: lead.order_id }, 'Discarded persisted')
}, { connection, concurrency: 20 })

deliveryWorker.on('failed',  (job, err) => logger.error({ order_id: job?.data?.order_id, err: err.message }, 'Persistence failed'))
deliveryWorker.on('ready',   () => logger.info('Persistence worker ready — delivery.results'))
discardedWorker.on('ready',  () => logger.info('Persistence worker ready — leads.discarded'))
