'use strict'

const { Worker, Queue } = require('bullmq')
const { fetch } = require('undici')
const pino = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
  base: { service: 'delivery' },
})

const connection  = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://webhook.site/test'
const resultsQueue = new Queue('delivery.results', { connection })

// ─── Envio com timeout ────────────────────────────────────────────

async function sendToWebhook(payload) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 10_000)
  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`)
    return { success: true, status: res.status }
  } finally {
    clearTimeout(timer)
  }
}

// ─── Worker ───────────────────────────────────────────────────────

const worker = new Worker('leads.valid', async (job) => {
  const lead = job.data
  logger.info({ order_id: lead.order_id, attempt: job.attemptsMade + 1 }, 'Delivering lead')

  let result

  try {
    await sendToWebhook({
      order_id:      lead.order_id,
      email:         lead.email,
      full_name:     lead.full_name,
      phone:         lead.phone,
      country:       lead.country,
      product_name:  lead.product_name,
      product_niche: lead.product_niche,
      order_value:   lead.order_value,
      bottles_qty:   lead.bottles_qty,
      purchase_date: lead.purchase_date,
      funnel_source: lead.funnel_source,
      tags:          lead.tags,
    })

    result = { ...pick(lead), status: 'enviado', error_message: null, processed_at: new Date().toISOString() }
    logger.info({ order_id: lead.order_id }, 'Delivered')

  } catch (err) {
    const isFinal = job.attemptsMade >= (job.opts.attempts ?? 1) - 1
    result = { ...pick(lead), status: isFinal ? 'erro' : 'retrying', error_message: err.message, processed_at: new Date().toISOString() }
    logger.error({ order_id: lead.order_id, attempt: job.attemptsMade + 1, err: err.message }, 'Delivery failed')
    if (!isFinal) throw err
  }

  await resultsQueue.add('result', result)
  return result

}, {
  connection,
  concurrency: 5,
  limiter: { max: 10, duration: 1000 },
})

function pick(lead) {
  return {
    order_id:       lead.order_id,
    correlation_id: lead.correlation_id,
    email:          lead.email,
    phone:          lead.phone,
    product_name:   lead.product_name,
    email_valid:    lead.email_valid,
    phone_valid:    lead.phone_valid,
  }
}

worker.on('failed', (job, err) => logger.error({ order_id: job?.data?.order_id }, 'Job failed after all attempts'))
worker.on('ready',  () => logger.info('Delivery worker ready'))
