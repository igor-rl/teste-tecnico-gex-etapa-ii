'use strict'

const { Worker, Queue } = require('bullmq')
const { fetch } = require('undici')
const pino = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { service: 'delivery' },
})

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://webhook.site/test'

// Fila de resultados (consumida pelo Persistence)
const resultsQueue = new Queue('delivery.results', { connection })

// ─── Função de envio com timeout ─────────────────────────────────

async function sendToWebhook(payload) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 10_000) // 10s timeout

  try {
    const response = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    return { success: true, status: response.status }
  } finally {
    clearTimeout(timeout)
  }
}

// ─── Worker ───────────────────────────────────────────────────────

const worker = new Worker(
  'leads.valid',
  async (job) => {
    const lead = job.data

    logger.info({ order_id: lead.order_id, attempt: job.attemptsMade + 1 }, 'Delivering lead')

    let deliveryResult

    try {
      // Monta payload exato conforme spec do teste
      const payload = {
        order_id: lead.order_id,
        email: lead.email,
        full_name: lead.full_name,
        phone: lead.phone,
        country: lead.country,
        product_name: lead.product_name,
        product_niche: lead.product_niche,
        order_value: lead.order_value,
        bottles_qty: lead.bottles_qty,
        purchase_date: lead.purchase_date,
        funnel_source: lead.funnel_source,
        tags: lead.tags,
      }

      const result = await sendToWebhook(payload)

      deliveryResult = {
        order_id: lead.order_id,
        correlation_id: lead.correlation_id,
        email: lead.email,
        phone: lead.phone,
        product_name: lead.product_name,
        email_valid: lead.email_valid,
        phone_valid: lead.phone_valid,
        status: 'enviado',
        error_message: null,
        processed_at: new Date().toISOString(),
      }

      logger.info({ order_id: lead.order_id }, 'Lead delivered successfully')
    } catch (err) {
      const isFinal = job.attemptsMade >= job.opts.attempts - 1

      deliveryResult = {
        order_id: lead.order_id,
        correlation_id: lead.correlation_id,
        email: lead.email,
        phone: lead.phone,
        product_name: lead.product_name,
        email_valid: lead.email_valid,
        phone_valid: lead.phone_valid,
        status: isFinal ? 'erro' : 'retrying',
        error_message: err.message,
        processed_at: new Date().toISOString(),
      }

      logger.error({ order_id: lead.order_id, error: err.message, attempt: job.attemptsMade + 1 }, 'Delivery failed')

      // Se ainda vai retry, não publica resultado final ainda
      if (!isFinal) throw err
    }

    // Publica resultado para o Persistence gravar
    await resultsQueue.add('result', deliveryResult)

    return deliveryResult
  },
  {
    connection,
    concurrency: 5,       // controla volume de envio
    limiter: {
      max: 10,            // máximo 10 jobs por intervalo
      duration: 1000,     // por segundo (rate limiting)
    },
  }
)

// Leads que foram para DLQ (esgotaram retries)
worker.on('failed', async (job, err) => {
  if (job && job.attemptsMade >= job.opts.attempts) {
    logger.error({ order_id: job.data?.order_id }, 'Lead moved to DLQ after max attempts')
  }
})

worker.on('ready', () => {
  logger.info('Delivery worker ready — consuming leads.valid')
})