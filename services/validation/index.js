'use strict'

const { Worker, Queue } = require('bullmq')
const pino = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { service: 'validation' },
})

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }

// Filas de saída
const validQueue = new Queue('leads.valid', { connection })
const discardedQueue = new Queue('leads.discarded', { connection })

// ─── Regras de Validação ──────────────────────────────────────────

function validateEmail(email) {
  if (!email) return { valid: false, clean: null }
  const clean = email.trim().toLowerCase()
  // Regex simples mas funcional: tem @, tem domínio com ponto
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(clean)
  return { valid, clean: valid ? clean : clean }
}

function validatePhone(phone) {
  if (!phone) return { valid: false, clean: null }
  // Remove tudo que não for dígito ou +
  const clean = phone.replace(/[^\d+]/g, '')
  // E.164: + seguido de 7 a 15 dígitos
  const normalized = clean.startsWith('+') ? clean : `+1${clean}` // assume US se sem código
  const valid = /^\+\d{7,15}$/.test(normalized)
  return { valid, clean: normalized }
}

function buildTags(event) {
  const tags = []
  if (event.product_name) tags.push(`buyer_${event.product_name.toLowerCase().replace(/\s+/g, '_')}`)
  if (event.product_niche) tags.push(event.product_niche)
  if (event.quantity) tags.push(`${event.quantity}_bottles`)
  return tags
}

// ─── Worker ───────────────────────────────────────────────────────

const worker = new Worker(
  'events.raw',
  async (job) => {
    const event = job.data

    logger.info({ order_id: event.order_id, correlation_id: event.correlation_id }, 'Processing event')

    // 1. Filtrar apenas aprovados
    const isApproved =
      event.payment_status === 'approved' && event.event === 'order.approved'

    if (!isApproved) {
      logger.info({ order_id: event.order_id, payment_status: event.payment_status }, 'Discarded - not approved')
      await discardedQueue.add('discarded', {
        ...event,
        discard_reason: `payment_status=${event.payment_status} | event=${event.event}`,
        processed_at: new Date().toISOString(),
      })
      return { status: 'discarded' }
    }

    // 2. Validar e limpar campos
    const emailResult = validateEmail(event.customer_email)
    const phoneResult = validatePhone(event.customer_phone)

    // 3. Nome padrão se vazio
    const firstName = event.customer_first_name?.trim() || 'Customer'
    const lastName = event.customer_last_name?.trim() || ''
    const fullName = `${firstName} ${lastName}`.trim()

    // 4. Enriquecer payload
    const enriched = {
      // IDs
      order_id: event.order_id,
      correlation_id: event.correlation_id,

      // Cliente
      email: emailResult.clean,
      email_valid: emailResult.valid,
      full_name: fullName,
      phone: phoneResult.clean,
      phone_valid: phoneResult.valid,
      country: event.customer_country,

      // Produto
      product_name: event.product_name,
      product_niche: event.product_niche,
      order_value: parseFloat(event.price_usd) || 0,
      bottles_qty: parseInt(event.quantity) || 1,

      // Rastreio
      purchase_date: event.created_at,
      funnel_source: event.funnel_source,
      tags: buildTags(event),

      // Auditoria
      processed_at: new Date().toISOString(),
    }

    // 5. Leads com e-mail inválido vão para descartados
    if (!emailResult.valid) {
      logger.warn({ order_id: event.order_id, email: emailResult.clean }, 'Discarded - invalid email')
      await discardedQueue.add('discarded', {
        ...enriched,
        discard_reason: `invalid_email: ${emailResult.clean}`,
      })
      return { status: 'discarded', reason: 'invalid_email' }
    }

    // 6. Lead válido — envia para leads.valid
    await validQueue.add('lead', enriched, {
      jobId: `valid-${event.order_id}`, // idempotência
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 },
    })

    logger.info({ order_id: event.order_id, email: enriched.email }, 'Lead validated and queued')

    return { status: 'valid' }
  },
  {
    connection,
    concurrency: 10, // processa 10 jobs em paralelo
  }
)

worker.on('failed', (job, err) => {
  logger.error({ order_id: job?.data?.order_id, err: err.message }, 'Validation job failed')
})

worker.on('ready', () => {
  logger.info('Validation worker ready — consuming events.raw')
})