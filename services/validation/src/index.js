'use strict'

const { Worker, Queue } = require('bullmq')
const pino = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
  base: { service: 'validation' },
})

const connection  = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const validQueue  = new Queue('leads.valid',     { connection })
const discardedQueue = new Queue('leads.discarded', { connection })

// ─── Validações ───────────────────────────────────────────────────

function validateEmail(email) {
  if (!email) return { valid: false, clean: null }
  const clean = email.trim().toLowerCase()
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(clean)
  return { valid, clean }
}

function validatePhone(phone) {
  if (!phone || !phone.trim()) return { valid: false, clean: null }
  const clean = phone.replace(/[^\d+]/g, '')
  const normalized = clean.startsWith('+') ? clean : `+1${clean}`
  const valid = /^\+\d{7,15}$/.test(normalized)
  return { valid, clean: normalized }
}

function buildTags(event) {
  const tags = []
  if (event.product_name) tags.push(`buyer_${event.product_name.toLowerCase().replace(/\s+/g, '_')}`)
  if (event.product_niche) tags.push(event.product_niche)
  if (event.quantity)      tags.push(`${event.quantity}_bottles`)
  return tags
}

// ─── Worker ───────────────────────────────────────────────────────

const worker = new Worker('events.raw', async (job) => {
  const event = job.data
  logger.info({ order_id: event.order_id }, 'Processing event')

  // 1. Filtrar apenas aprovados
  const isApproved = event.payment_status === 'approved' && event.event === 'order.approved'
  if (!isApproved) {
    await discardedQueue.add('discarded', {
      ...event,
      discard_reason: `payment_status=${event.payment_status} | event=${event.event}`,
      processed_at: new Date().toISOString(),
    })
    logger.info({ order_id: event.order_id }, 'Discarded — not approved')
    return { status: 'discarded' }
  }

  // 2. Validar campos
  const emailResult = validateEmail(event.customer_email)
  const phoneResult = validatePhone(event.customer_phone)
  const firstName   = event.customer_first_name?.trim() || 'Customer'
  const lastName    = event.customer_last_name?.trim()  || ''

  const enriched = {
    order_id:      event.order_id,
    correlation_id: event.correlation_id,
    email:         emailResult.clean,
    email_valid:   emailResult.valid,
    full_name:     `${firstName} ${lastName}`.trim(),
    phone:         phoneResult.clean,
    phone_valid:   phoneResult.valid,
    country:       event.customer_country,
    product_name:  event.product_name,
    product_niche: event.product_niche,
    order_value:   parseFloat(event.price_usd) || 0,
    bottles_qty:   parseInt(event.quantity)    || 1,
    purchase_date: event.created_at,
    funnel_source: event.funnel_source,
    tags:          buildTags(event),
    processed_at:  new Date().toISOString(),
  }

  // 3. Email inválido → descartado
  if (!emailResult.valid) {
    await discardedQueue.add('discarded', { ...enriched, discard_reason: `invalid_email: ${emailResult.clean}` })
    logger.warn({ order_id: event.order_id, email: emailResult.clean }, 'Discarded — invalid email')
    return { status: 'discarded', reason: 'invalid_email' }
  }

  // 4. Válido → fila de entrega
  await validQueue.add('lead', enriched, {
    jobId: `valid-${event.order_id}`,
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
  })

  logger.info({ order_id: event.order_id, email: enriched.email }, 'Lead validated')
  return { status: 'valid' }

}, { connection, concurrency: 10 })

worker.on('failed', (job, err) => logger.error({ order_id: job?.data?.order_id, err: err.message }, 'Job failed'))
worker.on('ready',  () => logger.info('Validation worker ready'))
