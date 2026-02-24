'use strict'

const Fastify = require('fastify')
const { Queue } = require('bullmq')
const { parse } = require('csv-parse/sync')
const { v4: uuidv4 } = require('uuid')
const fs = require('fs')
const path = require('path')
const logger = require('./logger')

// ─── Fila de saída ────────────────────────────────────────────────
const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const rawQueue = new Queue('events.raw', { connection })

// ─── Fastify ──────────────────────────────────────────────────────
const app = Fastify({ logger: false })

/**
 * POST /events
 * Recebe um único evento JSON (simulação real-time)
 */
app.post('/events', async (req, reply) => {
  const event = req.body

  if (!event || !event.order_id) {
    return reply.status(400).send({ error: 'order_id obrigatório' })
  }

  const correlation_id = uuidv4()
  const payload = { ...event, correlation_id, received_at: new Date().toISOString() }

  await rawQueue.add('event', payload, {
    jobId: `${event.order_id}-${correlation_id}`, // evita duplicatas
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
  })

  logger.info({ order_id: event.order_id, correlation_id }, 'Event ingested')

  return reply.status(202).send({ correlation_id, status: 'queued' })
})

/**
 * POST /batch
 * Recebe upload de CSV e processa em lote
 */
app.post('/batch', { config: { rawBody: true } }, async (req, reply) => {
  const csvContent = req.body?.toString()

  if (!csvContent) {
    return reply.status(400).send({ error: 'CSV body obrigatório' })
  }

  let records
  try {
    records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    })
  } catch (err) {
    return reply.status(400).send({ error: 'CSV inválido', detail: err.message })
  }

  const jobs = records.map((record) => {
    const correlation_id = uuidv4()
    return {
      name: 'event',
      data: { ...record, correlation_id, received_at: new Date().toISOString() },
      opts: {
        jobId: `${record.order_id}-${correlation_id}`,
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
      },
    }
  })

  await rawQueue.addBulk(jobs)

  logger.info({ count: jobs.length }, 'Batch ingested')

  return reply.status(202).send({ queued: jobs.length })
})

/**
 * GET /health
 */
app.get('/health', async () => {
  return { status: 'ok', service: 'ingestion', ts: new Date().toISOString() }
})

// ─── Start ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001

app.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
  if (err) {
    logger.error(err)
    process.exit(1)
  }
  logger.info({ port: PORT }, 'Ingestion service started')
})