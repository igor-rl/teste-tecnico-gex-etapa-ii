'use strict'
const Fastify = require('fastify')
const multipart = require('@fastify/multipart')
const { Queue } = require('bullmq')
const { parse } = require('csv-parse/sync')
const { v4: uuidv4 } = require('uuid')
const logger = require('./logger')

const connection = { url: process.env.REDIS_URL || 'redis://localhost:6379' }
const rawQueue = new Queue('events.raw', { connection })

async function start() {
  const app = Fastify({ 
    logger: false,
    bodyLimit: 52428800
  })

  await app.register(multipart)

  app.post('/events', async (req, reply) => {
    const event = req.body
    if (!event?.order_id) return reply.status(400).send({ error: 'order_id obrigatório' })

    const correlation_id = uuidv4()
    await rawQueue.add('event',
      { ...event, correlation_id, received_at: new Date().toISOString() },
      { jobId: `${event.order_id}-${correlation_id}`, attempts: 3, backoff: { type: 'exponential', delay: 1000 } }
    )

    logger.info({ order_id: event.order_id, correlation_id }, 'Event ingested')
    return reply.status(202).send({ correlation_id, status: 'queued' })
  })

  app.post('/batch', async (req, reply) => {
    let csvContent

    const contentType = req.headers['content-type'] || ''

    if (contentType.includes('multipart/form-data')) {
      const data = await req.file()
      csvContent = (await data.toBuffer()).toString()
    } else {
      csvContent = req.body?.toString()
    }

    if (!csvContent) return reply.status(400).send({ error: 'CSV obrigatório' })

    let records
    try {
      records = parse(csvContent, { columns: true, skip_empty_lines: true, trim: true, bom: true })
    } catch (err) {
      return reply.status(400).send({ error: 'CSV inválido', detail: err.message })
    }

    const jobs = records.map((record) => {
      const correlation_id = uuidv4()
      return {
        name: 'event',
        data: { ...record, correlation_id, received_at: new Date().toISOString() },
        opts: { jobId: `${record.order_id}-${correlation_id}`, attempts: 3, backoff: { type: 'exponential', delay: 1000 } },
      }
    })

    await rawQueue.addBulk(jobs)
    logger.info({ count: jobs.length }, 'Batch ingested')
    return reply.status(202).send({ queued: jobs.length })
  })

  app.get('/health', async () => ({ status: 'ok', service: 'ingestion', ts: new Date().toISOString() }))

  const PORT = process.env.PORT || 3001
  await app.listen({ port: PORT, host: '0.0.0.0' })
  logger.info({ port: PORT }, 'Ingestion service started')
}

start().catch((err) => {
  console.error(err)
  process.exit(1)
})