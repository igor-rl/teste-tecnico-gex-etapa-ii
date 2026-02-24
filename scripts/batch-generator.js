'use strict'

/**
 * generate-batch.js
 * Lê o CSV original e replica os registros até TARGET_SIZE,
 * substituindo apenas o order_id em cada linha.
 *
 * Uso:
 *   node scripts/generate-batch.js          → 10k registros
 *   node scripts/generate-batch.js 50000    → 50k registros
 */

const fs   = require('fs')
const path = require('path')
const { parse }     = require('csv-parse/sync')
const { stringify } = require('csv-stringify/sync')

const INPUT_FILE  = path.resolve(__dirname, './Base de Dados.csv')
const OUTPUT_FILE = path.resolve(__dirname, './batch_10k.csv')
const TARGET_SIZE = parseInt(process.argv[2]) || 10_000

function generateOrderId(counter) {
  return `ORD-2026-${(10000 + counter).toString().padStart(5, '0')}`
}

function generateBatch() {
  const raw     = fs.readFileSync(INPUT_FILE)
  const records = parse(raw, { columns: true, bom: true, skip_empty_lines: true, trim: true })

  if (!records.length) throw new Error('CSV vazio ou inválido')

  const rows  = []
  let counter = 0

  while (rows.length < TARGET_SIZE) {
    for (const row of records) {
      if (rows.length >= TARGET_SIZE) break
      rows.push({ ...row, order_id: generateOrderId(counter++) })
    }
  }

  fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true })
  fs.writeFileSync(OUTPUT_FILE, stringify(rows, { header: true, columns: Object.keys(records[0]) }))

  console.log(`✅ ${rows.length.toLocaleString()} registros → ${OUTPUT_FILE}`)
}

generateBatch()