'use strict'

const fs   = require('fs')
const path = require('path')

const INPUT_FILE  = path.resolve(__dirname, 'Base de Dados.csv')
const OUTPUT_FILE = path.resolve(__dirname, 'batch_10k.csv')
const TARGET_SIZE = parseInt(process.argv[2]) || 10_000

function parseCSV(content) {
  const lines = content.toString('utf8').replace(/^\uFEFF/, '').split('\n').filter(l => l.trim())
  const headers = lines[0].split(',').map(h => h.trim())
  return lines.slice(1).map(line => {
    const values = line.split(',')
    return Object.fromEntries(headers.map((h, i) => [h, (values[i] || '').trim()]))
  })
}

function stringifyCSV(rows, headers) {
  return [headers.join(','), ...rows.map(r => headers.map(h => r[h] ?? '').join(','))].join('\n')
}

function generateOrderId(counter) {
  return `ORD-2026-${(10000 + counter).toString().padStart(5, '0')}`
}

function generateBatch() {
  const raw     = fs.readFileSync(INPUT_FILE)
  const records = parseCSV(raw)
  if (!records.length) throw new Error('CSV vazio ou inválido')

  const headers = Object.keys(records[0])
  const rows    = []
  let counter   = 0

  while (rows.length < TARGET_SIZE) {
    for (const row of records) {
      if (rows.length >= TARGET_SIZE) break
      rows.push({ ...row, order_id: generateOrderId(counter++) })
    }
  }

  fs.writeFileSync(OUTPUT_FILE, stringifyCSV(rows, headers))
  console.log(`✅ ${rows.length.toLocaleString()} registros → ${OUTPUT_FILE}`)
}

generateBatch()