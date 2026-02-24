# Sales Pipeline — GEX Teste Técnico

Pipeline de processamento de eventos de vendas com 5 microserviços Node.js desacoplados via BullMQ/Redis.

## Estrutura

```
├── .devcontainer/
│   ├── devcontainer.json
│   ├── docker-compose.yaml
│   ├── Dockerfile.dev
│   └── setup.sh
├── services/
│   ├── ingestion/      → HTTP :3001 — recebe eventos e CSV
│   ├── validation/     → Worker — valida, enriquece e roteia
│   ├── delivery/       → Worker — envia para webhook externo
│   ├── persistence/    → Worker — grava no PostgreSQL
│   └── observability/  → HTTP :3005 — métricas e auditoria
├── scripts/
│   ├── generate-batch.js
│   └── Base de Dados.csv   ← coloque o CSV original aqui
├── infra/
│   └── init.sql
├── data/               ← criado automaticamente (git ignored)
│   ├── redis/
│   └── postgres/
└── docker-compose.yml
```

## Setup

```bash
# 1. Copie e configure o webhook
cp .env.example .env
# Edite .env com sua URL do https://webhook.site

# 2. Coloque o CSV original em scripts/Base de Dados.csv

# 3. Suba tudo
docker compose up --build
```

## Gerando o batch de 10k

```bash
# Dentro do devcontainer ou localmente
cd scripts/generate-batch && npm install
node generate-batch.js          # → 10k
node generate-batch.js 50000    # → 50k
```

## Enviando eventos

```bash
# Real-time (1 evento)
curl -X POST http://localhost:3001/events \
  -H "Content-Type: application/json" \
  -d '{"order_id":"ORD-TEST-001","event":"order.approved","payment_status":"approved",...}'

# Batch (CSV)
curl -X POST http://localhost:3001/batch \
  -H "Content-Type: text/plain" \
  --data-binary @scripts/batch_10k.csv
```

## Observabilidade

| Endpoint | Descrição |
|---|---|
| `GET :3005/health` | Health check |
| `GET :3005/metrics` | Status das filas |
| `GET :3005/summary` | Totais por status |
| `GET :3005/reconciliation` | Fila vs banco |
| `GET :3005/recent-errors` | Últimos 50 erros |

## Fluxo

```
POST /events ou /batch
        ↓
   [Ingestion] → events.raw
        ↓
   [Validation] → leads.valid / leads.discarded
        ↓
   [Delivery] → webhook.site → delivery.results
        ↓
   [Persistence] → PostgreSQL (lead_control)
        ↑
   [Observability] lê banco + filas
```
