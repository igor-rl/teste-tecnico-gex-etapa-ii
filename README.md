teste-tecnico/
├── docker-compose.yml
├── services/
│   ├── ingestion/
│   ├── validation/
│   ├── delivery/
│   ├── persistence/
│   └── observability/
├── shared/
│   └── queue/
└── infra/
    └── init.sql

# Banco direto
psql $DATABASE_URL

# Redis direto
redis-cli -u $REDIS_URL

# Testar serviços
curl http://ingestion:3001/health
curl http://observability:3005/summary

# Gerar batch
node scripts/generate-batch.js

# Enviar pro pipeline
curl -X POST http://ingestion:3001/batch \
  -H "Content-Type: text/plain" \
  --data-binary @infra/csv-base/batch_10k.csv