#!/bin/bash
set -e

for service in ingestion validation delivery persistence observability; do
  if [ -f "services/$service/package.json" ]; then
    npm install --prefix services/$service --silent
  fi
done

if [ -f "scripts/package.json" ]; then
  npm install --prefix scripts --silent
fi