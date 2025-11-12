#!/bin/bash

echo "🌐 URLs dos Serviços no Codespace:"
echo ""
echo "📊 Grafana:        https://${CODESPACE_NAME}-3000.app.github.dev"
echo "🐘 PostgreSQL:     localhost:5432 (use DBeaver/pgAdmin)"
echo "🔴 Redis:          localhost:6379"
echo "📦 MinIO API:      https://${CODESPACE_NAME}-9000.app.github.dev"
echo "🌐 MinIO Console:  https://${CODESPACE_NAME}-9001.app.github.dev"
echo ""
echo "💡 Acesse Grafana (login: admin/admin):"
echo "https://${CODESPACE_NAME}-3000.app.github.dev"