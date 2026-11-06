# CyberFinance 1.1 — PostgreSQL do laboratório (Patroni)

Front + API na mesma porta (**3030**), banco `cyberfinance` no PostgreSQL 17 Patroni pela porta **5000** do HAProxy (sempre o primário).

| Rota | Para quê | Token |
|---|---|---|
| `/` | a aplicação (pasta `public/`) | — |
| `GET /api/health` | saúde: nó primário, versão, uptime (503 se o banco não responde) | aberto |
| `GET /api/state` | estado completo | `API_TOKEN` se definido |
| `POST /api/sync` | grava o estado completo (tudo ou nada, validado, um envio por vez) | `API_TOKEN` se definido |
| `GET /metrics` | métricas Prometheus (`cyberfinance_*`) | aberto |

## Subir
- Normal: `tools\ops\start-apps.ps1` (sobe junto com as outras apps; só se existir `.env`).
- Manual: `npm start` nesta pasta. Teste do banco: `npm run testar`.

## .env (modelo em `.env.example`)
`PORT`, `PG_HOSTS` (3 entradas :5000), `PG_USER`, `PG_PASSWORD` (**entre aspas** se tiver `#`), `PG_DATABASE`, `PG_POOL_MAX`,
`API_TOKEN` (opcional: exige `Authorization: Bearer` em state/sync; na tela, aba PostgreSQL → Chave de Autenticação).

## Monitoramento e inventário
- Prometheus: jobs `cyberfinance` (/metrics) e `cyberfinance-health` (/api/health, módulo blackbox `cyberfinance_health`),
  TCP em "Apps do desktop"; alertas `CyberFinanceFora`, `CyberFinanceBancoFora`, `CyberFinanceSyncComErro`, `CyberFinanceLento`.
- Grafana: dashboard **69 · CyberFinance** — http://192.168.15.161:3000/d/pocerp-cyberfinance (gerado por
  `tools\observability\grafana\gerar_dashboards.py`, função `d_cyberfinance`).
- CMDB: CI "CyberFinance" (app) → depende de "PostgreSQL cyberfinance" → faz parte do "PostgreSQL Patroni (cluster)"; sessão web;
  credencial no cofre (pasta ".env das ferramentas"). Atualizar: `node tools\cmdb-web\scripts\atualiza_cyberfinance.js --aplicar`.

## 1.1 (30/10/2026)
- Servidor: `/metrics`, token opcional, gravação em lote (`unnest`, 121 lançamentos em ~20 ms), validação (400 com a mensagem),
  `/api/health` 503 quando o banco cai, parada limpa, cabeçalhos de segurança; a chave de API do navegador nunca é gravada no banco.
- Front: texto digitado escapado antes de ir para a tela (descrições, cartões, ativos, logs), botão "Unificar" não quebra com aspas no
  nome do cartão, sincronização automática agrupada (1,5 s) com indicador no topo (✓ / ✗), envio manual mostra quantos registros gravou.
- Banco: chave (mês, id) nas tabelas de lançamentos e compras; ids novos sempre únicos.
