// CyberFinance - API + front na mesma porta, PostgreSQL 17 do lab (Patroni) pela porta 5000 do HAProxy.
// A porta 5000 sempre leva ao primario; a lista de hosts so serve para tolerar um no fora do ar.
//   GET  /api/health  saude (aberto: usado pelo Prometheus/CMDB)      GET /metrics  metricas Prometheus (aberto, so leitura)
//   GET  /api/state   estado completo                                  POST /api/sync estado completo -> banco (tudo ou nada)
// API_TOKEN no .env (opcional) exige "Authorization: Bearer <token>" em /api/state e /api/sync (campo "Chave" da aba PostgreSQL).
require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const path = require('path');
const { Pool, types } = require('pg');
const prom = require('prom-client');
const { version: VERSAO } = require('./package.json');

// DATE volta como texto 'AAAA-MM-DD' (sem conversao de fuso que pode voltar um dia)
types.setTypeParser(1082, (v) => v);

const PG_HOSTS = (process.env.PG_HOSTS || '192.168.88.28:5000,192.168.88.29:5000,192.168.88.31:5000').split(',').map((h) => h.trim()).filter(Boolean);
const PG_USER = process.env.PG_USER || 'cyberfinance_app';
const PG_PASSWORD = process.env.PG_PASSWORD || '';
const PG_DATABASE = process.env.PG_DATABASE || 'cyberfinance';
const PG_POOL_MAX = parseInt(process.env.PG_POOL_MAX || '15', 10);
const API_TOKEN = process.env.API_TOKEN || '';
const MONTHS = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];

// ---------------------------------------------------------------- metricas (Prometheus)
const reg = new prom.Registry();
prom.collectDefaultMetrics({ register: reg, prefix: 'cyberfinance_' });
const mReq = new prom.Counter({ name: 'cyberfinance_http_requests_total', help: 'Requisicoes HTTP por rota e status', labelNames: ['method', 'route', 'status'], registers: [reg] });
const mLat = new prom.Histogram({ name: 'cyberfinance_http_request_duration_seconds', help: 'Duracao das requisicoes HTTP', labelNames: ['method', 'route'],
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5], registers: [reg] });
const mSync = new prom.Counter({ name: 'cyberfinance_sync_total', help: 'Sincronizacoes do estado (ok/erro)', labelNames: ['resultado'], registers: [reg] });
const mSyncDur = new prom.Histogram({ name: 'cyberfinance_sync_duration_seconds', help: 'Duracao da gravacao do estado no banco', buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5], registers: [reg] });
const mTroca = new prom.Counter({ name: 'cyberfinance_db_failover_total', help: 'Trocas de no/entrada do PostgreSQL', registers: [reg] });
const mDbUp = new prom.Gauge({ name: 'cyberfinance_db_up', help: '1 = primario do PostgreSQL respondeu na ultima coleta', registers: [reg] });
const mDbLat = new prom.Gauge({ name: 'cyberfinance_db_ping_seconds', help: 'Ida e volta SELECT 1 no primario', registers: [reg] });
const mPool = new prom.Gauge({ name: 'cyberfinance_db_pool_connections', help: 'Conexoes do pool por estado', labelNames: ['estado'], registers: [reg] });
const mLinhas = new prom.Gauge({ name: 'cyberfinance_rows', help: 'Linhas por tabela', labelNames: ['tabela'], registers: [reg] });
const mInfo = new prom.Gauge({ name: 'cyberfinance_info', help: 'Versao e entrada do PostgreSQL em uso', labelNames: ['versao', 'entrada', 'no'], registers: [reg] });

// ---------------------------------------------------------------- PostgreSQL (primario via HAProxy :5000)
let activePool = null;
let currentHostConnected = null;
let noAtual = null;
let proximo = 0; // proximo host a tentar (gira na lista quando um cai)

// erros que indicam conexao perdida ou no que deixou de ser primario -> trocar de host e repetir 1 vez
const ERRO_DE_CONEXAO = /ECONNREFUSED|ECONNRESET|ETIMEDOUT|EHOSTUNREACH|ENOTFOUND|Connection terminated|timeout exceeded/i;
const SQLSTATE_TROCA = new Set(['57P01', '57P02', '57P03', '25006', '08000', '08003', '08006']);
const deveTrocar = (err) => SQLSTATE_TROCA.has(err.code) || ERRO_DE_CONEXAO.test(String(err.message));

async function descartaPool() {
  const p = activePool; activePool = null; currentHostConnected = null; noAtual = null;
  if (p) await p.end().catch(() => {});
}

async function getPrimaryPool() {
  if (activePool) return activePool;
  for (let i = 0; i < PG_HOSTS.length; i++) {
    const entrada = PG_HOSTS[(proximo + i) % PG_HOSTS.length];
    const [host, portStr] = entrada.split(':');
    const port = parseInt(portStr || '5000', 10);
    const pool = new Pool({ host, port, user: PG_USER, password: PG_PASSWORD, database: PG_DATABASE, ssl: false,
      max: PG_POOL_MAX, idleTimeoutMillis: 30000, connectionTimeoutMillis: 3000, application_name: `cyberfinance ${VERSAO}` });
    pool.on('error', (err) => console.warn(`⚠️ Conexão ociosa perdida (${entrada}): ${err.message}`));
    try {
      const r = (await pool.query('SELECT host(inet_server_addr()) AS node_ip, pg_is_in_recovery() AS in_recovery, version() AS pg_version')).rows[0];
      if (r.in_recovery) { await pool.end(); continue; }
      activePool = pool; currentHostConnected = `${host}:${port}`; noAtual = r.node_ip; proximo = (proximo + i) % PG_HOSTS.length;
      mInfo.reset(); mInfo.set({ versao: VERSAO, entrada: currentHostConnected, no: noAtual }, 1);
      console.log(`✅ Primário via ${currentHostConnected} (nó real ${r.node_ip}) | ${r.pg_version.split(',')[0]}`);
      return activePool;
    } catch (err) {
      console.warn(`❌ ${entrada} indisponível: ${err.message}`);
      await pool.end().catch(() => {});
    }
  }
  proximo = (proximo + 1) % PG_HOSTS.length;
  throw new Error('Nenhum primário disponível em PG_HOSTS (porta 5000).');
}

// executa fn(client) com um cliente do pool; se a conexao caiu, troca de host e tenta mais 1 vez
async function comCliente(fn) {
  for (let tentativa = 1; ; tentativa++) {
    const pool = await getPrimaryPool();
    let client;
    try {
      client = await pool.connect();
      return await fn(client);
    } catch (err) {
      if (tentativa < 2 && deveTrocar(err)) {
        console.warn(`⚠️ ${currentHostConnected}: ${err.message} - trocando de nó`);
        if (client) { client.release(true); client = null; }
        mTroca.inc();
        proximo = (proximo + 1) % PG_HOSTS.length;
        await descartaPool();
        continue;
      }
      throw err;
    } finally {
      if (client) client.release();
    }
  }
}

// transacao: BEGIN/COMMIT, ROLLBACK em qualquer erro
const emTransacao = (fn) => comCliente(async (c) => {
  await c.query('BEGIN');
  try { const r = await fn(c); await c.query('COMMIT'); return r; } catch (err) { await c.query('ROLLBACK').catch(() => {}); throw err; }
});

// ---------------------------------------------------------------- migrations (rodam com o proprio cyberfinance_app)
async function runMigrations() {
  await emTransacao((c) => c.query(`
    CREATE TABLE IF NOT EXISTS fin_lancamentos (
        id BIGINT NOT NULL,
        mes_ref VARCHAR(3) NOT NULL CHECK (mes_ref IN ('Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez')),
        ano_ref INT NOT NULL DEFAULT 2026,
        descricao VARCHAR(255) NOT NULL,
        categoria VARCHAR(100) NOT NULL,
        tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('Receita', 'Despesa', 'Investimento')),
        valor NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
        data_vencimento DATE NOT NULL,
        status VARCHAR(20) NOT NULL CHECK (status IN ('Pago', 'Pendente')),
        cartao_unificado VARCHAR(100),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mes_ref, id)
    );
    CREATE INDEX IF NOT EXISTS idx_fin_lanc_mes ON fin_lancamentos(mes_ref, status);
    CREATE INDEX IF NOT EXISTS idx_fin_lanc_venc ON fin_lancamentos(data_vencimento);

    CREATE TABLE IF NOT EXISTS fin_cartoes_compras (
        id BIGINT NOT NULL,
        mes_ref VARCHAR(3) NOT NULL CHECK (mes_ref IN ('Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez')),
        cartao VARCHAR(100) NOT NULL,
        item VARCHAR(255) NOT NULL,
        categoria VARCHAR(100) NOT NULL,
        valor_parcela NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
        parcela_atual INT NOT NULL DEFAULT 1,
        total_parcelas INT NOT NULL DEFAULT 1,
        parcelas_faltantes INT GENERATED ALWAYS AS (GREATEST(0, total_parcelas - parcela_atual)) STORED,
        saldo_futuro_restante NUMERIC(14, 2) GENERATED ALWAYS AS (GREATEST(0, total_parcelas - parcela_atual) * valor_parcela) STORED,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mes_ref, id)
    );
    CREATE INDEX IF NOT EXISTS idx_fin_cc_mes_cartao ON fin_cartoes_compras(mes_ref, cartao);

    -- o front identifica cada item pelo id DENTRO do mes (os dados de exemplo repetem ids entre meses):
    -- tabelas criadas antes com PK so em id passam para PK (mes_ref, id)
    DO $$
    DECLARE t text;
    BEGIN
      FOREACH t IN ARRAY ARRAY['fin_lancamentos', 'fin_cartoes_compras'] LOOP
        IF (SELECT array_length(conkey, 1) FROM pg_constraint WHERE conrelid = t::regclass AND contype = 'p') = 1 THEN
          EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I, ADD PRIMARY KEY (mes_ref, id)', t, t || '_pkey');
        END IF;
      END LOOP;
    END $$;

    CREATE TABLE IF NOT EXISTS fin_investimentos (
        id SERIAL PRIMARY KEY,
        ativo VARCHAR(150) NOT NULL,
        classe VARCHAR(100) NOT NULL,
        valor_aplicado NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
        saldo_atual NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
        renda_passiva_mes NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS fin_configuracoes (
        chave VARCHAR(50) PRIMARY KEY,
        email_destino VARCHAR(150) NOT NULL DEFAULT 'jcnogueira.oliveira@gmail.com',
        dias_alerta INT NOT NULL DEFAULT 3,
        payload_json JSONB,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `));
  console.log('🚀 Migrations concluídas pelo usuário', PG_USER);
}

// ---------------------------------------------------------------- validacao do estado enviado pelo front
class ErroValidacao extends Error {}
const DATA_RE = /^\d{4}-\d{2}-\d{2}$/;
const texto = (v, max, campo) => {
  const s = String(v ?? '').trim();
  if (!s) throw new ErroValidacao(`${campo} vazio`);
  return s.slice(0, max);
};
const numero = (v, campo) => {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new ErroValidacao(`${campo} não é número: ${v}`);
  return n;
};
const inteiro = (v, campo) => Math.trunc(numero(v, campo));

function validaEstado(b) {
  if (!b || typeof b !== 'object') throw new ErroValidacao('corpo vazio');
  const lanc = [], cc = [], inv = [];
  for (const [mes, lista] of Object.entries(b.lancamentosPorMes || {})) {
    if (!MONTHS.includes(mes)) throw new ErroValidacao(`mês inválido: ${mes}`);
    for (const it of lista || []) {
      const venc = String(it.venc || '');
      if (!DATA_RE.test(venc)) throw new ErroValidacao(`vencimento inválido em ${mes}: ${venc}`);
      if (!['Receita', 'Despesa', 'Investimento'].includes(it.tipo)) throw new ErroValidacao(`tipo inválido em ${mes}: ${it.tipo}`);
      if (!['Pago', 'Pendente'].includes(it.status)) throw new ErroValidacao(`status inválido em ${mes}: ${it.status}`);
      lanc.push([inteiro(it.id, 'id'), mes, texto(it.desc, 255, 'descrição'), texto(it.cat, 100, 'categoria'), it.tipo, numero(it.valor, 'valor'),
        venc, it.status, it.cartaoUnificado ? String(it.cartaoUnificado).slice(0, 100) : null]);
    }
  }
  for (const [mes, lista] of Object.entries(b.comprasCartaoPorMes || {})) {
    if (!MONTHS.includes(mes)) throw new ErroValidacao(`mês inválido: ${mes}`);
    for (const c of lista || []) {
      cc.push([inteiro(c.id, 'id'), mes, texto(c.cartao, 100, 'cartão'), texto(c.item, 255, 'item'), texto(c.cat, 100, 'categoria'),
        numero(c.valorParcela, 'valor da parcela'), Math.max(1, inteiro(c.parcAtual, 'parcela atual')), Math.max(1, inteiro(c.parcTotal, 'total de parcelas'))]);
    }
  }
  for (const i of b.investimentos || []) {
    inv.push([texto(i.ativo, 150, 'ativo'), texto(i.classe, 100, 'classe'), numero(i.aplicado, 'aplicado'), numero(i.atual, 'saldo atual'), numero(i.rendaMes, 'renda/mês')]);
  }
  return { lanc, cc, inv, config: b.config || null, cartoesConfig: b.cartoesConfig || null };
}

// colunas -> arrays para um INSERT ... SELECT FROM unnest(...) (uma ida ao banco por tabela)
const colunas = (linhas, n) => Array.from({ length: n }, (_, j) => linhas.map((l) => l[j]));

// ---------------------------------------------------------------- app
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '10mb' }));

// metricas por rota (rota "normalizada" para nao explodir a cardinalidade)
app.use((req, res, next) => {
  const fim = mLat.startTimer();
  res.on('finish', () => {
    const route = req.path.startsWith('/api/') ? req.path : req.path === '/metrics' ? '/metrics' : 'estatico';
    mReq.inc({ method: req.method, route, status: res.statusCode });
    fim({ method: req.method, route });
  });
  next();
});

// cabecalhos basicos de seguranca (a pagina usa CDNs: Tailwind, Chart.js, EmailJS, Google Fonts)
app.use((req, res, next) => {
  res.set({ 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'same-origin' });
  next();
});

// token opcional para ler/gravar dados (health e metrics ficam abertos para o monitoramento)
const exigeToken = (req, res, next) => {
  if (!API_TOKEN) return next();
  const m = /^Bearer\s+(.+)$/i.exec(req.get('authorization') || '');
  const ok = m && m[1].length === API_TOKEN.length && crypto.timingSafeEqual(Buffer.from(m[1]), Buffer.from(API_TOKEN));
  if (!ok) return res.status(401).json({ error: 'token inválido ou ausente (aba PostgreSQL → Chave de Autenticação)' });
  next();
};

// so a pasta public/ e publicada (server.js, package.json e .env ficam fora)
app.use(express.static(path.join(__dirname, 'public')));

// 1. Healthcheck: 200 com o no primario; 503 se o banco nao responde
app.get('/api/health', async (req, res) => {
  try {
    const r = await comCliente((c) => c.query(`SELECT host(inet_server_addr()) AS primary_node_ip, inet_server_port() AS internal_port,
      current_database() AS db_name, current_user AS db_user, pg_is_in_recovery() AS in_recovery, version() AS pg_version`));
    res.json({ status: 'ok', versao: VERSAO, uptime_s: Math.round(process.uptime()), entrypoint: currentHostConnected,
      auth: API_TOKEN ? 'token' : 'aberta', patroni_primary: r.rows[0] });
  } catch (err) {
    res.status(503).json({ status: 'error', versao: VERSAO, message: err.message });
  }
});

// 2. Estado completo
app.get('/api/state', exigeToken, async (req, res) => {
  try {
    const [lanc, cc, inv, cfg] = await comCliente(async (c) => [
      (await c.query('SELECT * FROM fin_lancamentos ORDER BY data_vencimento ASC, id')).rows,
      (await c.query('SELECT * FROM fin_cartoes_compras ORDER BY mes_ref, id')).rows,
      (await c.query('SELECT * FROM fin_investimentos ORDER BY id ASC')).rows,
      (await c.query("SELECT payload_json FROM fin_configuracoes WHERE chave = 'default'")).rows,
    ]);
    const lancamentosPorMes = Object.fromEntries(MONTHS.map((m) => [m, []]));
    const comprasCartaoPorMes = Object.fromEntries(MONTHS.map((m) => [m, []]));
    for (const r of lanc) {
      (lancamentosPorMes[r.mes_ref] ||= []).push({ id: Number(r.id), desc: r.descricao, cat: r.categoria, tipo: r.tipo, valor: parseFloat(r.valor),
        venc: r.data_vencimento, status: r.status, cartaoUnificado: r.cartao_unificado || undefined });
    }
    for (const r of cc) {
      (comprasCartaoPorMes[r.mes_ref] ||= []).push({ id: Number(r.id), cartao: r.cartao, item: r.item, cat: r.categoria,
        valorParcela: parseFloat(r.valor_parcela), parcAtual: r.parcela_atual, parcTotal: r.total_parcelas });
    }
    const investimentos = inv.map((r) => ({ ativo: r.ativo, classe: r.classe, aplicado: parseFloat(r.valor_aplicado),
      atual: parseFloat(r.saldo_atual), rendaMes: parseFloat(r.renda_passiva_mes) }));
    const saved = (cfg[0] && cfg[0].payload_json) || {};
    res.json({
      config: saved.config || { email: 'jcnogueira.oliveira@gmail.com', diasAlerta: 3 },
      cartoesConfig: saved.cartoesConfig || [
        { nome: 'Cartão Interempresas', diaVenc: 15, limite: 15000 },
        { nome: 'Nubank Ultravioleta', diaVenc: 10, limite: 20000 },
      ],
      investimentos, lancamentosPorMes, comprasCartaoPorMes, logs: [],
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Sincroniza o estado completo no primario (tudo ou nada; uma ida ao banco por tabela)
app.post('/api/sync', exigeToken, async (req, res) => {
  const fim = mSyncDur.startTimer();
  try {
    const e = validaEstado(req.body);
    // nunca gravar chaves de API (EmailJS / token) no banco: ficam so no navegador
    const cfg = e.config ? { ...e.config, pgApiKey: undefined } : null;
    const n = await emTransacao(async (c) => {
      // um sync por vez: dois envios simultaneos (sync automatico) apagariam/inseririam por cima um do outro
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))', ['cyberfinance_sync']);
      await c.query(
        `INSERT INTO fin_configuracoes (chave, email_destino, dias_alerta, payload_json, updated_at)
         VALUES ('default', $1, $2, $3, NOW())
         ON CONFLICT (chave) DO UPDATE SET email_destino = EXCLUDED.email_destino, dias_alerta = EXCLUDED.dias_alerta,
           payload_json = EXCLUDED.payload_json, updated_at = NOW()`,
        [cfg?.email || 'jcnogueira.oliveira@gmail.com', cfg?.diasAlerta || 3, JSON.stringify({ config: cfg, cartoesConfig: e.cartoesConfig })]);

      await c.query('DELETE FROM fin_lancamentos');
      if (e.lanc.length) {
        await c.query(`INSERT INTO fin_lancamentos (id, mes_ref, descricao, categoria, tipo, valor, data_vencimento, status, cartao_unificado)
                       SELECT * FROM unnest($1::bigint[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[], $6::numeric[], $7::date[], $8::varchar[], $9::varchar[])`,
          colunas(e.lanc, 9));
      }
      await c.query('DELETE FROM fin_cartoes_compras');
      if (e.cc.length) {
        await c.query(`INSERT INTO fin_cartoes_compras (id, mes_ref, cartao, item, categoria, valor_parcela, parcela_atual, total_parcelas)
                       SELECT * FROM unnest($1::bigint[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[], $6::numeric[], $7::int[], $8::int[])`,
          colunas(e.cc, 8));
      }
      await c.query('DELETE FROM fin_investimentos');
      if (e.inv.length) {
        await c.query(`INSERT INTO fin_investimentos (ativo, classe, valor_aplicado, saldo_atual, renda_passiva_mes)
                       SELECT * FROM unnest($1::varchar[], $2::varchar[], $3::numeric[], $4::numeric[], $5::numeric[])`, colunas(e.inv, 5));
      }
      return { lancamentos: e.lanc.length, compras: e.cc.length, investimentos: e.inv.length };
    });
    mSync.inc({ resultado: 'ok' });
    res.json({ ok: true, primary_entrypoint: currentHostConnected, gravados: n });
  } catch (err) {
    mSync.inc({ resultado: 'erro' });
    // 23505 = chave duplicada (mesmo id duas vezes no mesmo mes)
    const status = err instanceof ErroValidacao || err.code === '23505' || err.code === '23514' ? 400 : 500;
    res.status(status).json({ error: err.message });
  } finally {
    fim();
  }
});

// 4. Metricas Prometheus (estado do banco medido na hora da coleta)
app.get('/metrics', async (req, res) => {
  try {
    const t0 = process.hrtime.bigint();
    const r = await comCliente((c) => c.query(`SELECT (SELECT count(*) FROM fin_lancamentos) l, (SELECT count(*) FROM fin_cartoes_compras) c,
      (SELECT count(*) FROM fin_investimentos) i`));
    mDbLat.set(Number(process.hrtime.bigint() - t0) / 1e9);
    mDbUp.set(1);
    const x = r.rows[0];
    mLinhas.set({ tabela: 'fin_lancamentos' }, Number(x.l)); mLinhas.set({ tabela: 'fin_cartoes_compras' }, Number(x.c)); mLinhas.set({ tabela: 'fin_investimentos' }, Number(x.i));
  } catch (err) {
    mDbUp.set(0);
  }
  if (activePool) {
    mPool.set({ estado: 'total' }, activePool.totalCount); mPool.set({ estado: 'ociosa' }, activePool.idleCount); mPool.set({ estado: 'esperando' }, activePool.waitingCount);
  }
  res.set('Content-Type', reg.contentType);
  res.end(await reg.metrics());
});

const PORT = process.env.PORT || 3030;
const servidor = app.listen(PORT, async () => {
  console.log(`⚡ CyberFinance ${VERSAO} em http://localhost:${PORT} (API ${API_TOKEN ? 'com token' : 'aberta'})`);
  try { await runMigrations(); } catch (err) { console.warn('⚠️ Banco ainda não inicializado:', err.message); }
});

// parada limpa: termina as requisicoes em andamento e fecha o pool
for (const sinal of ['SIGINT', 'SIGTERM']) {
  process.on(sinal, () => {
    console.log(`⏹️ ${sinal}: encerrando`);
    servidor.close(async () => { await descartaPool(); process.exit(0); });
    setTimeout(() => process.exit(0), 5000).unref();
  });
}
