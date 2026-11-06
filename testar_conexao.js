// Teste de login + escrita pela porta 5000 em cada endereço do PG_HOSTS (substitui o psql, que não existe nesta máquina).
// Uso: npm run testar   (lê o .env; não imprime a senha)
require('dotenv').config();
const { Client } = require('pg');

(async () => {
  const hosts = (process.env.PG_HOSTS || '').split(',').map((h) => h.trim()).filter(Boolean);
  let falhas = 0;
  for (const h of hosts) {
    const [host, port] = h.split(':');
    const c = new Client({ host, port: Number(port || 5000), user: process.env.PG_USER, password: process.env.PG_PASSWORD,
      database: process.env.PG_DATABASE, ssl: false, connectionTimeoutMillis: 3000 });
    try {
      await c.connect();
      const r = (await c.query('SELECT host(inet_server_addr()) AS no, pg_is_in_recovery() AS replica, current_user AS usuario')).rows[0];
      await c.query('CREATE TEMP TABLE t_teste (x int); INSERT INTO t_teste VALUES (1)');   // escrita sem deixar lixo
      console.log(`OK   ${h} -> nó ${r.no} | réplica=${r.replica} | usuário ${r.usuario} | escrita ok`);
    } catch (e) {
      falhas++; console.log(`FALHA ${h}: ${e.message}`);
    } finally {
      await c.end().catch(() => {});
    }
  }
  process.exit(falhas ? 1 : 0);
})();
