const express = require('express');
const sql = require('mssql');
const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const envFile = fs.existsSync(path.join(__dirname, '.env')) ? dotenv.parse(fs.readFileSync(path.join(__dirname, '.env'))) : {};
const cors = require('cors');

const app = express();
app.use(cors());

// Logging middleware para depuración rápida
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

const PORT = process.env.PORT || 3000;

const DRY_RUN = (process.env.DRY_RUN || 'false').toLowerCase() === 'true';

// Sample data for DryRun mode (small, deterministic)
const sampleData = (() => {
  const products = ['Producto A','Producto B','Producto C','Producto D','Producto E','Producto F'];
  const dates = [];
  const today = new Date();
  for (let i=29;i>=0;i--) {
    const d = new Date(today);
    d.setDate(d.getDate()-i);
    dates.push(d.toISOString().slice(0,10));
  }
  const byProductDate = [];
  products.forEach((p, pi) => {
    dates.forEach((dt, di) => {
      // generate counts with simple pattern
      const base = 20 + (pi*5);
      const variance = Math.round(5 * Math.sin((di+pi)/5) + Math.random()*3);
      byProductDate.push({ Fecha: dt, ProductoNombre: p, TotalOpiniones: Math.max(0, base + variance) });
    });
  });
  const pctByProduct = products.map((p, i) => ({ ProductoNombre: p, PctSatisfechos: 0.5 + (i*0.05), TotalOpiniones: byProductDate.filter(r=>r.ProductoNombre===p).reduce((s,r)=>s+r.TotalOpiniones,0) }));
  const trend = dates.map((dt, idx) => ({ Fecha: dt, PctSatisfechos: 0.5 + 0.1*Math.sin(idx/6) }));
  const totalComments = { TotalComentariosProcesados: byProductDate.reduce((s,r)=>s+r.TotalOpiniones,0) };
  return { byProductDate, pctByProduct, trend, totalComments };
})();

const getDbConfig = () => {
  // Priority: DB_CONNECTION_STRING -> SQL auth -> Windows auth (msnodesqlv8)
  if (process.env.DB_CONNECTION_STRING) {
    return process.env.DB_CONNECTION_STRING;
  }
  // Allow users to specify server as 'host,port' in DB_SERVER; prefer explicit DB_PORT otherwise
  let server = (process.env.DB_SERVER || 'localhost').toString();
  let port = process.env.DB_PORT ? parseInt(process.env.DB_PORT, 10) : undefined;
  if (server.includes(',')) {
    const parts = server.split(',');
    server = parts[0].trim();
    if (!port && parts[1]) {
      const p = parseInt(parts[1].trim(), 10);
      if (!isNaN(p)) port = p;
    }
  }
  port = port || 1433;
  const database = process.env.DB_DATABASE || 'DWOpinionClientes';
  const user = envFile.DB_USER || process.env.DB_USER || '';
  const password = envFile.DB_PASSWORD || process.env.DB_PASSWORD || '';

  if (user) {
    return {
      user,
      password,
      server,
      port,
      database,
      options: { encrypt: false, trustServerCertificate: true }
    };
  }
  // If no SQL user provided and USE_WINDOWS_AUTH is true, attempt msnodesqlv8 connection string
  const useWindows = (envFile.USE_WINDOWS_AUTH || process.env.USE_WINDOWS_AUTH || 'false').toLowerCase() === 'true';
  if (useWindows) {
    // Prefer native msnodesqlv8 when available for Windows Integrated Auth.
    try {
      require.resolve('msnodesqlv8');
      return {
        server,
        port,
        database,
        driver: 'msnodesqlv8',
        options: { trustedConnection: true }
      };
    } catch (e) {
      // msnodesqlv8 not installed; fall back to TCP connection with trustServerCertificate
      console.warn('msnodesqlv8 not installed; falling back to TCP. Ensure SQL auth or install msnodesqlv8 for Windows Auth.');
      return {
        server,
        port,
        database,
        options: { encrypt: false, trustServerCertificate: true }
      };
    }
  }

  // Fallback to object with empty user (will likely cause ELOGIN) so we surface clearer instructions
  return {
    user: undefined,
    password: undefined,
    server,
    port,
    database,
    options: { encrypt: false, trustServerCertificate: true }
  };
};

let poolPromise;
async function getPool() {
  // if previous attempt failed, allow retry by resetting poolPromise to undefined when error occurs
  if (!poolPromise) {
    const config = getDbConfig();
    // connect may throw; we capture and rethrow so callers can handle
    try {
      poolPromise = sql.connect(config);
      await poolPromise; // ensure connection established or raise
      return poolPromise;
    } catch (err) {
      poolPromise = undefined;
      throw err;
    }
  }
  return poolPromise;
}

app.use(express.static(path.join(__dirname, '..', 'web')));

// Health endpoint rápido
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/api/total-comments', async (req, res) => {
  try {
    if (DRY_RUN) return res.json(sampleData.totalComments);
    const pool = await getPool();
    const result = await pool.request()
      .query("SELECT COUNT(DISTINCT Opinion_id) AS TotalComentariosProcesados FROM vw_AnaliticaOpiniones;");
    res.json(result.recordset[0]);
  } catch (err) {
    console.error('total-comments error', err);
    const friendly = (err && err.originalError && err.originalError.message) ? err.originalError.message : (err.message || String(err));
    res.status(500).json({ error: 'Error al conectar/consultar la base de datos. ' + friendly + ' Comprueba `.env` y las credenciales.' });
  }
});

app.get('/api/opinions-by-product-date', async (req, res) => {
  try {
    if (DRY_RUN) return res.json(sampleData.byProductDate);
    const pool = await getPool();
    const sqlQuery = `
      SELECT Fecha, ProductoNombre, COUNT(*) AS TotalOpiniones
      FROM vw_AnaliticaOpiniones
      GROUP BY Fecha, ProductoNombre
      ORDER BY Fecha, ProductoNombre`;
    const result = await pool.request().query(sqlQuery);
    res.json(result.recordset);
  } catch (err) {
    console.error('opinions-by-product-date error', err);
    const friendly = (err && err.originalError && err.originalError.message) ? err.originalError.message : (err.message || String(err));
    res.status(500).json({ error: 'Error al conectar/consultar la base de datos. ' + friendly + ' Comprueba `.env` y las credenciales.' });
  }
});

app.get('/api/pct-satisfied-by-product', async (req, res) => {
  try {
    if (DRY_RUN) return res.json(sampleData.pctByProduct);
    const pool = await getPool();
    const definition = (req.query.definition || 'rating').toLowerCase();
    const threshold = req.query.threshold ? parseFloat(req.query.threshold) : (definition === 'score' ? 0.5 : 4);
    const condition = definition === 'score'
      ? `SUM(CASE WHEN Score_sentimiento >= ${threshold} THEN 1 ELSE 0 END)*1.0/COUNT(*)`
      : `SUM(CASE WHEN Rating >= ${threshold} THEN 1 ELSE 0 END)*1.0/COUNT(*)`;
    const sqlQuery = `
      SELECT ProductoNombre,
             ${condition} AS PctSatisfechos,
             COUNT(*) AS TotalOpiniones
      FROM vw_AnaliticaOpiniones
      GROUP BY ProductoNombre
      ORDER BY PctSatisfechos DESC`;
    const result = await pool.request().query(sqlQuery);
    res.json(result.recordset);
  } catch (err) {
    console.error('pct-satisfied-by-product error', err);
    const friendly = (err && err.originalError && err.originalError.message) ? err.originalError.message : (err.message || String(err));
    res.status(500).json({ error: 'Error al conectar/consultar la base de datos. ' + friendly + ' Comprueba `.env` y las credenciales.' });
  }
});

app.get('/api/satisfaction-trend', async (req, res) => {
  try {
    if (DRY_RUN) return res.json(sampleData.trend);
    const pool = await getPool();
    const definition = (req.query.definition || 'rating').toLowerCase();
    const threshold = req.query.threshold ? parseFloat(req.query.threshold) : (definition === 'score' ? 0.5 : 4);
    const condition = definition === 'score'
      ? `SUM(CASE WHEN Score_sentimiento >= ${threshold} THEN 1 ELSE 0 END)*1.0/COUNT(*)`
      : `SUM(CASE WHEN Rating >= ${threshold} THEN 1 ELSE 0 END)*1.0/COUNT(*)`;
    const sqlQuery = `
      SELECT Fecha,
             ${condition} AS PctSatisfechos
      FROM vw_AnaliticaOpiniones
      GROUP BY Fecha
      ORDER BY Fecha`;
    const result = await pool.request().query(sqlQuery);
    res.json(result.recordset);
  } catch (err) {
    console.error('satisfaction-trend error', err);
    const friendly = (err && err.originalError && err.originalError.message) ? err.originalError.message : (err.message || String(err));
    res.status(500).json({ error: 'Error al conectar/consultar la base de datos. ' + friendly + ' Comprueba `.env` y las credenciales.' });
  }
});

// Debug endpoint para obtener detalle de la conexión (no exponer en producción)
app.get('/api/debug-connection', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query("SELECT TOP 1 1 AS ok");
    res.json({ ok: true });
  } catch (err) {
    console.error('debug-connection error', err);
    const friendly = (err && err.originalError && err.originalError.message) ? err.originalError.message : (err.message || String(err));
    res.status(500).json({ ok: false, error: friendly });
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  try {
    console.log('ENV DB_USER:', process.env.DB_USER);
    console.log('Resolved DB config:', JSON.stringify(getDbConfig()));
  } catch (e) {
    console.warn('Could not resolve DB config for logging:', e && e.message);
  }
});
