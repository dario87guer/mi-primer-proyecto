require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');

const app = express();
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

// --- API GESTIÓN ---
app.get('/api/arbol-configuracion', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT s.id as suc_id, s.nombre as suc_nombre, 
                   c.id as caja_id, c.nombre_caja,
                   t.id as term_id, t.identificador_externo, t.empresa
            FROM sucursales s
            LEFT JOIN cajas c ON s.id = c.sucursal_id
            LEFT JOIN terminales t ON c.id = t.caja_id
            ORDER BY s.nombre, c.nombre_caja, t.identificador_externo
        `);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/:tipo', async (req, res) => {
    const { tipo } = req.params;
    if (tipo === 'sucursales') await pool.query('INSERT INTO sucursales (nombre) VALUES ($1)', [req.body.nombre]);
    if (tipo === 'cajas') await pool.query('INSERT INTO cajas (sucursal_id, nombre_caja) VALUES ($1, $2)', [req.body.sucursal_id, req.body.nombre]);
    if (tipo === 'terminales') {
        const { caja_id, empresa, identificador } = req.body;
        await pool.query(`INSERT INTO terminales (caja_id, empresa, identificador_externo) VALUES ($1, $2, $3)`, [caja_id, empresa, identificador]);
    }
    res.json({ mensaje: 'Ok' });
});

app.put('/api/:tipo/:id', async (req, res) => {
    const { tipo, id } = req.params;
    if (tipo === 'sucursales') await pool.query('UPDATE sucursales SET nombre = $1 WHERE id = $2', [req.body.nombre, id]);
    if (tipo === 'cajas') await pool.query('UPDATE cajas SET nombre_caja = $1 WHERE id = $2', [req.body.nombre, id]);
    if (tipo === 'terminales') {
        await pool.query(`UPDATE terminales SET identificador_externo=$1, empresa=$2 WHERE id=$3`, [req.body.identificador, req.body.empresa, id]);
    }
    res.json({ mensaje: 'Ok' });
});

app.delete('/api/:tipo/:id', async (req, res) => {
    const tabla = req.params.tipo === 'sucursales' ? 'sucursales' : (req.params.tipo === 'cajas' ? 'cajas' : 'terminales');
    await pool.query(`DELETE FROM ${tabla} WHERE id = $1`, [req.params.id]);
    res.json({ mensaje: 'Ok' });
});

// --- API INFORMES ---
app.get('/api/informes', async (req, res) => {
    const { desde, hasta, sucursal } = req.query;
    let query = `
        SELECT 
            t.fecha, s.id as suc_id, s.nombre as suc_nombre, c.id as caja_id, c.nombre_caja,
            COUNT(CASE WHEN t.empresa = 'PAGO FÁCIL' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN 1 END) as pf_cant_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as pf_monto_e,
            COUNT(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN 1 END) as pf_cant_d,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as pf_monto_d,
            COUNT(CASE WHEN t.empresa = 'SEAC' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN 1 END) as seac_cant_e,
            SUM(CASE WHEN t.empresa = 'SEAC' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as seac_monto_e,
            COUNT(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN 1 END) as seac_cant_d,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as seac_monto_d,
            COUNT(CASE WHEN t.empresa = 'COBRO EXPRESS' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN 1 END) as ce_cant_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as ce_monto_e,
            COUNT(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN 1 END) as ce_cant_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as ce_monto_d
        FROM transacciones t
        JOIN terminales term ON t.identificador_terminal = term.identificador_externo
        JOIN cajas c ON term.caja_id = c.id
        JOIN sucursales s ON c.sucursal_id = s.id
        WHERE t.fecha BETWEEN $1 AND $2
    `;
    const params = [desde, hasta];
    if (sucursal && sucursal !== 'todas') { params.push(sucursal); query += ` AND s.id = $${params.length}`; }
    query += ` GROUP BY t.fecha, s.id, s.nombre, c.id, c.nombre_caja ORDER BY s.nombre, t.fecha ASC, c.nombre_caja ASC`;
    const result = await pool.query(query, params);
    res.json(result.rows);
});

// --- IMPORTADORES ---
app.post('/importar/seac', upload.single('archivo'), async (req, res) => {
    const tipo = req.body.tipo;
    let n = 0, r = 0, i_ign = 0;
    const text = fs.readFileSync(req.file.path, 'utf8');
    for (let linea of text.split('\n')) {
        const c = linea.split(';');
        if (c.length >= 4) {
            let pdv, impStr, fFull;
            if (tipo === 'ventas') { fFull = c[0].trim(); pdv = c[1].trim(); impStr = c[3].trim(); }
            else { fFull = c[1].trim(); pdv = c[2].trim(); impStr = c[3].trim(); }
            const imp = parseFloat(impStr);
            if (pdv.length === 6 && !isNaN(imp)) {
                const fL = fFull.split(' ')[0].replace(/\//g, '-');
                const medio = tipo === 'ventas' ? 'EFECTIVO' : 'DEBITO';
                try {
                    if (medio === 'DEBITO') await pool.query(`UPDATE transacciones SET importe = importe - $1 WHERE empresa = 'SEAC' AND identificador_terminal = $2 AND fecha = $3 AND medio_pago = 'EFECTIVO' AND importe >= $1`, [imp, pdv, fL]);
                    const resDb = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago) VALUES ($1, $2, $3, 'SEAC', $4, $5) ON CONFLICT DO NOTHING`, [`SEAC_${pdv}_${fL}_${imp}_${medio}`, fL, imp, pdv, medio]);
                    if (resDb.rowCount > 0) n++; else r++;
                } catch (e) { i_ign++; }
            }
        }
    }
    fs.unlinkSync(req.file.path);
    res.json({ nuevos: n, repetidos: r, ignorados: i_ign });
});

app.post('/importar/pagofacil', upload.single('archivo'), async (req, res) => {
    let n = 0, r = 0, i_ign = 0;
    const content = fs.readFileSync(req.file.path, 'utf8');
    for (let linea of content.split('\n')) {
        const term = linea.match(/A\d{5}/), f = linea.match(/\d{2}\/\d{2}\/\d{2}/), m = linea.match(/[\d.]+\,\d{2}/);
        if (term && f && m) {
            const medio = linea.includes(' D ') ? 'DEBITO' : 'EFECTIVO', imp = parseFloat(m[0].replace(/\./g, '').replace(',', '.'));
            try {
                const resDb = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago) VALUES ($1, TO_DATE($2, 'DD/MM/YY'), $3, 'PAGO FÁCIL', $4, $5) ON CONFLICT DO NOTHING`, [`PF_${term[0]}_${f[0].replace(/\//g,'')}_${imp}_${medio}`, f[0], imp, term[0], medio]);
                if (resDb.rowCount > 0) n++; else r++;
            } catch (e) { i_ign++; }
        }
    }
    fs.unlinkSync(req.file.path);
    res.json({ nuevos: n, repetidos: r, ignorados: i_ign });
});

app.listen(3000, () => console.log('🚀 Servidor Online'));