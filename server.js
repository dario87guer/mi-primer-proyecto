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

// --- API GESTIÓN (ÁRBOL) ---
app.get('/api/arbol-configuracion', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT s.id as suc_id, s.nombre as suc_nombre, 
                   c.id as caja_id, c.nombre_caja,
                   t.id as term_id, t.identificador_externo, t.empresa,
                   t.comision_porcentaje, t.comision_fija
            FROM sucursales s
            LEFT JOIN cajas c ON s.id = c.sucursal_id
            LEFT JOIN terminales t ON c.id = t.caja_id
            ORDER BY s.nombre, c.nombre_caja, t.identificador_externo
        `);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- RUTAS DE EDICIÓN ---
app.put('/api/:tipo/:id', async (req, res) => {
    const { tipo, id } = req.params;
    try {
        if (tipo === 'sucursales') {
            await pool.query('UPDATE sucursales SET nombre = $1 WHERE id = $2', [req.body.nombre, id]);
        } else if (tipo === 'cajas') {
            await pool.query('UPDATE cajas SET nombre_caja = $1 WHERE id = $2', [req.body.nombre, id]);
        } else if (tipo === 'terminales') {
            const { identificador, empresa, caja_id, porc, fijo } = req.body;
            await pool.query(`UPDATE terminales SET identificador_externo=$1, empresa=$2, caja_id=$3, comision_porcentaje=$4, comision_fija=$5 WHERE id=$6`, 
            [identificador, empresa, caja_id, porc, fijo, id]);
        }
        res.json({ mensaje: 'Actualizado' });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- RUTAS DE CREACIÓN Y BORRADO ---
app.post('/api/sucursales', async (req, res) => {
    await pool.query('INSERT INTO sucursales (nombre) VALUES ($1)', [req.body.nombre]);
    res.json({ mensaje: 'Ok' });
});

app.post('/api/cajas', async (req, res) => {
    await pool.query('INSERT INTO cajas (sucursal_id, nombre_caja) VALUES ($1, $2)', [req.body.sucursal_id, req.body.nombre]);
    res.json({ mensaje: 'Ok' });
});

app.post('/api/terminales', async (req, res) => {
    const { caja_id, empresa, identificador, porc, fijo } = req.body;
    await pool.query(`INSERT INTO terminales (caja_id, empresa, identificador_externo, comision_porcentaje, comision_fija) 
                      VALUES ($1, $2, $3, $4, $5)`, [caja_id, empresa, identificador, porc || 0, fijo || 0]);
    res.json({ mensaje: 'Ok' });
});

app.delete('/api/:tipo/:id', async (req, res) => {
    const tabla = req.params.tipo === 'sucursales' ? 'sucursales' : (req.params.tipo === 'cajas' ? 'cajas' : 'terminales');
    try {
        await pool.query(`DELETE FROM ${tabla} WHERE id = $1`, [req.params.id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(400).json({ error: "No se puede eliminar: tiene datos vinculados." }); }
});

// --- API INFORMES DETALLADOS ---
app.get('/api/informes', async (req, res) => {
    const { desde, hasta, sucursal } = req.query;
    let query = `
        SELECT 
            t.fecha, s.id as suc_id, s.nombre as suc_nombre, c.id as caja_id, c.nombre_caja,
            -- Pago Fácil
            COUNT(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.importe > 0 THEN 1 END) as pf_cant_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' THEN t.importe ELSE 0 END) as pf_monto_e,
            0 as pf_cant_d, 0 as pf_monto_d,
            -- SEAC
            COUNT(CASE WHEN t.empresa = 'SEAC' AND t.importe > 0 THEN 1 END) as seac_cant_e,
            SUM(CASE WHEN t.empresa = 'SEAC' THEN t.importe ELSE 0 END) as seac_monto_e,
            0 as seac_cant_d, 0 as seac_monto_d,
            -- Cobro Express
            COUNT(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.importe > 0 THEN 1 END) as ce_cant_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe ELSE 0 END) as ce_monto_e,
            0 as ce_cant_d, 0 as ce_monto_d
        FROM transacciones t
        JOIN terminales term ON t.identificador_terminal = term.identificador_externo
        JOIN cajas c ON term.caja_id = c.id
        JOIN sucursales s ON c.sucursal_id = s.id
        WHERE 1=1
    `;
    const params = [];
    if (desde) { params.push(desde); query += ` AND t.fecha >= $${params.length}`; }
    if (hasta) { params.push(hasta); query += ` AND t.fecha <= $${params.length}`; }
    if (sucursal) { params.push(sucursal); query += ` AND s.id = $${params.length}`; }

    query += ` GROUP BY t.fecha, s.id, s.nombre, c.id, c.nombre_caja ORDER BY s.nombre, c.nombre_caja, t.fecha ASC`;

    try {
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- IMPORTADOR ---
app.post('/importar/pagofacil', upload.single('archivo'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'Sin archivo' });
    try {
        const contenido = fs.readFileSync(req.file.path, 'utf8');
        const lineas = contenido.split('\n');
        let nuevos = 0; let fallidos = [];
        for (let linea of lineas) {
            const termMatch = linea.match(/A\d{5}/);
            const fechaMatch = linea.match(/\d{2}\/\d{2}\/\d{2}/);
            const importeMatch = linea.match(/[\d.]+\,\d{2}/);
            if (termMatch && fechaMatch && importeMatch) {
                const terminal = termMatch[0];
                const importe = parseFloat(importeMatch[0].replace(/\./g, '').replace(',', '.'));
                const idUnico = `PF_${terminal}_${fechaMatch[0].replace(/\//g,'')}_${importe}`;
                try {
                    const r = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal) 
                        VALUES ($1, TO_DATE($2, 'DD/MM/YY'), $3, 'PAGO FÁCIL', $4) ON CONFLICT DO NOTHING`, 
                        [idUnico, fechaMatch[0], importe, terminal]);
                    if (r.rowCount > 0) nuevos++;
                } catch (e) { if (e.code === '23503') fallidos.push(terminal); }
            }
        }
        fs.unlinkSync(req.file.path);
        if (fallidos.length > 0) return res.status(400).json({ error: `La terminal ${[...new Set(fallidos)].join(', ')} no existe.` });
        res.json({ mensaje: `Éxito: ${nuevos} cargados.` });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.listen(3000, () => console.log('🚀 Servidor en puerto 3000'));