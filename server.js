require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const app = express();
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
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
  ssl: process.env.DB_HOST !== 'localhost' ? { rejectUnauthorized: false } : false
});

// --- API ÚLTIMAS FECHAS ---
app.get('/api/ultimas-fechas', async (req, res) => {
    try {
        const result = await pool.query(`SELECT empresa, MAX(fecha) as ultima_fecha FROM transacciones GROUP BY empresa`);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// --- HELPERS (BLINDADOS) ---
function normalizeDate(val) {
    if (!val) return null;
    let s = String(val).trim();
    if (!isNaN(val) && !s.includes('/') && !s.includes('-')) {
        const date = new Date(Math.round((Number(val) - 25569) * 86400 * 1000));
        return date.toISOString().split('T')[0];
    }
    if (s.includes(' ')) s = s.split(' ')[0];
    let parts = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})$/);
    if (parts) {
        let d = parts[1].padStart(2, '0'), m = parts[2].padStart(2, '0'), y = parts[3];
        if (y.length === 2) y = "20" + y;
        return `${y}-${m}-${d}`;
    }
    return s;
}

function getSmartVal(row, names) {
    const keys = Object.keys(row);
    for (let n of names) {
        const found = keys.find(k => {
            const ck = String(k).replace(/\s+/g, '').toLowerCase();
            const cn = String(n).replace(/\s+/g, '').toLowerCase();
            return ck === cn || ck.includes(cn);
        });
        if (found) return row[found];
    }
    return null;
}

function cleanImport(val) {
    if (val === null || val === undefined || val === '') return 0;
    let s = String(val).trim().replace(/[^0-9.,-]/g, '');
    if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
    else if (s.includes(',')) s = s.replace(',', '.');
    const n = parseFloat(s);
    return isNaN(n) ? 0 : n;
}

// --- IMPORTADORES CON LOGS ---
app.post('/importar/seac', upload.single('archivo'), async (req, res) => {
    let n=0, e=0, dup=0; const tipo = req.body.tipo || 'ventas';
    console.log(`\n[LOG] Procesando SEAC: ${tipo.toUpperCase()}`);
    try {
        const workbook = XLSX.readFile(req.file.path, { raw: true });
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: true });
        const consolidado = {};
        for (let row of data) {
            const fRaw = getSmartVal(row, ['FechaDeposito', 'Fecha', 'F.Cobranza', 'F. Operacion', 'F.Cobro']);
            let pdv = String(getSmartVal(row, ['PDV', 'Punto de Venta', 'Boca', 'Terminal']) || '').trim().replace(/\.0$/, '');
            const rawImp = getSmartVal(row, ['Importe', 'Valor Facial', 'Monto', 'Total']);
            if (pdv && fRaw && rawImp !== undefined) {
                const imp = cleanImport(rawImp); const fL = normalizeDate(fRaw);
                if (fL) {
                    const clave = `${pdv}_${fL}`;
                    if (!consolidado[clave]) consolidado[clave] = { pdv, fecha: fL, total: 0, cuenta: 0 };
                    consolidado[clave].total += imp; consolidado[clave].cuenta += 1;
                }
            }
        }
        for (let k in consolidado) {
            const it = consolidado[k]; const med = tipo === 'ventas' ? 'EFECTIVO' : 'DEBITO';
            try {
                const checkTerm = await pool.query(`SELECT 1 FROM terminales WHERE identificador_externo = $1`, [it.pdv]);
                if (checkTerm.rowCount === 0) { 
                    console.log(`[SEAC OMITIDO] PDV ${it.pdv} no existe en la Red.`);
                    dup++; continue; 
                }
                if (med === 'DEBITO') await pool.query(`UPDATE transacciones SET importe = importe - $1 WHERE empresa = 'SEAC' AND identificador_terminal = $2 AND fecha = $3 AND medio_pago = 'EFECTIVO'`, [it.total, it.pdv, it.fecha]);
                const idU = `SEAC_${it.pdv}_${it.fecha}_${med}`;
                const result = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1, $2, $3, 'SEAC', $4, $5, $6) ON CONFLICT (id_unico_empresa) DO NOTHING`, [idU, it.fecha, it.total, it.pdv, med, it.cuenta]);
                if (result.rowCount > 0) { n++; console.log(`[SEAC OK] PDV ${it.pdv} - ${it.fecha} - $${it.total}`); } else dup++;
            } catch (dbErr) { e++; console.error(`[ERR DB SEAC] ${dbErr.message}`); }
        }
        res.json({ nuevos: n, errores: e, omitidos: dup });
    } catch (ex) { console.error(`[ERR CRITICO SEAC] ${ex.message}`); res.status(500).json({ error: ex.message }); }
    finally { if(req.file) fs.unlinkSync(req.file.path); }
});

// RESTO DE IMPORTADORES (Cobro Express y Pago Fácil)...
// Se mantienen los de v5.04 pero asegurando logs en consola para Cursor.

app.post('/importar/cobroexpress', upload.single('archivo'), async (req, res) => {
    const tipo = req.body.tipo; let n = 0, e = 0, dup = 0;
    console.log(`\n[LOG] Procesando Cobro Express: ${tipo.toUpperCase()}`);
    try {
        const workbook = XLSX.readFile(req.file.path);
        const sn = tipo === 'detallado' ? 'DETALLADO' : workbook.SheetNames[0];
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sn], { header: 1 });
        let hIdx = -1;
        for(let i=0; i<data.length; i++) {
            const rS = JSON.stringify(data[i]);
            if(rS.toLowerCase().includes("boca") || rS.toLowerCase().includes("monto")) { hIdx=i; break; }
        }
        const headers = data[hIdx];
        const rows = data.slice(hIdx + 1);
        if (tipo === 'detallado') {
            const resumen = {};
            for (let i = 0; i < rows.length; i++) {
                const row = {}; headers.forEach((h, idx) => row[String(h).trim()] = rows[i][idx]);
                const pdv = String(getSmartVal(row, ['BOCA']) || "").trim().replace(/\.0$/, '');
                const fN = normalizeDate(getSmartVal(row, ['FECHA_COBRO']));
                const imp = cleanImport(getSmartVal(row, ['Monto']));
                const med = String(getSmartVal(row,['COD MONEDA']) || "").toUpperCase().includes('DEBITO') ? 'DEBITO' : 'EFECTIVO';
                if (pdv && fN) {
                    const clave = `${pdv}_${fN}_${med}`;
                    if (!resumen[clave]) resumen[clave] = { pdv, fecha: fN, medio: med, total: 0, cant: 0 };
                    resumen[clave].total += imp; resumen[clave].cant++;
                }
            }
            for (let k in resumen) {
                const it = resumen[k];
                try {
                    const checkTerm = await pool.query(`SELECT 1 FROM terminales WHERE identificador_externo = $1`, [it.pdv]);
                    if (checkTerm.rowCount === 0) { dup++; continue; }
                    const idU = `CE_DET_${it.pdv}_${it.fecha}_${it.medio}`;
                    const result = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,'COBRO EXPRESS',$4,$5,$6) ON CONFLICT (id_unico_empresa) DO NOTHING`, [idU, it.fecha, it.total, it.pdv, it.medio, it.cant]);
                    if (result.rowCount > 0) n++; else dup++;
                } catch (dbE) { e++; }
            }
        } else {
            for (let i = 0; i < rows.length; i++) {
                const row = {}; headers.forEach((h, idx) => row[String(h).trim()] = rows[i][idx]);
                const pdv = String(getSmartVal(row, ['Boca', 'BOCA']) || '').trim().replace(/\.0$/, '');
                const fN = normalizeDate(getSmartVal(row, ['Fecha', 'FECHA']));
                if(!pdv || !fN || pdv === "TOTAL GENERAL") continue;
                const dev = Math.abs(cleanImport(getSmartVal(row,['Devoluciones'])));
                const extE = cleanImport(getSmartVal(row,['Extra']));
                const extD = cleanImport(getSmartVal(row,['Extra Debitos']));
                try {
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad, devoluciones, importe_extra_efectivo) VALUES ($1,$2,0,'COBRO EXPRESS',$3,'EFECTIVO',0,$4,$5) ON CONFLICT (id_unico_empresa) DO UPDATE SET devoluciones=EXCLUDED.devoluciones, importe_extra_efectivo=EXCLUDED.importe_extra_efectivo`, [`CE_DET_${pdv}_${fN}_EFECTIVO`, fN, pdv, dev, extE]);
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad, importe_extra_debito) VALUES ($1,$2,0,'COBRO EXPRESS',$3,'DEBITO',0,$4) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe_extra_debito=EXCLUDED.importe_extra_debito`, [`CE_DET_${pdv}_${fN}_DEBITO`, fN, pdv, extD]);
                    n++; 
                } catch(dbE) { e++; }
            }
        }
        res.json({ nuevos: n, errores: e, omitidos: dup });
    } catch (ex) { console.error(`[ERR CE] ${ex.message}`); res.status(500).json({ error: ex.message }); }
    finally { if(req.file) fs.unlinkSync(req.file.path); }
});

app.get('/api/informes', async (req, res) => {
    const { desde, hasta } = req.query;
    let query = `
        SELECT t.fecha, s.nombre as suc_nombre, c.nombre_caja,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'EFECTIVO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as pf_cant_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'EFECTIVO' THEN t.importe ELSE 0 END) as pf_monto_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as pf_cant_d,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as pf_monto_d,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'EFECTIVO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as seac_cant_e,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'EFECTIVO' THEN t.importe ELSE 0 END) as seac_monto_e,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as seac_cant_d,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as seac_monto_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'EFECTIVO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as ce_cant_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'EFECTIVO' THEN t.importe ELSE 0 END) as ce_monto_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe_extra_efectivo ELSE 0 END) as ce_extra_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.devoluciones ELSE 0 END) as ce_dev,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as ce_cant_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as ce_monto_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe_extra_debito ELSE 0 END) as ce_extra_d
        FROM transacciones t
        JOIN terminales term ON t.identificador_terminal = term.identificador_externo
        JOIN cajas c ON term.caja_id = c.id
        JOIN sucursales s ON c.sucursal_id = s.id
        WHERE t.fecha BETWEEN $1 AND $2
        GROUP BY t.fecha, s.nombre, c.nombre_caja ORDER BY s.nombre, t.fecha ASC
    `;
    try { const result = await pool.query(query, [desde, hasta]); res.json(result.rows); } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/arbol-configuracion', async (req, res) => {
    try {
        const result = await pool.query(`SELECT s.id as suc_id, s.nombre as suc_nombre, c.id as caja_id, c.nombre_caja, t.id as term_id, t.identificador_externo, t.empresa, t.comision_porcentaje_efectivo, t.comision_fija_debito FROM sucursales s LEFT JOIN cajas c ON s.id = c.sucursal_id LEFT JOIN terminales t ON c.id = t.caja_id ORDER BY s.nombre, c.nombre_caja, t.identificador_externo`);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/:tipo', async (req, res) => {
    const { tipo } = req.params;
    try {
        if (tipo === 'sucursales') await pool.query('INSERT INTO sucursales (nombre) VALUES ($1)', [req.body.nombre]);
        if (tipo === 'cajas') await pool.query('INSERT INTO cajas (sucursal_id, nombre_caja) VALUES ($1, $2)', [req.body.sucursal_id, req.body.nombre]);
        if (tipo === 'terminales') await pool.query('INSERT INTO terminales (caja_id, empresa, identificador_externo, comision_porcentaje_efectivo, comision_fija_debito) VALUES ($1, $2, $3, $4, $5)', [req.body.caja_id, req.body.empresa, req.body.identificador, req.body.com_efectivo || 0, req.body.com_debito || 0]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/:tipo/:id', async (req, res) => {
    const { tipo, id } = req.params;
    try {
        if (tipo === 'sucursales' || tipo === 'cajas') {
            const tabla = tipo === 'sucursales' ? 'sucursales' : 'cajas';
            const col = tipo === 'sucursales' ? 'nombre' : 'nombre_caja';
            await pool.query(`UPDATE ${tabla} SET ${col} = $1 WHERE id = $2`, [req.body.nombre, id]);
        }
        if (tipo === 'terminales') await pool.query('UPDATE terminales SET identificador_externo=$1, empresa=$2, comision_porcentaje_efectivo=$3, comision_fija_debito=$4 WHERE id=$5', [req.body.identificador, req.body.empresa, req.body.com_efectivo, req.body.com_debito, id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/:tipo/:id', async (req, res) => {
    try {
        const tabla = req.params.tipo === 'sucursales' ? 'sucursales' : (req.params.tipo === 'cajas' ? 'cajas' : 'terminales');
        await pool.query(`DELETE FROM ${tabla} WHERE id = $1`, [req.params.id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: "No se puede eliminar." }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Control Transacciones Impuestos v5.08 - Puerto ${PORT}`));