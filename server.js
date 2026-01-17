require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const XLSX = require('xlsx');

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

// Helpers
function normalizeDate(val) {
    if (!val) return null;
    if (!isNaN(val) && typeof val !== 'string') {
        const date = new Date(Math.round((val - 25569) * 86400 * 1000));
        return date.toISOString().split('T')[0];
    }
    let s = String(val).trim();
    if (s.includes('/')) {
        let parts = s.split('/');
        if (parts.length === 3) {
            let d = parts[0].padStart(2, '0');
            let m = parts[1].padStart(2, '0');
            let y = parts[2];
            if (y.length === 2) y = "20" + y;
            return `${y}-${m}-${d}`;
        }
    }
    return s;
}

function getSmartVal(row, names) {
    const keys = Object.keys(row);
    for (let n of names) {
        const found = keys.find(k => k.trim().toLowerCase() === n.toLowerCase());
        if (found) return row[found];
    }
    return null;
}

// --- API GESTIÓN ---
app.get('/api/arbol-configuracion', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT s.id as suc_id, s.nombre as suc_nombre, c.id as caja_id, c.nombre_caja,
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
    try {
        if (tipo === 'sucursales') await pool.query('INSERT INTO sucursales (nombre) VALUES ($1)', [req.body.nombre]);
        if (tipo === 'cajas') await pool.query('INSERT INTO cajas (sucursal_id, nombre_caja) VALUES ($1, $2)', [req.body.sucursal_id, req.body.nombre]);
        if (tipo === 'terminales') await pool.query('INSERT INTO terminales (caja_id, empresa, identificador_externo) VALUES ($1, $2, $3)', [req.body.caja_id, req.body.empresa, req.body.identificador]);
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
        if (tipo === 'terminales') await pool.query('UPDATE terminales SET identificador_externo=$1, empresa=$2 WHERE id=$3', [req.body.identificador, req.body.empresa, id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/:tipo/:id', async (req, res) => {
    try {
        const tabla = req.params.tipo === 'sucursales' ? 'sucursales' : (req.params.tipo === 'cajas' ? 'cajas' : 'terminales');
        await pool.query(`DELETE FROM ${tabla} WHERE id = $1`, [req.params.id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: "No se puede eliminar: tiene datos vinculados." }); }
});

// --- API INFORMES ---
app.get('/api/informes', async (req, res) => {
    const { desde, hasta, sucursal } = req.query;
    let query = `
        SELECT t.fecha, s.id as suc_id, s.nombre as suc_nombre, c.nombre_caja,
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
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe_extra_efectivo ELSE 0 END) as ce_extra_e,
            COUNT(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN 1 END) as ce_cant_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as ce_monto_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe_extra_debito ELSE 0 END) as ce_extra_d
        FROM transacciones t
        JOIN terminales term ON t.identificador_terminal = term.identificador_externo
        JOIN cajas c ON term.caja_id = c.id
        JOIN sucursales s ON c.sucursal_id = s.id
        WHERE t.fecha BETWEEN $1 AND $2
    `;
    const p = [desde, hasta];
    if (sucursal && sucursal !== 'todas') { p.push(sucursal); query += ` AND s.id = $3`; }
    query += ` GROUP BY t.fecha, s.id, s.nombre, c.nombre_caja ORDER BY s.nombre, t.fecha ASC`;
    try {
        const result = await pool.query(query, p);
        res.json(result.rows);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// --- IMPORTADOR COBRO EXPRESS ---
app.post('/importar/cobroexpress', upload.single('archivo'), async (req, res) => {
    const tipo = req.body.tipo;
    let n = 0, r = 0, e = 0;
    console.log(`\n>>> INICIANDO PROCESO CE: ${tipo.toUpperCase()} <<<`);
    try {
        const workbook = XLSX.readFile(req.file.path);
        const sn = tipo === 'detallado' ? 'DETALLADO' : workbook.SheetNames[0];
        const sheet = workbook.Sheets[sn];
        if(!sheet) throw new Error(`Pestaña ${sn} no encontrada`);
        const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });
        let hIdx = -1;
        for(let i=0; i<data.length; i++) {
            const rS = JSON.stringify(data[i]);
            if(rS.toLowerCase().includes("boca") && (rS.toLowerCase().includes("fecha") || rS.toLowerCase().includes("monto"))) { hIdx=i; break; }
        }
        if(hIdx === -1) throw new Error("Encabezado real no detectado");
        const headers = data[hIdx];
        const rows = data.slice(hIdx + 1);
        for (let rArr of rows) {
            const row = {}; headers.forEach((h, i) => row[String(h).trim()] = rArr[i]);
            try {
                if (tipo === 'diario') {
                    const pdv = String(getSmartVal(row, ['Boca', 'BOCA']) || '').trim();
                    const fN = normalizeDate(getSmartVal(row, ['Fecha', 'FECHA']));
                    if(!pdv || !fN || pdv === "TOTAL GENERAL" || pdv === "Boca") continue;
                    const tot = parseFloat(getSmartVal(row,['Total Boletas'])||0), dev = Math.abs(parseFloat(getSmartVal(row,['Devoluciones'])||0)), deb = Math.abs(parseFloat(getSmartVal(row,['Debitos'])||0));
                    const rE = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, importe_extra_efectivo, empresa, identificador_terminal, medio_pago) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5,'EFECTIVO') ON CONFLICT (id_unico_empresa) DO NOTHING`, [`CE_DIA_${pdv}_${fN}`,fN,tot-dev-deb,parseFloat(getSmartVal(row,['Extra'])||0),pdv]);
                    const rD = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, importe_extra_debito, empresa, identificador_terminal, medio_pago) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5,'DEBITO') ON CONFLICT (id_unico_empresa) DO NOTHING`, [`CE_DIA_DEB_${pdv}_${fN}`,fN,deb,parseFloat(getSmartVal(row,['Extra Debitos'])||0),pdv]);
                    if(rE.rowCount > 0 || rD.rowCount > 0) n++; else r++;
                } else {
                    const idT = String(getSmartVal(row, ['ID_TRANSACCION']) || ""), pdv = String(getSmartVal(row, ['BOCA']) || ""), fN = normalizeDate(getSmartVal(row, ['FECHA_COBRO']));
                    if(!idT || !pdv || !fN) continue;
                    const med = String(getSmartVal(row,['COD MONEDA'])||"").toLowerCase().includes('debito') ? 'DEBITO' : 'EFECTIVO';
                    const res = await pool.query(`INSERT INTO transacciones (id_unico_empresa, id_transaccion_externo, fecha, importe, empresa, identificador_terminal, medio_pago) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5,$6) ON CONFLICT (id_transaccion_externo) DO NOTHING`, [`CE_DET_${idT}`,idT,fN,parseFloat(getSmartVal(row,['Monto'])||0),pdv,med]);
                    if (res.rowCount > 0) n++; else r++;
                }
            } catch (err) { e++; console.log(`[CE ERROR] ${err.message}`); }
        }
        console.log(`<<< CARGA CE FINALIZADA: ${n} OK | ${r} Repetidos | ${e} Fallas >>>`);
        res.json({ nuevos: n, repetidos: r, errores: e });
    } catch (ex) { console.error(`[CE CRÍTICO] ${ex.message}`); res.status(500).json({ error: ex.message }); }
    finally { if(req.file) fs.unlinkSync(req.file.path); }
});

app.post('/importar/:emp', upload.single('archivo'), async (req, res) => {
    const emp = req.params.emp; let n=0, r=0, e=0;
    console.log(`\n>>> INICIANDO PROCESO ${emp.toUpperCase()} <<<`);
    try {
        const text = fs.readFileSync(req.file.path, 'utf8');
        for (let l of text.split('\n')) {
            if (emp === 'pagofacil') {
                const t = l.match(/A\d{5}/), f = l.match(/\d{2}\/\d{2}\/\d{2}/), m = l.match(/[\d.]+\,\d{2}/);
                if (t && f && m) {
                    try {
                        const med = l.includes(' D ') ? 'DEBITO' : 'EFECTIVO', imp = parseFloat(m[0].replace(/\./g, '').replace(',', '.'));
                        const rDb = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago) VALUES ($1, TO_DATE($2, 'DD/MM/YY'), $3, 'PAGO FÁCIL', $4, $5) ON CONFLICT (id_unico_empresa) DO NOTHING`, [`PF_${t[0]}_${f[0].replace(/\//g,'')}_${imp}_${med}`, f[0], imp, t[0], med]);
                        if (rDb.rowCount > 0) n++; else r++;
                    } catch (ex) { e++; }
                }
            } else if (emp === 'seac') {
                const c = l.split(';');
                if (c.length >= 4) {
                    try {
                        const tipo = req.body.tipo; let pdv, fF, imp = parseFloat(c[3].trim());
                        if (tipo === 'ventas') { fF = c[0].trim(); pdv = c[1].trim(); } else { fF = c[1].trim(); pdv = c[2].trim(); }
                        if (pdv.length === 6 && !isNaN(imp)) {
                            const fL = fF.split(' ')[0].replace(/\//g, '-'), med = tipo === 'ventas' ? 'EFECTIVO' : 'DEBITO';
                            if (med === 'DEBITO') await pool.query(`UPDATE transacciones SET importe = importe - $1 WHERE empresa = 'SEAC' AND identificador_terminal = $2 AND fecha = $3 AND medio_pago = 'EFECTIVO'`, [imp, pdv, fL]);
                            const rDb = await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago) VALUES ($1, $2, $3, 'SEAC', $4, $5) ON CONFLICT (id_unico_empresa) DO NOTHING`, [`SEAC_${pdv}_${fL}_${imp}_${med}`, fL, imp, pdv, med]);
                            if (rDb.rowCount > 0) n++; else r++;
                        }
                    } catch (ex) { e++; }
                }
            }
        }
        console.log(`<<< CARGA ${emp.toUpperCase()} FINALIZADA: ${n} OK | ${r} Repetidos >>>`);
        res.json({ nuevos: n, repetidos: r, errores: e });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file) fs.unlinkSync(req.file.path); }
});

app.listen(3000, () => console.log('🚀 Sistema Dario v4.15 - Auditoría y Totales Ok'));