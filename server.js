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

// Helpers (Motor SEAC Infalible)
function normalizeDate(val) {
    if (!val) return null;
    let s = String(val).trim();
    if (s.includes(' ')) s = s.split(' ')[0];
    let parts = s.match(/(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})/);
    if (parts) return `${parts[3]}-${parts[2].padStart(2, '0')}-${parts[1].padStart(2, '0')}`;
    if (!isNaN(val) && typeof val !== 'string') {
        const date = new Date(Math.round((val - 25569) * 86400 * 1000));
        return date.toISOString().split('T')[0];
    }
    return s;
}

function getSmartVal(row, names) {
    if (!row) return null;
    const keys = Object.keys(row);
    for (let n of names) {
        const found = keys.find(k => {
            const cleanK = String(k).replace(/\s+/g, '').toLowerCase();
            const cleanN = String(n).replace(/\s+/g, '').toLowerCase();
            return cleanK === cleanN || cleanK.includes(cleanN);
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

// --- API GESTIÓN (RESTAURADA) ---
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

// --- API INFORMES (RESTAURADA VERSIÓN ORIGINAL) ---
app.get('/api/informes', async (req, res) => {
    const { desde, hasta, sucursal } = req.query;
    let query = `
        SELECT t.fecha, s.id as suc_id, s.nombre as suc_nombre, c.nombre_caja,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN COALESCE(t.cantidad, 1) ELSE 0 END) as pf_cant_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as pf_monto_e,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as pf_cant_d,
            SUM(CASE WHEN t.empresa = 'PAGO FÁCIL' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as pf_monto_d,
            SUM(CASE WHEN t.empresa = 'SEAC' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN COALESCE(t.cantidad, 1) ELSE 0 END) as seac_cant_e,
            SUM(CASE WHEN t.empresa = 'SEAC' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as seac_monto_e,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as seac_cant_d,
            SUM(CASE WHEN t.empresa = 'SEAC' AND t.medio_pago = 'DEBITO' THEN t.importe ELSE 0 END) as seac_monto_d,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN COALESCE(t.cantidad, 1) ELSE 0 END) as ce_cant_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND (t.medio_pago = 'EFECTIVO' OR t.medio_pago IS NULL) THEN t.importe ELSE 0 END) as ce_monto_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' THEN t.importe_extra_efectivo ELSE 0 END) as ce_extra_e,
            SUM(CASE WHEN t.empresa = 'COBRO EXPRESS' AND t.medio_pago = 'DEBITO' THEN COALESCE(t.cantidad, 1) ELSE 0 END) as ce_cant_d,
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

// --- IMPORTADOR SEAC (SOPORTE VENTAS Y DEBITOS) ---
app.post('/importar/seac', upload.single('archivo'), async (req, res) => {
    let n=0, e=0;
    const tipo = req.body.tipo || 'ventas';
    const omitidos = new Set();
    console.log(`\n>>> SEAC PROCESANDO: ${tipo.toUpperCase()} <<<`);
    try {
        const workbook = XLSX.readFile(req.file.path, { raw: true });
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: true });
        const consolidado = {};

        for (let row of data) {
            const fRaw = getSmartVal(row, ['FechaDeposito', 'Fecha', 'F.Cobranza', 'Hora']);
            let pdv = String(getSmartVal(row, ['PDV', 'Punto de Venta', 'Boca', 'Terminal']) || '').trim().replace(/\.0$/, '');
            const rawImp = getSmartVal(row, ['Importe', 'Valor Facial', 'Monto', 'Total']);

            if (pdv && fRaw && rawImp !== undefined) {
                const imp = cleanImport(rawImp);
                const fL = normalizeDate(fRaw);
                if (fL && fL.length === 10) {
                    const clave = `${pdv}_${fL}`;
                    if (!consolidado[clave]) consolidado[clave] = { pdv, fecha: fL, total: 0, cuenta: 0 };
                    consolidado[clave].total += imp;
                    consolidado[clave].cuenta += 1;
                }
            }
        }

        for (let k in consolidado) {
            const item = consolidado[k];
            const med = tipo === 'ventas' ? 'EFECTIVO' : 'DEBITO';
            const idU = `SEAC_${item.pdv}_${item.fecha}_${med}`;
            
            const check = await pool.query(`SELECT 1 FROM terminales WHERE identificador_externo = $1`, [item.pdv]);
            if (check.rowCount === 0) { omitidos.add(item.pdv); continue; }

            try {
                if (med === 'DEBITO') {
                    await pool.query(`UPDATE transacciones SET importe = importe - $1 WHERE empresa = 'SEAC' AND identificador_terminal = $2 AND fecha = $3 AND medio_pago = 'EFECTIVO'`, [item.total, item.pdv, item.fecha]);
                }
                await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1, $2, $3, 'SEAC', $4, $5, $6) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe, cantidad = EXCLUDED.cantidad`, [idU, item.fecha, item.total, item.pdv, med, item.cuenta]);
                n++;
                console.log(`[SEAC OK] PDV: ${item.pdv} | Total: ${item.total.toFixed(2)}`);
            } catch (dbErr) { e++; }
        }
        res.json({ nuevos: n, errores: e, omitidos: omitidos.size });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

// --- COBRO EXPRESS ---
app.post('/importar/cobroexpress', upload.single('archivo'), async (req, res) => {
    const tipo = req.body.tipo; let n = 0, r = 0, e = 0;
    try {
        const workbook = XLSX.readFile(req.file.path);
        const sn = tipo === 'detallado' ? 'DETALLADO' : workbook.SheetNames[0];
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sn], { header: 1 });
        let hIdx = -1;
        for(let i=0; i<data.length; i++) {
            const rS = JSON.stringify(data[i]);
            if(rS.toLowerCase().includes("boca") && (rS.toLowerCase().includes("fecha") || rS.toLowerCase().includes("monto"))) { hIdx=i; break; }
        }
        const headers = data[hIdx];
        const rows = data.slice(hIdx + 1);
        for (let rArr of rows) {
            const row = {}; headers.forEach((h, i) => row[String(h).trim()] = rArr[i]);
            try {
                if (tipo === 'diario') {
                    const pdv = String(getSmartVal(row, ['Boca', 'BOCA']) || '').trim().replace(/\.0$/, '');
                    const fN = normalizeDate(getSmartVal(row, ['Fecha', 'FECHA']));
                    if(!pdv || !fN || pdv === "TOTAL GENERAL") continue;
                    const tot = cleanImport(getSmartVal(row,['Total Boletas'])), dev = Math.abs(cleanImport(getSmartVal(row,['Devoluciones']))), deb = Math.abs(cleanImport(getSmartVal(row,['Debitos'])));
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, importe_extra_efectivo, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5,'EFECTIVO', $6) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe=EXCLUDED.importe, cantidad=EXCLUDED.cantidad`, [`CE_DIA_${pdv}_${fN}`,fN,tot-dev-deb,parseFloat(getSmartVal(row,['Extra'])||0),pdv, parseInt(getSmartVal(row,['Cant Boletas'])||1)]);
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, importe_extra_debito, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5,'DEBITO', 0) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe=EXCLUDED.importe`, [`CE_DIA_DEB_${pdv}_${fN}`,fN,deb,parseFloat(getSmartVal(row,['Extra Debitos'])||0),pdv]);
                    n++;
                } else {
                    const idT = String(getSmartVal(row, ['ID_TRANSACCION']) || ""), pdv = String(getSmartVal(row, ['BOCA']) || "").trim().replace(/\.0$/, '');
                    if(!idT || !pdv) continue;
                    const fN = normalizeDate(getSmartVal(row, ['FECHA_COBRO']));
                    const res = await pool.query(`INSERT INTO transacciones (id_unico_empresa, id_transaccion_externo, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,$4,'COBRO EXPRESS',$5, $6, 1) ON CONFLICT (id_transaccion_externo) DO NOTHING`, [`CE_DET_${idT}`,idT,fN,cleanImport(getSmartVal(row,['Monto'])),pdv, String(getSmartVal(row,['COD MONEDA'])||"").includes('DEBITO')?'DEBITO':'EFECTIVO']);
                    if (res.rowCount > 0) n++; else r++;
                }
            } catch (err) { e++; }
        }
        res.json({ nuevos: n, repetidos: r, errores: e });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

// --- PAGO FÁCIL ---
app.post('/importar/pagofacil', upload.single('archivo'), async (req, res) => {
    let n=0, r=0, e=0;
    try {
        const content = fs.readFileSync(req.file.path, 'utf8');
        const lines = content.split('\n');
        for (let l of lines) {
            const t = l.match(/A\d{5}/), f = l.match(/\d{2}\/\d{2}\/\d{2}/), m = l.match(/[\d.]+\,\d{2}/);
            if (t && f && m) {
                try {
                    const med = l.includes(' D ') ? 'DEBITO' : 'EFECTIVO';
                    const imp = cleanImport(m[0]);
                    const matchCant = l.match(/,\d{2}\s+(D\s+)?(\d+,\d{2})\s+AMB/);
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1, TO_DATE($2, 'DD/MM/YY'), $3, 'PAGO FÁCIL', $4, $5, $6) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe=EXCLUDED.importe, cantidad=EXCLUDED.cantidad`, [`PF_${t[0]}_${f[0].replace(/\//g,'')}_${imp}_${med}`, f[0], imp, t[0], med, matchCant ? parseInt(matchCant[2].split(',')[0]) : 1]);
                    n++;
                } catch (ex) { e++; }
            }
        }
        res.json({ nuevos: n, repetidos: r, errores: e });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

app.listen(3000, () => console.log('🚀 Sistema Dario v4.53 - Restauración Visual Completa'));