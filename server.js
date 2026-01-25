require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const app = express();
const uploadDir = path.join('/tmp', 'uploads'); 
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

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
    if (typeof val === 'number') return val;
    let s = String(val).trim().replace(/[^0-9.,-]/g, '');
    if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
    else if (s.includes(',')) s = s.replace(',', '.');
    const n = parseFloat(s);
    return isNaN(n) ? 0 : n;
}

// --- SECCION PROHIBIDO TOCAR - INICIO ---

// IMPORTADOR SEAC
app.post('/importar/seac', upload.single('archivo'), async (req, res) => {
    let n=0, e=0, dup=0; const detalles = [];
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
            const it = consolidado[k];
            try {
                const idU = `SEAC_${it.pdv}_${it.fecha}_EFECTIVO`;
                const check = await pool.query(`SELECT importe, cantidad FROM transacciones WHERE id_unico_empresa = $1`, [idU]);
                const yaExiste = check.rowCount > 0 && Math.abs(parseFloat(check.rows[0].importe) - it.total) < 0.01 && parseInt(check.rows[0].cantidad) === it.cuenta;
                if (yaExiste) {
                    dup++; detalles.push({ status: 'DUPLICADO', pdv: it.pdv, fecha: it.fecha, monto: it.total, msg: 'Ya existe.' });
                } else {
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1, $2, $3, 'SEAC', $4, 'EFECTIVO', $5) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe, cantidad = EXCLUDED.cantidad`, [idU, it.fecha, it.total, it.pdv, it.cuenta]);
                    n++; detalles.push({ status: 'OK', pdv: it.pdv, fecha: it.fecha, monto: it.total, msg: 'Sincronizado.' });
                }
            } catch (dbErr) { e++; }
        }
        res.json({ nuevos: n, errores: e, omitidos: dup, detalles });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

// IMPORTADOR PAGO FÁCIL (TCA)
app.post('/importar/pagofacil', upload.single('archivo'), async (req, res) => {
    let n = 0, e = 0, dup = 0; const detalles = [];
    try {
        if (!req.file) throw new Error("No se recibió archivo.");
        const content = fs.readFileSync(req.file.path, 'utf8');
        const lines = content.split(/\r?\n/);
        const consolidado = {};
        for (let line of lines) {
            const matchTerm = line.match(/^(A\d{5})\s+/); 
            if (matchTerm) {
                const terminal = matchTerm[1];
                const matchesFechas = line.match(/(\d{1,2}\/\d{1,2}\/\d{2,4})/g);
                if (!matchesFechas || matchesFechas.length < 2) continue;
                const fCobro = normalizeDate(matchesFechas[1]);
                const esDebito = line.includes(' D '); const medio = esDebito ? 'DEBITO' : 'EFECTIVO';
                const matchMonto = line.match(/([\d.]+,\d{2})\s+D/) || line.match(/([\d.]+,\d{2})$/);
                const importe = cleanImport(matchMonto ? matchMonto[1] : "0");
                const matchCant = line.match(/D\s+([\d,.]+)/) || line.match(/EFEC\s+([\d,.]+)/) || line.match(/\s+([\d,.]+)\s+AMB/);
                const cantidad = matchCant ? parseInt(cleanImport(matchCant[1])) : 1;
                if (terminal && fCobro && importe > 0) {
                    const clave = `${terminal}_${fCobro}_${medio}`;
                    if (!consolidado[clave]) consolidado[clave] = { terminal, fecha: fCobro, medio, total: 0, cant: 0 };
                    consolidado[clave].total += importe; consolidado[clave].cant += cantidad;
                }
            }
        }
        for (let it of Object.values(consolidado)) {
            try {
                const checkTerm = await pool.query(`SELECT 1 FROM terminales WHERE identificador_externo = $1`, [it.terminal]);
                if (checkTerm.rowCount === 0) { detalles.push({ status: 'DENEGADO', pdv: it.terminal, msg: 'No registrada.' }); dup++; continue; }
                const idU = `PF_${it.terminal}_${it.fecha}_${it.medio}`;
                const check = await pool.query(`SELECT importe FROM transacciones WHERE id_unico_empresa = $1`, [idU]);
                if (check.rowCount > 0 && Math.abs(parseFloat(check.rows[0].importe) - it.total) < 0.01) {
                    dup++; detalles.push({ status: 'DUPLICADO', pdv: it.terminal, msg: 'Ya existe.' });
                } else {
                    await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,'PAGO FÁCIL',$4,$5,$6) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe, cantidad = EXCLUDED.cantidad`, [idU, it.fecha, it.total, it.terminal, it.medio, it.cant]);
                    n++; detalles.push({ status: 'OK', pdv: it.terminal, fecha: it.fecha, monto: it.total, msg: `Cargado: ${it.medio}` });
                }
            } catch (dbE) { e++; }
        }
        res.json({ nuevos: n, errores: e, omitidos: dup, detalles });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

// IMPORTADOR COBRO EXPRESS (DIARIO + DETALLADO)
app.post('/importar/cobroexpress', upload.single('archivo'), async (req, res) => {
    const tipo = req.body.tipo; let n = 0, e = 0, dup = 0; const detalles = [];
    try {
        const workbook = XLSX.readFile(req.file.path);
        let sn = workbook.SheetNames[0];
        if (tipo === 'detallado') {
            const found = workbook.SheetNames.find(s => s.toUpperCase().includes("DET"));
            if (found) sn = found;
        }
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sn], { header: 1 });
        if (!data || data.length === 0) throw new Error("Excel vacío.");
        let hIdx = -1;
        for(let i=0; i<data.length; i++) {
            const rowStr = JSON.stringify(data[i]).toUpperCase();
            if(rowStr.includes("BOCA") || rowStr.includes("MONTO")) { hIdx=i; break; }
        }
        if (hIdx === -1) throw new Error("No se hallaron encabezados.");
        const headers = data[hIdx].map(h => String(h || "").trim().toUpperCase());
        const rows = data.slice(hIdx + 1);

        if (tipo === 'detallado') {
            const idxBoca = headers.indexOf("BOCA"), idxFecha = headers.indexOf("FECHA_COBRO"), idxMonto = headers.findIndex(h => h.includes("MONTO") || h.includes("TOTAL")), idxMon = headers.indexOf("COD MONEDA");
            const resumen = {};
            for (let r of rows) {
                if (!r[idxBoca]) continue;
                const pdv = String(r[idxBoca]).trim().replace(/\.0$/, '');
                const fN = normalizeDate(r[idxFecha]);
                const imp = cleanImport(r[idxMonto]);
                const monRaw = String(r[idxMon] || "").toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                const med = monRaw.includes('DEBITO') ? 'DEBITO' : 'EFECTIVO';
                if (pdv && fN) {
                    const clave = `${pdv}_${fN}_${med}`;
                    if (!resumen[clave]) resumen[clave] = { pdv, fecha: fN, medio: med, total: 0, cant: 0 };
                    resumen[clave].total += imp; resumen[clave].cant++;
                }
            }
            for (let it of Object.values(resumen)) {
                try {
                    const idU = `CE_DET_${it.pdv}_${it.fecha}_${it.medio}`;
                    const check = await pool.query(`SELECT importe, cantidad FROM transacciones WHERE id_unico_empresa = $1`, [idU]);
                    if (check.rowCount > 0 && Math.abs(parseFloat(check.rows[0].importe) - it.total) < 0.01 && parseInt(check.rows[0].cantidad) === it.cant) {
                        dup++; detalles.push({ status: 'DUPLICADO', pdv: it.pdv, fecha: it.fecha, monto: it.total, msg: 'Sin cambios.' });
                    } else {
                        await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad) VALUES ($1,$2,$3,'COBRO EXPRESS',$4,$5,$6) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe, cantidad = EXCLUDED.cantidad`, [idU, it.fecha, it.total, it.pdv, it.medio, it.cant]);
                        n++; detalles.push({ status: 'OK', pdv: it.pdv, fecha: it.fecha, monto: it.total, msg: 'Detalle sincronizado.' });
                    }
                } catch (dbE) { e++; }
            }
        } else {
            for (let i = 0; i < rows.length; i++) {
                const row = {}; data[hIdx].forEach((h, idx) => row[String(h).trim()] = rows[i][idx]);
                const pdv = String(getSmartVal(row, ['Boca', 'BOCA']) || '').trim().replace(/\.0$/, '');
                const fN = normalizeDate(getSmartVal(row, ['Fecha', 'FECHA']));
                if(!pdv || !fN || pdv === "TOTAL GENERAL") continue;
                const cantBol = parseInt(getSmartVal(row, ['Cant Boletas']) || 0);
                const totBol = Math.abs(cleanImport(getSmartVal(row, ['Total Boletas'])));
                const dev = Math.abs(cleanImport(getSmartVal(row,['Devoluciones'])));
                const impDeb = Math.abs(cleanImport(getSmartVal(row,['Debitos'])));
                const extE = cleanImport(getSmartVal(row,['Extra']));
                const impEfec = totBol - impDeb;
                try {
                    const idUE = `CE_DET_${pdv}_${fN}_EFECTIVO`;
                    const checkD = await pool.query(`SELECT devoluciones FROM transacciones WHERE id_unico_empresa = $1`, [idUE]);
                    if (checkD.rowCount > 0 && parseFloat(checkD.rows[0].devoluciones) === dev && Math.abs(parseFloat(checkD.rows[0].importe) - impEfec) < 0.01) {
                        dup++; detalles.push({ status: 'DUPLICADO', pdv, fecha: fN, msg: 'Ya cargado.' });
                    } else {
                        await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad, devoluciones, importe_extra_efectivo) VALUES ($1,$2,$3,'COBRO EXPRESS',$4,'EFECTIVO',$5,$6,$7) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe, devoluciones = EXCLUDED.devoluciones, importe_extra_efectivo = EXCLUDED.importe_extra_efectivo`, [idUE, fN, impEfec, pdv, cantBol, dev, extE]);
                        await pool.query(`INSERT INTO transacciones (id_unico_empresa, fecha, importe, empresa, identificador_terminal, medio_pago, cantidad, importe_extra_debito) VALUES ($1,$2,$3,'COBRO EXPRESS',$4,'DEBITO',0,0) ON CONFLICT (id_unico_empresa) DO UPDATE SET importe = EXCLUDED.importe`, [`CE_DET_${pdv}_${fN}_DEBITO`, fN, impDeb, pdv]);
                        n++; detalles.push({ status: 'OK', pdv, fecha: fN, msg: 'Diario procesado.' });
                    }
                } catch(dbE) { e++; }
            }
        }
        res.json({ nuevos: n, errores: e, omitidos: dup, detalles });
    } catch (ex) { res.status(500).json({ error: ex.message }); }
    finally { if(req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); }
});

// // SECCION PROHIBIDO TOCAR - FIN.

// APIS DE RED (GUTEN)
app.get('/api/arbol-configuracion', async (req, res) => {
    try {
        const result = await pool.query(`SELECT s.id as suc_id, s.nombre as suc_nombre, c.id as caja_id, c.nombre_caja, t.id as term_id, t.identificador_externo, t.empresa, COALESCE(t.comision_efectivo_porcentaje, 0) as comision_efectivo_porcentaje, COALESCE(t.precio_fijo_debito, 0) as precio_fijo_debito FROM sucursales s LEFT JOIN cajas c ON s.id = c.sucursal_id LEFT JOIN terminales t ON c.id = t.caja_id ORDER BY s.nombre, c.nombre_caja`);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/:tipo/:id', async (req, res) => {
    const { tipo, id } = req.params;
    try {
        if (tipo === 'sucursales') await pool.query('UPDATE sucursales SET nombre = $1 WHERE id = $2', [req.body.nombre, id]);
        else if (tipo === 'cajas') await pool.query('UPDATE cajas SET nombre_caja = $1 WHERE id = $2', [req.body.nombre, id]);
        else if (tipo === 'terminales') await pool.query(`UPDATE terminales SET identificador_externo = $1, comision_efectivo_porcentaje = $2, precio_fijo_debito = $3 WHERE id = $4`, [req.body.identificador, req.body.comision_efec || 0, req.body.precio_deb || 0, id]);
        res.json({ mensaje: 'Ok' });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/:tipo', async (req, res) => {
    const { tipo } = req.params;
    try {
        if (tipo === 'sucursales') await pool.query('INSERT INTO sucursales (nombre) VALUES ($1)', [req.body.nombre]);
        else if (tipo === 'cajas') await pool.query('INSERT INTO cajas (sucursal_id, nombre_caja) VALUES ($1, $2)', [req.body.sucursal_id, req.body.nombre]);
        else if (tipo === 'terminales') await pool.query(`INSERT INTO terminales (caja_id, empresa, identificador_externo, comision_efectivo_porcentaje, precio_fijo_debito) VALUES ($1, $2, $3, $4, $5)`, [req.body.caja_id, req.body.empresa, req.body.identificador, req.body.comision_efec || 0, req.body.precio_deb || 0]);
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

app.get('/api/ultimas-fechas', async (req, res) => {
    try {
        const result = await pool.query(`SELECT empresa, MAX(fecha) as ultima_fecha, MAX(CASE WHEN empresa = 'COBRO EXPRESS' AND (cantidad = 0 OR cantidad IS NULL) THEN fecha ELSE NULL END) as ultima_fecha_diario, MAX(CASE WHEN empresa = 'COBRO EXPRESS' AND cantidad > 0 THEN fecha ELSE NULL END) as ultima_fecha_detalle FROM transacciones GROUP BY empresa`);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/informes', async (req, res) => {
    const { desde, hasta } = req.query;
    let query = `
        SELECT t.fecha, s.nombre as suc_nombre, t.identificador_terminal as boca, t.empresa, t.medio_pago,
            t.importe, COALESCE(t.cantidad, 1) as cantidad, 
            COALESCE(t.devoluciones, 0) as devoluciones,
            COALESCE(t.importe_extra_efectivo, 0) as extra_e,
            COALESCE(t.importe_extra_debito, 0) as extra_d
        FROM transacciones t
        LEFT JOIN terminales term ON t.identificador_terminal = term.identificador_externo
        LEFT JOIN cajas c ON term.caja_id = c.id
        LEFT JOIN sucursales s ON c.sucursal_id = s.id
        WHERE t.fecha BETWEEN $1 AND $2
        ORDER BY t.fecha ASC, s.nombre ASC, t.identificador_terminal ASC
    `;
    try { const result = await pool.query(query, [desde, hasta]); res.json(result.rows); } catch (e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 v6.63 Activo puerto ${PORT}`));