'use strict';
const DB_NAME = process.env.MYSQL_DATABASE || 'incubadora_industria4';
const IDENTIFIER = /^[A-Za-z0-9_]+$/;
const isoDb = t => new Date(t).toISOString().slice(0,23).replace('T',' '); // UTC sin Z, documentado
const fromDb = t => {
  if (t instanceof Date) return t.getTime();
  const s=String(t);
  const iso=s.replace(' ','T')+(s.endsWith('Z')?'':'Z');
  return Date.parse(iso);
};
async function initDb(){
  const mysql = require('mysql2/promise');
  if (!IDENTIFIER.test(DB_NAME)) throw new Error('Nombre de BD invalido');
  const pool = mysql.createPool({
    host:process.env.MYSQL_HOST||'127.0.0.1',
    port:Number(process.env.MYSQL_PORT||3306),
    user:process.env.MYSQL_USER||'incubadora_app',
    password:process.env.MYSQL_PASSWORD||'',
    database:DB_NAME, waitForConnections:true,
    connectionLimit:4, timezone:'Z', dateStrings:true,
    enableKeepAlive:true, keepAliveInitialDelay:0
  });
  try{
    await pool.query('SELECT 1');
    await pool.query(`CREATE TABLE IF NOT EXISTS lecturas (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      fecha DATETIME(3) NOT NULL,
      temp_lm35_c DOUBLE NULL, temp_lcd_c DOUBLE NULL,
      setpoint_c DOUBLE NULL, modo VARCHAR(8) NOT NULL,
      mando_solicitado_pct DOUBLE NULL,
      alarma TINYINT(1) NOT NULL DEFAULT 0,
      ac_configurada TINYINT(1) NOT NULL DEFAULT 0,
      fuente VARCHAR(12) NOT NULL DEFAULT 'ARDUINO',
      INDEX idx_lecturas_fecha (fecha)) ENGINE=InnoDB`);
    await pool.query(`CREATE TABLE IF NOT EXISTS eventos_alarmas (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      fecha DATETIME(3) NOT NULL, prioridad VARCHAR(14) NOT NULL,
      origen VARCHAR(30) NOT NULL, descripcion VARCHAR(250) NOT NULL,
      reconocido TINYINT(1) NOT NULL DEFAULT 0,
      INDEX idx_eventos_fecha (fecha)) ENGINE=InnoDB`);
    await pool.query(`CREATE TABLE IF NOT EXISTS comandos_setpoint (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      fecha_solicitud DATETIME(3) NOT NULL,
      fecha_confirmacion DATETIME(3) NULL,
      consigna_solicitada_c DOUBLE NOT NULL,
      consigna_confirmada_c DOUBLE NULL,
      estado VARCHAR(20) NOT NULL,
      detalle VARCHAR(250) NULL,
      origen VARCHAR(25) NOT NULL DEFAULT 'WEB',
      INDEX idx_comandos_fecha (fecha_solicitud)) ENGINE=InnoDB`);
    const cols={lecturas:{
      alarma_codigo:'TINYINT UNSIGNED NULL',
      aviso_codigo:'TINYINT UNSIGNED NULL',
      sincronizado:'TINYINT(1) NULL',
      adc:'DOUBLE NULL',
      t_ms:'BIGINT UNSIGNED NULL',
      error_c:'DOUBLE NULL'
    }, eventos_alarmas:{codigo:'TINYINT UNSIGNED NULL'}};
    for(const [table,extras] of Object.entries(cols)){
      const [found]=await pool.execute(`SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,[DB_NAME,table]);
      const existing=new Set(found.map(x=>x.COLUMN_NAME));
      for(const [field,type] of Object.entries(extras)){
        if(!existing.has(field)) await pool.query(`ALTER TABLE \`${table}\` ADD COLUMN \`${field}\` ${type}`);
      }
    }
    return pool;
  } catch(e){await pool.end();throw e;}
}
async function insertReading(db,m){
  await db.execute(`INSERT INTO lecturas
    (fecha,temp_lm35_c,temp_lcd_c,setpoint_c,error_c,modo,mando_solicitado_pct,alarma,alarma_codigo,aviso_codigo,ac_configurada,sincronizado,adc,t_ms,fuente)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [isoDb(m.ts),m.pv,m.pv_lcd,m.sp,m.error_c,m.modo,m.mando,
    Number(m.alarma),m.alarma_codigo,m.aviso_codigo,Number(m.ac_habilitada),
    Number(m.sincronizado),m.adc,m.t_ms,'ARDUINO']);
}
async function insertEvent(db,ev){
  await db.execute('INSERT INTO eventos_alarmas (fecha,prioridad,origen,descripcion,codigo) VALUES (?,?,?,?,?)',
    [isoDb(ev.ts),ev.prioridad,ev.origen,ev.descripcion,ev.codigo??null]);
}
async function beginCommand(db,ts,sp){
  const [r]=await db.execute(`INSERT INTO comandos_setpoint
    (fecha_solicitud,consigna_solicitada_c,estado,origen) VALUES (?,?,'ENVIADO','WEB')`,[isoDb(ts),sp]);
  return r.insertId;
}
async function finishCommand(db,id,status,sp,detail){
  await db.execute(`UPDATE comandos_setpoint SET fecha_confirmacion=?,consigna_confirmada_c=?,estado=?,detalle=? WHERE id=?`,
    [isoDb(Date.now()),sp??null,status,detail??null,id]);
}
async function history(db,from,to){
  const [rows]=await db.execute(`SELECT fecha,temp_lm35_c,temp_lcd_c,setpoint_c,error_c,modo,
    mando_solicitado_pct,alarma,alarma_codigo,aviso_codigo,ac_configurada,sincronizado,adc,t_ms
    FROM lecturas WHERE fecha BETWEEN ? AND ? ORDER BY fecha ASC LIMIT 50000`,[isoDb(from),isoDb(to)]);
  return rows.map(r=>({ts:fromDb(r.fecha),pv:r.temp_lm35_c,pv_lcd:r.temp_lcd_c,
    sp:r.setpoint_c,error_c:r.error_c,modo:r.modo,mando:r.mando_solicitado_pct,
    alarma:!!r.alarma,alarma_codigo:r.alarma_codigo,aviso_codigo:r.aviso_codigo,
    ac_habilitada:!!r.ac_configurada,sincronizado:!!r.sincronizado,adc:r.adc,t_ms:r.t_ms,fuente:'ARDUINO'}));
}
async function events(db){
  const [rows]=await db.query('SELECT fecha,prioridad,origen,descripcion,codigo FROM eventos_alarmas ORDER BY id DESC LIMIT 300');
  return rows.map(r=>({ts:fromDb(r.fecha),prioridad:r.prioridad,origen:r.origen,descripcion:r.descripcion,codigo:r.codigo}));
}
module.exports={initDb,insertReading,insertEvent,beginCommand,finishCommand,history,events,isoDb,fromDb};
