
'use strict';

require('dotenv').config();

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const { WebSocketServer } = require('ws');
const { parseSerial } = require('./parser');
const database = require('./db');

const HOST = process.env.WEB_HOST || '0.0.0.0';
const PORT = Number(process.env.WEB_PORT || 3000);
const PIN = process.env.REMOTE_SP_PIN || '';
const REMOTE_ON = PIN.length >= 6;

const data = [];
const events = [];

let db = null;
let sqlConnected = false;
let lastDbWrite = null;
let lastDbError = null;

let latest = null;
let lastReadingAt = 0;
let serialPort = null;
let serialReady = false;

let lastAlarm = 0;
let lastWarning = 0;
let previous = null;
let spikeAt = 0;

let pendingSP = null;
let activeCommandId = null;

const ipAttempts = new Map();

const mime = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8'
};

const safeResult = (
  res,
  code,
  obj,
  mimeType = 'application/json; charset=utf-8'
) => {
  res.writeHead(code, {
    'Content-Type': mimeType,
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY'
  });

  res.end(
    typeof obj === 'string' ? obj : JSON.stringify(obj)
  );
};

let wss;

function publish(obj) {
  if (!wss) return;

  const s = JSON.stringify(obj);

  for (const ws of wss.clients) {
    if (ws.readyState === 1) {
      ws.send(s);
    }
  }
}

function sqlState() {
  return {
    enabled: true,
    connected: sqlConnected,
    guardando:
      sqlConnected &&
      lastDbWrite !== null &&
      Date.now() - lastDbWrite < 12000,
    lastWrite: lastDbWrite,
    lastError: lastDbError
  };
}

function live() {
  return (
    serialReady &&
    latest &&
    Date.now() - lastReadingAt < 4000
  );
}

// ===================================
// CONTROL REMOTO DE SETPOINT
// ===================================

function remoteStatus() {
  const reason =
    !REMOTE_ON
      ? 'Falta REMOTE_SP_PIN en .env'
      : !db || !sqlConnected
      ? 'MySQL no disponible; no se autoriza comando sin bitácora'
      : !live()
      ? 'Esperando datos reales y recientes por USB'
      : latest.alarma
      ? 'Bloqueado: alarma crítica enclavada en Arduino'
      : latest.modo !== 'AUTO'
      ? 'Cambia Arduino a AUTO con el botón MODE'
      : pendingSP
      ? 'Esperando confirmación del comando anterior'
      : 'Listo: enviará setpoint al Arduino real con confirmación';

  return {
    enabled: REMOTE_ON,
    ready: reason.startsWith('Listo:'),
    reason,
    min: 25,
    max: 55
  };
}

function checkPin(v) {
  if (!REMOTE_ON || typeof v !== 'string') {
    return false;
  }

  const a = Buffer.from(PIN);
  const b = Buffer.from(v);

  return (
    a.length === b.length &&
    crypto.timingSafeEqual(a, b)
  );
}

function limitAttempts(req) {
  const ip = req.socket.remoteAddress || 'unknown';
  const now = Date.now();

  let val = ipAttempts.get(ip);

  if (!val || now - val.ts > 600000) {
    val = { ts: now, n: 0 };
    ipAttempts.set(ip, val);
  }

  val.n++;

  return val.n > 8;
}

// ===================================
// MYSQL
// ===================================

function dbError(e) {
  sqlConnected = false;
  lastDbError = e.message;

  console.error('MySQL ERROR:', e.message);
}

async function logEvent(
  priority,
  origin,
  desc,
  code = null
) {
  const event = {
    ts: Date.now(),
    prioridad: priority,
    origen: origin,
    descripcion: desc,
    codigo: code
  };

  events.push(event);

  if (events.length > 500) {
    events.shift();
  }

  publish({
    type: 'event',
    data: event
  });

  try {
    await database.insertEvent(db, event);

    sqlConnected = true;
    lastDbError = null;
  } catch (e) {
    dbError(e);
  }
}

// ===================================
// TIPOS DE ALARMAS
// ===================================

const ALARM = {
  1: 'Sensor LM35: lectura inválida o desconexión',
  2: 'Sensor LM35: salto anómalo de medición',
  3: 'Temperatura crítica por encima del límite en Arduino',
  4: 'Pérdida de sincronismo del cruce por cero',
  5: 'Consigna fuera del rango seguro con calefactor habilitado'
};

let queue = [];
let flushing = false;

function enqueue(m) {
  queue.push(m);

  if (queue.length > 600) {
    queue.shift();

    lastDbError =
      'Cola SQL llena: se descartaron registros durante un fallo';

    sqlConnected = false;
  }
}

async function flushQueue() {
  if (flushing || queue.length === 0 || !db) {
    return;
  }

  flushing = true;

  try {
    while (queue.length) {
      await database.insertReading(db, queue[0]);

      queue.shift();

      sqlConnected = true;
      lastDbWrite = Date.now();
      lastDbError = null;
    }
  } catch (e) {
    if (lastDbError !== e.message) {
      dbError(e);
    }
  } finally {
    flushing = false;
  }
}

// ===================================
// RECIBIR DATOS DEL ARDUINO
// ===================================

async function onReading(m) {
  latest = m;
  lastReadingAt = m.ts;

  data.push(m);

  if (data.length > 7200) {
    data.shift();
  }

  publish({
    type: 'reading',
    data: m
  });

  enqueue(m);
  void flushQueue();

  const ac = m.alarma
    ? (m.alarma_codigo || 99)
    : 0;

  if (ac !== lastAlarm) {
    if (ac) {
      void logEvent(
        'critica',
        'Arduino',
        ALARM[ac] ||
          'Alarma del Arduino sin código de causa',
        ac
      );
    } else if (lastAlarm) {
      void logEvent(
        'info',
        'Arduino',
        'Fin de alarma después de rearme/reinicio',
        lastAlarm
      );
    }

    lastAlarm = ac;
  }

  const warning = (
    !m.alarma &&
    m.sp !== null &&
    m.pv > m.sp + 1
  ) ? 1 : 0;

  if (warning !== lastWarning) {
    if (warning) {
      void logEvent(
        'advertencia',
        'Temperatura',
        `Temperatura mayor al setpoint: PV ${m.pv.toFixed(2)} °C; SP ${Number(m.sp).toFixed(1)} °C`,
        10
      );
    } else if (lastWarning) {
      void logEvent(
        'info',
        'Temperatura',
        'Temperatura regresó cerca del setpoint',
        10
      );
    }

    lastWarning = warning;
  }

  if (
    previous &&
    m.ts - previous.ts < 3000 &&
    Math.abs(m.pv - previous.pv) >= 3 &&
    m.ts - spikeAt > 10000
  ) {
    spikeAt = m.ts;

    void logEvent(
      'advertencia',
      'LM35',
      `Salto sospechoso: ${previous.pv.toFixed(2)} a ${m.pv.toFixed(2)} °C; verificar interferencias`,
      11
    );
  }

  previous = m;
}

// ===================================
// FILTROS DE HISTORIAL
// ===================================

function rangeFromQuery(url) {
  const hasCustom =
    url.searchParams.has('start') ||
    url.searchParams.has('end');

  let from;
  let to;

  if (hasCustom) {
    from = Date.parse(
      url.searchParams.get('start') || ''
    );

    to = Date.parse(
      url.searchParams.get('end') || ''
    );

    if (
      !Number.isFinite(from) ||
      !Number.isFinite(to) ||
      to < from
    ) {
      throw Error('Rango de fechas inválido');
    }
  } else {
    const minutes = Number(
      url.searchParams.get('minutes') || 30
    );

    if (
      !Number.isFinite(minutes) ||
      minutes < 1 ||
      minutes > 10080
    ) {
      throw Error(
        'Intervalo inválido (máximo 7 días)'
      );
    }

    to = Date.now();
    from = to - minutes * 60000;
  }

  if (to - from > 7 * 86400e3) {
    throw Error('Máximo 7 días por consulta');
  }

  return { from, to };
}

// ===================================
// CAMBIAR SETPOINT DESDE DASHBOARD
// ===================================

async function command(req, res) {
  const origin = req.headers.origin;

  if (origin) {
    try {
      if (
        new URL(origin).host !== req.headers.host
      ) {
        return safeResult(
          res,
          403,
          { error: 'Origen no permitido' }
        );
      }
    } catch {
      return safeResult(
        res,
        403,
        { error: 'Origen inválido' }
      );
    }
  }

  if (
    req.headers['content-type']?.split(';')[0] !==
    'application/json'
  ) {
    return safeResult(
      res,
      415,
      { error: 'Solo JSON' }
    );
  }

  if (!remoteStatus().ready) {
    return safeResult(
      res,
      423,
      { error: remoteStatus().reason }
    );
  }

  let body = '';

  try {
    for await (const chunk of req) {
      body += chunk.toString();

      if (body.length > 1024) {
        return safeResult(
          res,
          413,
          { error: 'Contenido demasiado largo' }
        );
      }
    }

    const v = JSON.parse(body);
    const sp = v.sp;

    // Rango original restaurado
    const low = 25;
    const high = 55;

    if (
      typeof sp !== 'number' ||
      !Number.isFinite(sp) ||
      sp < low ||
      sp > high ||
      Math.abs(
        sp * 10 - Math.round(sp * 10)
      ) > 1e-6
    ) {
      return safeResult(
        res,
        400,
        {
          error:
            `Consigna fuera de rango: ${low}–${high} °C, pasos de 0.1 °C`
        }
      );
    }

    if (!checkPin(v.pin)) {
      if (limitAttempts(req)) {
        return safeResult(
          res,
          429,
          {
            error:
              'Demasiados intentos; reinicia en diez minutos'
          }
        );
      }

      return safeResult(
        res,
        401,
        { error: 'PIN incorrecto' }
      );
    }

    if (!remoteStatus().ready) {
      return safeResult(
        res,
        423,
        { error: remoteStatus().reason }
      );
    }

    const cmdId = await database.beginCommand(
      db,
      Date.now(),
      sp
    );

    activeCommandId = cmdId;

    try {
      const confirmed = await new Promise(
        (resolve, reject) => {
          const timer = setTimeout(() => {
            const p = pendingSP;

            pendingSP = null;

            if (p) {
              reject(
                Error('Sin confirmación del Arduino')
              );
            }
          }, 4000);

          pendingSP = {
            sp,

            resolve: x => {
              clearTimeout(timer);
              pendingSP = null;
              resolve(x);
            },

            reject: e => {
              clearTimeout(timer);
              pendingSP = null;
              reject(e);
            }
          };

          serialPort.write(
            `SP:${sp.toFixed(1)}\n`,
            err => {
              if (err && pendingSP) {
                pendingSP.reject(err);
              }
            }
          );
        }
      );

      await database.finishCommand(
        db,
        cmdId,
        'CONFIRMADO',
        confirmed,
        null
      );

      activeCommandId = null;

      void logEvent(
        'info',
        'Dashboard',
        `Consigna confirmada por Arduino: ${confirmed.toFixed(1)} °C`,
        20
      );

      return safeResult(
        res,
        200,
        {
          ok: true,
          sp: confirmed,
          mode: 'ARDUINO'
        }
      );
    } catch (e) {
      try {
        await database.finishCommand(
          db,
          cmdId,
          'FALLIDO',
          null,
          e.message
        );
      } catch (err) {
        dbError(err);
      }

      activeCommandId = null;

      return safeResult(
        res,
        504,
        { error: e.message }
      );
    }
  } catch (e) {
    return safeResult(
      res,
      400,
      { error: e.message }
    );
  }
}

// ===================================
// PROCESAR DATOS SERIALES
// ===================================

function processLine(line) {
  if (
    pendingSP &&
    line.startsWith('SP_ACK=')
  ) {
    const value = Number(line.slice(7));
    const p = pendingSP;

    if (
      Number.isFinite(value) &&
      Math.abs(value - p.sp) < 0.051
    ) {
      p.resolve(value);
    } else {
      p.reject(
        Error(
          'Arduino confirmó una consigna distinta'
        )
      );
    }

    return;
  }

  if (
    pendingSP &&
    line.startsWith('SP_ERROR=')
  ) {
    pendingSP.reject(
      Error(
        `Arduino rechazó: ${line.slice(9)}`
      )
    );

    return;
  }

  const m = parseSerial(
    line,
    Date.now()
  );

  if (m) {
    void onReading(m);
  }
}

// ===================================
// CONEXION USB ARDUINO
// ===================================

function startSerial() {
  const { SerialPort } = require('serialport');

  const com =
    process.env.SERIAL_PORT || 'COM4';

  const baud = Number(
    process.env.SERIAL_BAUD || 9600
  );

  let buffer = '';
  let reconnectTimer = null;

  function connect() {
    const port = new SerialPort({
      path: com,
      baudRate: baud,
      autoOpen: false
    });

    serialPort = port;
    serialReady = false;

    port.open(err => {
      if (err) {
        console.warn('USB:', err.message);

        reconnectTimer = setTimeout(
          connect,
          5000
        );

        return;
      }

      serialReady = true;
      buffer = '';

      console.log(
        `Arduino REAL: ${com}, ${baud} baudios`
      );

      publish({
        type: 'connection',
        data: { connected: true }
      });
    });

    port.on('data', bytes => {
      buffer += bytes.toString('utf8');

      if (buffer.length > 8192) {
        buffer = '';
      }

      const lines = buffer.split(/\r?\n/);

      buffer = lines.pop();

      for (const line of lines) {
        processLine(line.trim());
      }
    });

    port.on('error', err => {
      console.error(
        'Serie:',
        err.message
      );
    });

    port.on('close', () => {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      serialReady = false;
      lastReadingAt = 0;

      if (pendingSP) {
        pendingSP.reject(
          Error('Arduino desconectado')
        );
      }

      publish({
        type: 'connection',
        data: { connected: false }
      });

      void logEvent(
        'advertencia',
        'USB',
        'Arduino desconectado del puerto serie',
        12
      );

      reconnectTimer = setTimeout(
        connect,
        5000
      );
    });
  }

  connect();
}

// ===================================
// RUTAS DEL SERVIDOR
// ===================================

function setRoutes(server) {
  wss = new WebSocketServer({
    server,
    path: '/ws',
    maxPayload: 4096
  });

  server.on(
    'request',
    async (req, res) => {
      let url;

      try {
        url = new URL(
          req.url,
          `http://localhost:${PORT}`
        );
      } catch {
        return safeResult(
          res,
          400,
          { error: 'URL inválida' }
        );
      }

      if (
        req.method === 'POST' &&
        url.pathname ===
          '/api/control/setpoint'
      ) {
        return command(req, res);
      }

      if (req.method !== 'GET') {
        return safeResult(
          res,
          405,
          { error: 'Método no permitido' }
        );
      }

      // Estado actual
      if (
        url.pathname === '/api/state'
      ) {
        return safeResult(
          res,
          200,
          {
            connected: !!live(),
            mode: 'ARDUINO',
            latest: live() ? latest : null,
            sql: sqlState(),
            remote: remoteStatus(),
            queued: queue.length
          }
        );
      }

      // Histórico y exportación CSV
      if (
        url.pathname === '/api/history' ||
        url.pathname === '/api/export.csv'
      ) {
        try {
          const { from, to } =
            rangeFromQuery(url);

          const result =
            await database.history(
              db,
              from,
              to
            );

          if (
            url.pathname ===
            '/api/history'
          ) {
            return safeResult(
              res,
              200,
              result
            );
          }

          const columns = [
            'fecha_iso',
            'temp_lm35_c',
            'temp_lcd_c',
            'setpoint_c',
            'error_c',
            'modo',
            'mando_solicitado_pct',
            'alarma',
            'alarma_codigo',
            'aviso_codigo',
            'ac_configurada',
            'fuente'
          ];

          const rows = result.map(m =>
            [
              new Date(m.ts).toISOString(),
              m.pv,
              m.pv_lcd,
              m.sp,
              m.error_c,
              m.modo,
              m.mando,
              Number(m.alarma),
              m.alarma_codigo ?? '',
              m.aviso_codigo ?? '',
              Number(m.ac_habilitada),
              'ARDUINO'
            ].join(',')
          );

          res.setHeader(
            'Content-Disposition',
            'attachment; filename="incubadora_historico.csv"'
          );

          return safeResult(
            res,
            200,
            columns.join(',') +
              '\n' +
              rows.join('\n'),
            'text/csv; charset=utf-8'
          );
        } catch (e) {
          if (
            /inválido|Máximo/.test(
              e.message
            )
          ) {
            return safeResult(
              res,
              400,
              { error: e.message }
            );
          }

          dbError(e);

          return safeResult(
            res,
            503,
            { error: e.message }
          );
        }
      }

      // Historial de alarmas
      if (
        url.pathname === '/api/events'
      ) {
        try {
          return safeResult(
            res,
            200,
            await database.events(db)
          );
        } catch (e) {
          dbError(e);

          return safeResult(
            res,
            503,
            { error: e.message }
          );
        }
      }

      // Chart.js local
      if (
        url.pathname ===
        '/vendor/chart.js'
      ) {
        try {
          const b =
            await fs.promises.readFile(
              path.resolve(
                __dirname,
                'node_modules/chart.js/dist/chart.umd.js'
              )
            );

          res.writeHead(
            200,
            {
              'Content-Type':
                'text/javascript; charset=utf-8'
            }
          );

          return res.end(b);
        } catch (e) {
          return safeResult(
            res,
            404,
            {
              error:
                'Falta instalar Chart.js con npm install'
            }
          );
        }
      }

      // Archivos del dashboard
      const root = path.resolve(
        __dirname,
        'public'
      );

      let file;

      try {
        file = path.resolve(
          root,
          '.' +
            decodeURIComponent(
              url.pathname === '/'
                ? '/index.html'
                : url.pathname
            )
        );
      } catch {
        return safeResult(
          res,
          400,
          { error: 'Ruta inválida' }
        );
      }

      if (
        !file.startsWith(
          root + path.sep
        )
      ) {
        return safeResult(
          res,
          403,
          {
            error: 'Ruta no permitida'
          }
        );
      }

      try {
        const b =
          await fs.promises.readFile(
            file
          );

        res.writeHead(
          200,
          {
            'Content-Type':
              mime[path.extname(file)] ||
              'application/octet-stream',
            'X-Content-Type-Options':
              'nosniff'
          }
        );

        return res.end(b);
      } catch {
        return safeResult(
          res,
          404,
          { error: 'No encontrado' }
        );
      }
    }
  );
}

// ===================================
// INICIAR SERVIDOR
// ===================================

async function main() {
  if (!REMOTE_ON) {
    console.warn(
      'AVISO: configura REMOTE_SP_PIN en .env para control de consigna'
    );
  }

  try {
    db = await database.initDb();

    sqlConnected = true;

    console.log(
      'MySQL REAL conectado; tablas verificadas'
    );
  } catch (e) {
    console.error(
      'No se pudo iniciar MySQL:',
      e.message
    );

    console.error(
      'El servidor no empieza sin MySQL; evita presentar datos no guardados.'
    );

    process.exitCode = 1;

    return;
  }

  setInterval(
    () => void flushQueue(),
    2000
  );

  const server =
    http.createServer();

  setRoutes(server);

  server.listen(
    PORT,
    HOST,
    () => {
      console.log(
        `Dashboard real PC: http://127.0.0.1:${PORT}`
      );

      console.log(
        `Celular LAN: http://IP_DE_TU_PC:${PORT}`
      );

      startSerial();
    }
  );

  const stop = async () => {
    if (serialPort?.isOpen) {
      serialPort.close();
    }

    server.close();

    await db.end();

    process.exit(0);
  };

  process.once(
    'SIGINT',
    () => void stop()
  );

  process.once(
    'SIGTERM',
    () => void stop()
  );
}

if (require.main === module) {
  void main();
}

module.exports = {
  remoteStatus,
  rangeFromQuery
};
