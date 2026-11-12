'use strict';
// Pares clave=valor emitidos una vez por segundo por Arduino UNO.
function numero(value) {
  if (value === undefined || value === null || String(value).trim() === '' || String(value).toUpperCase() === 'NA') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function parseSerial(line, now=Date.now()) {
  if (typeof line !== 'string' || !line.startsWith('t_ms=')) return null;
  const tags = {};
  for (const pair of line.trim().split(';')) {
    const eq=pair.indexOf('=');
    if(eq>0) tags[pair.slice(0,eq)] = pair.slice(eq+1);
  }
  const pv=numero(tags.PV);
  if(pv===null) return null;
  const sp=numero(tags.SP);
  return {
    ts:now, t_ms:numero(tags.t_ms), pv,
    pv_lcd:numero(tags.PV_LCD), sp,
    error_c:sp===null?null:sp-pv,
    modo:tags.MODO==='MAN'?'MAN':'AUTO',
    mando:numero(tags.MANDO),
    potencia_medida_w:null, // Sin medidor electrico: no confundir mando y vatios
    alarma:tags.ALARMA==='1',
    alarma_codigo:numero(tags.ALM_CODIGO),
    aviso_codigo:numero(tags.AVISO_CODIGO),
    ac_habilitada:tags.AC_HABILITADA==='1',
    sincronizado:tags.SYNC==='1',
    adc:numero(tags.ADC),
    fuente:'ARDUINO'
  };
}
module.exports={parseSerial};
