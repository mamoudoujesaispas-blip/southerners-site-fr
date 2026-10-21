// Lecture des musiques envoyées (avec prise en charge des plages d'octets)
const U = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const T = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const CH = 524288, MAXR = 1048576;
async function pipe(cmds) {
  const r = await fetch(U + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + T, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  const j = await r.json();
  if (!Array.isArray(j)) throw new Error('db');
  return j.map(x => x.result);
}
module.exports = async (req, res) => {
  try {
    const id = String((req.query && req.query.id) || '');
    if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).end();
    const [m] = await pipe([['GET', 'audm:' + id]]);
    const meta = m ? JSON.parse(m) : null;
    if (!meta) return res.status(404).end();
    const size = meta.size;
    let start = 0, end = size - 1;
    const x = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
    if (x) {
      if (x[1] === '' && x[2] !== '') { start = Math.max(0, size - Number(x[2])); }
      else { if (x[1] !== '') start = Number(x[1]); if (x[2] !== '') end = Math.min(Number(x[2]), size - 1); }
    }
    if (start >= size || start > end) { res.setHeader('Content-Range', 'bytes */' + size); return res.status(416).end(); }
    end = Math.min(end, start + MAXR - 1);
    const a = Math.floor(start / CH), z = Math.floor(end / CH);
    const parts = await pipe(Array.from({ length: z - a + 1 }, (_, k) => ['GET', 'aud:' + id + ':' + (a + k)]));
    if (parts.some(p => !p)) return res.status(404).end();
    const buf = Buffer.concat(parts.map(p => Buffer.from(p, 'base64')));
    const out = buf.subarray(start - a * CH, end - a * CH + 1);
    res.setHeader('Content-Type', meta.type || 'audio/mpeg');
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Content-Range', 'bytes ' + start + '-' + (start + out.length - 1) + '/' + size);
    res.setHeader('Content-Length', out.length);
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    return res.status(206).send(out);
  } catch (e) { return res.status(500).end(); }
};
