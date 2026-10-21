// Serveur SOUTHERNERS — fonction Vercel + base Upstash Redis (REST)
const crypto = require('crypto');
const U = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const T = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const COLS = ['members','grades','vehicles','vault','vlog','lockers','debts','compta','playlist','users','settings'];
const OK = /^[a-f0-9]{32}$/;
const H = s => crypto.createHash('sha256').update(String(s)).digest('hex');
const js = s => { try { return JSON.parse(s); } catch (e) { return null; } };
const obj = a => { const o = {}; for (let i = 0; i < (a || []).length; i += 2) o[a[i]] = a[i + 1]; return o; };

async function pipe(cmds) {
  const r = await fetch(U + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + T, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  const j = await r.json();
  if (!Array.isArray(j)) throw new Error(j.error || 'Erreur base de données');
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
class HttpErr extends Error { constructor(c, m) { super(m); this.c = c; } }

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const out = (c, o) => res.status(c).json(o);
  if (req.method !== 'POST') return out(405, { error: 'Méthode non autorisée' });
  if (!U || !T) return out(500, { error: 'Base de données non configurée (Upstash Redis).' });
  try {
    const b = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
    const { a, uid, token } = b, now = Date.now();
    if (!OK.test(uid || '') || !OK.test(token || '')) throw new HttpErr(400, 'Identifiants invalides');

    /* ---------- synchronisation + présence ---------- */
    if (a === 'sync') {
      const [st, rv] = await pipe([['HGET', 'auth', uid], ['GET', 'rev']]);
      let rev = Number(rv || 0);
      if (st && st !== H(token)) throw new HttpErr(401, 'Accès refusé');
      let fresh = false;
      if (!st) {
        await pipe([['HSET', 'auth', uid, H(token)], ['HSET', 'd:users', uid, JSON.stringify({ name: 'Arrivant(e)', memberId: null, duty: false, at: now })], ['INCR', 'rev']]);
        rev++; fresh = true;
      }
      const [, pres] = await pipe([['HSET', 'pres', uid, String(now)], ['HGETALL', 'pres']]);
      const p = obj(pres), online = [], stale = [];
      for (const k in p) (now - Number(p[k]) < 45000 ? online : stale).push(k);
      if (stale.length && Math.random() < 0.05) await pipe([['HDEL', 'pres', ...stale]]);
      if (!fresh && Number(b.rev) === rev) return out(200, { rev, same: true, online });
      const all = await pipe(COLS.map(c => ['HGETALL', 'd:' + c]));
      const d = {};
      COLS.forEach((c, i) => { const o = obj(all[i]), m = {}; for (const k in o) { const v = js(o[k]); if (v) m[k] = v; } d[c] = m; });
      const site = d.settings.site || {}, meU = d.users[uid];
      if (site.president !== uid && !(meU && meU.memberId)) {
        const us = {};
        for (const k in d.users) {
          const x = d.users[k];
          if (k === uid) us[k] = x;
          else if (!x.memberId && x.num) us[k] = { name: 'Arrivant(e)', memberId: null, duty: false, num: x.num };
        }
        const e = {}; COLS.forEach(c => e[c] = {});
        e.users = us; e.settings = { site: { ...site, president: undefined } };
        return out(200, { rev, d: e, online: online.filter(k => us[k]) });
      }
      return out(200, { rev, d, online });
    }

    /* ---------- authentification commune ---------- */
    const [st] = await pipe([['HGET', 'auth', uid]]);
    if (!st || st !== H(token)) throw new HttpErr(401, 'Accès refusé');

    /* ---------- numéro d'arrivant ---------- */
    if (a === 'num') {
      const [sraw, uraw] = await pipe([['HGET', 'd:settings', 'site'], ['HGET', 'd:users', uid]]);
      const site = js(sraw) || {}, me = js(uraw);
      if (!me) throw new HttpErr(404, 'Compte introuvable');
      if (!site.president || site.president === uid || me.memberId || me.num) return out(200, { ok: true, num: me.num || null });
      const [lk] = await pipe([['SET', 'nl:' + uid, '1', 'NX', 'EX', '20']]);
      if (!lk) return out(200, { ok: true, num: null });
      const [n] = await pipe([['INCR', 'ucount']]);
      me.num = Number(n);
      await pipe([['HSET', 'd:users', uid, JSON.stringify(me)], ['INCR', 'rev']]);
      return out(200, { ok: true, num: me.num });
    }

    /* ---------- envoi d'un fichier audio (par morceaux) ---------- */
    if (a === 'aup') {
      const { aid, i, n, size, type, data } = b;
      { const [sr, ur] = await pipe([['HGET', 'd:settings', 'site'], ['HGET', 'd:users', uid]]);
        if (!((js(sr) || {}).president === uid || (js(ur) || {}).memberId)) throw new HttpErr(403, 'Action non autorisée'); }
      if (!OK.test(aid || '') || !Number.isInteger(i) || !Number.isInteger(n) || i < 0 || i >= n || n > 24 || !Number.isInteger(size) || size < 1 || size > 12 * 1024 * 1024 || typeof data !== 'string' || data.length > 760000)
        throw new HttpErr(400, 'Envoi invalide');
      await pipe([['SET', 'aud:' + aid + ':' + i, data, 'EX', '7200']]);
      if (i === n - 1) {
        const keys = Array.from({ length: n }, (_, k) => 'aud:' + aid + ':' + k);
        const ex = await pipe(keys.map(k => ['EXISTS', k]));
        if (ex.some(x => !x)) throw new HttpErr(400, 'Envoi incomplet, recommence.');
        await pipe([...keys.map(k => ['PERSIST', k]), ['SET', 'audm:' + aid, JSON.stringify({ size, n, type: /^audio\/[\w.+-]+$/.test(type || '') ? type : 'audio/mpeg' })]]);
      }
      return out(200, { ok: true });
    }

    /* ---------- prise du rôle de président par code ---------- */
    if (a === 'prez') {
      const code = process.env.PRESIDENT_CODE;
      const [sraw] = await pipe([['HGET', 'd:settings', 'site']]);
      const site = js(sraw) || {};
      if (code) {
        const [n] = await pipe([['INCR', 'rl:' + uid], ['EXPIRE', 'rl:' + uid, '600']]);
        if (Number(n) > 8) throw new HttpErr(429, 'Trop d\'essais, réessaie dans quelques minutes.');
        if (!crypto.timingSafeEqual(Buffer.from(H(b.code || '')), Buffer.from(H(code)))) throw new HttpErr(403, 'Code incorrect');
      } else if (site.president && site.president !== uid) {
        throw new HttpErr(403, 'Un président est déjà désigné.');
      }
      site.president = uid;
      await pipe([['HSET', 'd:settings', 'site', JSON.stringify(site)], ['INCR', 'rev']]);
      return out(200, { ok: true });
    }

    /* ---------- écriture ---------- */
    if (a === 'write') {
      const { op, col } = b; let { id, data } = b;
      if (!COLS.includes(col) || !['add', 'set', 'update', 'delete'].includes(op)) throw new HttpErr(400, 'Requête invalide');
      const [sraw, uraw] = await pipe([['HGET', 'd:settings', 'site'], ['HGET', 'd:users', uid]]);
      const site = js(sraw) || {}, me = js(uraw) || {}, P = site.president === uid, mid = me.memberId;
      if (op !== 'delete') {
        if (!data || typeof data !== 'object' || Array.isArray(data)) throw new HttpErr(400, 'Données invalides');
        data = { ...data }; delete data.id;
        if (JSON.stringify(data).length > 120000) throw new HttpErr(413, 'Données trop volumineuses');
      }
      let cur = null;
      if (op === 'add') id = crypto.randomUUID();
      else {
        if (typeof id !== 'string' || !/^[\w-]{1,80}$/.test(id)) throw new HttpErr(400, 'Identifiant invalide');
        const [c] = await pipe([['HGET', 'd:' + col, id]]); cur = js(c);
        if (op === 'update' && !cur) throw new HttpErr(404, 'Introuvable');
      }
      const keys = data ? Object.keys(data) : [];
      let ok = false;
      if (!P && !mid && !(col === 'users' && id === uid)) throw new HttpErr(403, 'Action non autorisée');
      switch (col) {
        case 'settings': case 'grades': case 'debts': ok = P; break;
        case 'members':
          if (op === 'add') ok = P || (!!mid && !data.uid);
          else if (op === 'delete') ok = P;
          else ok = P || (!!cur && cur.uid === uid && (!('uid' in data) || data.uid === uid));
          break;
        case 'users':
          if (op !== 'update') ok = false;
          else if (P) ok = keys.every(k => ['name', 'duty', 'memberId'].includes(k)) && (!('memberId' in data) || data.memberId === null || typeof data.memberId === 'string');
          else ok = id === uid && keys.every(k => ['name', 'duty'].includes(k)) && !(('duty' in data) && !mid);
          if (ok && 'name' in data && String(data.name).length > 40) ok = false;
          break;
        case 'vehicles': case 'vault': case 'playlist': ok = true; break;
        case 'vlog':
          if (op === 'add') ok = !data.uid || data.uid === uid;
          else ok = P || (op === 'update' && !!cur && cur.uid === uid && keys.length === 1 && data.status === 'Remis');
          break;
        case 'lockers': ok = P || (!!mid && id === mid); break;
        case 'compta':
          ok = op === 'add' ? (P || (!!mid && data.mid === mid)) : (P || (!!cur && !!mid && cur.mid === mid));
          break;
      }
      if (!ok) throw new HttpErr(403, 'Action non autorisée');
      if (op === 'delete' && col === 'playlist' && cur && typeof cur.url === 'string') {
        const m = /^\/api\/audio\?id=([a-f0-9]{32})$/.exec(cur.url);
        if (m) {
          const [mr] = await pipe([['GET', 'audm:' + m[1]]]); const mt = js(mr), ks = ['audm:' + m[1]];
          if (mt) for (let k = 0; k < mt.n; k++) ks.push('aud:' + m[1] + ':' + k);
          await pipe([['DEL', ...ks]]);
        }
      }
      if (op === 'delete') await pipe([['HDEL', 'd:' + col, id], ['INCR', 'rev']]);
      else {
        const val = op === 'update' ? { ...cur, ...data } : data;
        await pipe([['HSET', 'd:' + col, id, JSON.stringify(val)], ['INCR', 'rev']]);
      }
      return out(200, { ok: true, id });
    }
    throw new HttpErr(400, 'Action inconnue');
  } catch (e) {
    return out(e.c || 500, { error: e.c ? e.message : 'Erreur serveur : ' + e.message });
  }
};
