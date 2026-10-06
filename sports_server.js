/**
 * Personal Sports Data Server — Cricket + Football
 * ------------------------------------------------
 * Sources (no API keys, all free):
 *   - ESPN public JSON  → football (scoreboard per league) + cricket (live header feed)
 *   - Cricbuzz page     → cricket (optional secondary, merged with ESPN)
 *
 * Requirements: Node 18+ (built-in fetch)
 * Setup:        npm i express cors
 * Self-test:    node server.js --test     ← run this first if anything fails
 * Run:          node server.js
 */
'use strict';
const express = require('express');
const cors = require('cors');

// ═══════════════════════════════ CONFIG ═══════════════════════════════
const CONFIG = {
  port: process.env.PORT || 4005,
  timezone: 'Asia/Kolkata', // used for ?date= filtering (override per request with ?tz=)

  football: {
    // ESPN league slugs → add/remove freely
    leagues: {
      'eng.1': 'Premier League',
      'esp.1': 'La Liga',
      'ita.1': 'Serie A',
      'ger.1': 'Bundesliga',
      'fra.1': 'Ligue 1',
      'uefa.champions': 'UEFA Champions League',
      'uefa.europa': 'UEFA Europa League',
      'ind.1': 'Indian Super League',
      'usa.1': 'MLS',
    },
    daysBack: 3,   // results window
    daysAhead: 10, // fixtures window
  },

  cricket: {
    useCricbuzz: true, // set false if Cricbuzz keeps failing; ESPN alone still works
  },

  every: {
    footballLive: 20_000,
    footballSchedule: 10 * 60_000,
    cricketLive: 30_000,   // when any cricket match is live
    cricketIdle: 2 * 60_000,
  },

  timeoutMs: 15_000,
  minGapPerHostMs: 400, // politeness: min gap between requests to the same host
};

const ESPN = 'https://site.api.espn.com/apis';
const ESPN_CRICKET_HEADER = 'https://site.web.api.espn.com/apis/personalized/v2/scoreboard/header?sport=cricket';
const CRICBUZZ_LIVE = 'https://www.cricbuzz.com/cricket-match/live-scores';
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36';

// ═══════════════════════════════ UTILS ════════════════════════════════
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const addDays = (d, n) => new Date(d.getTime() + n * 86_400_000);
const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, '');
const normName = (s) =>
  String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '');
const localDate = (iso, tz = CONFIG.timezone) =>
  iso
    ? new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(
        new Date(iso)
      )
    : null;
const clean = (m) => {
  if (!m) return m;
  const { _fp, seenAt, ...rest } = m;
  return rest;
};

// ═══════════════════════ HTTP (polite + retries) ══════════════════════
const hostNext = new Map();
async function politeWait(url) {
  const host = new URL(url).host;
  const now = Date.now();
  const slot = Math.max(now, hostNext.get(host) || 0);
  hostNext.set(host, slot + CONFIG.minGapPerHostMs);
  if (slot > now) await sleep(slot - now);
}

async function http(url, { as = 'json', retries = 2 } = {}) {
  for (let attempt = 0; ; attempt++) {
    await politeWait(url);
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), CONFIG.timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        headers: {
          'User-Agent': UA,
          Accept: as === 'json' ? 'application/json, text/plain, */*' : 'text/html,application/xhtml+xml,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
        },
      });
      if (!res.ok) {
        const err = new Error(`HTTP ${res.status}`);
        err.noRetry = res.status >= 400 && res.status < 500 && res.status !== 429;
        throw err;
      }
      return as === 'json' ? await res.json() : await res.text();
    } catch (e) {
      const msg = e.name === 'AbortError' ? 'timeout' : e.cause?.code || e.message;
      if (e.noRetry || attempt >= retries) throw new Error(`${url} → ${msg}`);
      await sleep(1000 * 2 ** attempt);
    } finally {
      clearTimeout(timer);
    }
  }
}

// ═══════════════════════ HEALTH + SCHEDULER ═══════════════════════════
const health = {};
async function track(name, fn) {
  const h = (health[name] ||= { ok: null, lastSuccess: null, lastError: null, failures: 0, info: null });
  try {
    const r = await fn();
    Object.assign(h, { ok: true, lastSuccess: new Date().toISOString(), failures: 0, info: r ?? null });
    return r;
  } catch (e) {
    h.failures++;
    Object.assign(h, { ok: false, lastError: `${new Date().toISOString()} ${e.message}` });
    if (h.failures === 1 || h.failures % 20 === 0) console.warn(`⚠️  [${name}] ${e.message} (failures: ${h.failures})`);
    return null;
  }
}

function every(name, interval, fn) {
  const run = async () => {
    await track(name, fn);
    setTimeout(run, typeof interval === 'function' ? interval() : interval);
  };
  run();
}

// ═══════════════════════════ STORE + SSE ══════════════════════════════
const store = {
  football: new Map(),
  cricketEspn: new Map(),
  cricketCb: new Map(),
  cricketSeries: new Map(),
};
const sseClients = new Map(); // res → sport filter

function broadcast(event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const [res, sport] of sseClients) if (!sport || sport === data.sport) res.write(msg);
}

function fingerprint(m) {
  return JSON.stringify([m.status, m.statusText, m.home?.score, m.away?.score, m.teams?.map((t) => t.score)]);
}

function upsert(map, m) {
  const prev = map.get(m.id);
  const fp = fingerprint(m);
  const changed = !prev || prev._fp !== fp;
  const next = { ...prev, ...m, _fp: fp, seenAt: Date.now(), updatedAt: changed ? new Date().toISOString() : prev.updatedAt };
  map.set(m.id, next);
  if (changed && prev && (m.status === 'live' || prev.status !== m.status)) broadcast('update', clean(next));
}

function prune(map, days = CONFIG.football.daysBack + 2) {
  const cutoff = Date.now() - days * 86_400_000;
  for (const [id, m] of map) {
    const old = m.startTime && new Date(m.startTime).getTime() < cutoff;
    const staleLive = m.status === 'live' && Date.now() - (m.seenAt || 0) > 30 * 60_000;
    if ((old && m.status !== 'live') || staleLive) map.delete(id);
  }
}

function isHot(m) {
  if (m.status === 'live') return true;
  if (m.status !== 'scheduled' || !m.startTime) return false;
  const diff = new Date(m.startTime).getTime() - Date.now();
  return diff < 10 * 60_000 && diff > -4 * 3600_000;
}

const detailCache = new Map();
async function cached(key, ttl, fn) {
  const hit = detailCache.get(key);
  if (hit && Date.now() - hit.at < ttl) return hit.data;
  const data = await fn();
  detailCache.set(key, { at: Date.now(), data });
  if (detailCache.size > 500) detailCache.delete(detailCache.keys().next().value);
  return data;
}

function espnStatus(state, label = '') {
  const n = String(label).toUpperCase();
  if (n.includes('POSTPONED')) return 'postponed';
  if (n.includes('CANCEL') || n.includes('ABANDON')) return 'cancelled';
  if (state === 'in') return 'live';
  if (state === 'post') return 'finished';
  return 'scheduled';
}

// ═══════════════════════ FOOTBALL — ESPN ADAPTER ══════════════════════
function normFootball(ev, slug, leagueName) {
  const comp = ev.competitions?.[0] || {};
  const st = comp.status || ev.status || {};
  const status = espnStatus(st.type?.state, st.type?.name);
  const team = (c) =>
    c && {
      id: c.team?.id,
      name: c.team?.displayName,
      short: c.team?.abbreviation,
      logo: c.team?.logo,
      form: c.form || null,
      score: status === 'scheduled' || c.score == null || c.score === '' ? null : Number(c.score),
      winner: c.winner === true,
    };
  const comps = comp.competitors || [];
  return {
    id: `fb_${ev.id}`,
    sourceId: String(ev.id),
    source: 'espn',
    sport: 'football',
    league: { slug, name: leagueName },
    startTime: ev.date,
    status,
    statusText: st.type?.shortDetail || st.type?.detail || '',
    clock: status === 'live' ? st.displayClock || null : null,
    venue: comp.venue?.fullName || null,
    home: team(comps.find((c) => c.homeAway === 'home') || comps[0]),
    away: team(comps.find((c) => c.homeAway === 'away') || comps[1]),
    events: (comp.details || []).map((d) => ({
      type: d.type?.text,
      minute: d.clock?.displayValue,
      teamId: d.team?.id,
      player: d.athletesInvolved?.[0]?.displayName || null,
      goal: !!d.scoringPlay,
      penalty: !!d.penaltyKick,
      ownGoal: !!d.ownGoal,
      yellow: !!d.yellowCard,
      red: !!d.redCard,
    })),
  };
}

let footballRangeOk = true; // falls back to per-day requests if date ranges are rejected
async function fetchFootballLeague(slug, from, to) {
  const base = `${ESPN}/site/v2/sports/soccer/${slug}/scoreboard`;
  let events = [];
  let name = null;
  if (footballRangeOk) {
    try {
      const d = await http(`${base}?dates=${ymd(from)}-${ymd(to)}&limit=500`);
      events = d.events || [];
      name = d.leagues?.[0]?.name;
    } catch (e) {
      if (!/HTTP 4\d\d/.test(e.message)) throw e;
      footballRangeOk = false;
      console.warn('ℹ️  ESPN rejected date range — switching to per-day requests');
    }
  }
  if (!footballRangeOk) {
    for (let d = new Date(from); d <= to; d = addDays(d, 1)) {
      const r = await http(`${base}?dates=${ymd(d)}`);
      events.push(...(r.events || []));
      name ||= r.leagues?.[0]?.name;
    }
  }
  const leagueName = name || CONFIG.football.leagues[slug] || slug;
  for (const ev of events) upsert(store.football, normFootball(ev, slug, leagueName));
  return `${events.length} matches`;
}

async function footballScheduleSync() {
  const now = new Date();
  const from = addDays(now, -CONFIG.football.daysBack);
  const to = addDays(now, CONFIG.football.daysAhead);
  for (const slug of Object.keys(CONFIG.football.leagues)) {
    await track(`football:${slug}`, () => fetchFootballLeague(slug, from, to));
  }
  prune(store.football);
  return `${store.football.size} matches stored`;
}

async function footballLiveSync() {
  const hot = new Set([...store.football.values()].filter(isHot).map((m) => m.league.slug));
  const now = new Date();
  for (const slug of hot) {
    await track(`football:${slug}`, () => fetchFootballLeague(slug, addDays(now, -1), addDays(now, 1)));
  }
  return `${hot.size} leagues refreshed`;
}

async function footballDetail(m) {
  const d = await http(`${ESPN}/site/v2/sports/soccer/${m.league.slug}/summary?event=${m.sourceId}`);
  return {
    match: clean(m),
    stats: (d.boxscore?.teams || []).map((t) => ({
      team: t.team?.displayName,
      stats: Object.fromEntries((t.statistics || []).map((s) => [s.name, s.displayValue])),
    })),
    lineups: (d.rosters || []).map((r) => ({
      team: r.team?.displayName,
      formation: r.formation || null,
      players: (r.roster || []).map((p) => ({
        name: p.athlete?.displayName,
        number: p.jersey,
        position: p.position?.abbreviation,
        starter: !!p.starter,
        subbedIn: !!p.subbedIn,
        subbedOut: !!p.subbedOut,
      })),
    })),
    keyEvents: (d.keyEvents || []).map((e) => ({
      minute: e.clock?.displayValue,
      type: e.type?.text,
      text: e.text,
      team: e.team?.displayName,
    })),
    commentary: (d.commentary || []).slice(0, 60).map((c) => ({ minute: c.time?.displayValue, text: c.text })),
    headToHead: d.headToHeadGames || [],
  };
}

async function footballStandings(slug) {
  const d = await http(`${ESPN}/v2/sports/soccer/${slug}/standings`);
  const groups = d.children?.length ? d.children : [d];
  return groups.map((g) => ({
    group: g.name || g.abbreviation || null,
    table: (g.standings?.entries || [])
      .map((e) => {
        const s = Object.fromEntries((e.stats || []).map((x) => [x.name, x.value]));
        return {
          rank: s.rank,
          team: e.team?.displayName,
          short: e.team?.abbreviation,
          logo: e.team?.logos?.[0]?.href,
          played: s.gamesPlayed,
          won: s.wins,
          drawn: s.ties,
          lost: s.losses,
          gf: s.pointsFor,
          ga: s.pointsAgainst,
          gd: s.pointDifferential,
          points: s.points,
        };
      })
      .sort((a, b) => (a.rank ?? 99) - (b.rank ?? 99)),
  }));
}

// ═══════════════════ CRICKET — ESPN ADAPTER (header feed) ═════════════
// Handles: "465", "308/6 (97.5 ov)", "214 & 84/2 (16 ov, target 140)", "92/7 (10.4/11 ov, target 92)"
function parseCricketScore(str) {
  if (!str) return [];
  return String(str)
    .split('&')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => {
      const m = p.match(/^(\d+)(?:\/(\d+))?\s*(d)?(?:\s*\(\s*(\d+(?:\.\d)?)(?:\/\d+)?\s*ov)?/i);
      const target = p.match(/target\s+(\d+)/i);
      return m
        ? {
            runs: +m[1],
            wickets: m[2] != null ? +m[2] : 10, // ESPN omits wickets when all out
            overs: m[4] ?? null,
            declared: !!m[3],
            target: target ? +target[1] : null,
            display: p,
          }
        : { display: p };
    });
}

function normCricketEspn(ev, lg) {
  const fs = ev.fullStatus || {};
  const state = fs.type?.state || ev.status;
  const label = fs.type?.description || ev.summary || '';
  let status = espnStatus(state, label);
  const anyScore = (ev.competitors || []).some((c) => c.score);
  // ESPN marks some matches "in" before a ball is bowled
  if (status === 'live' && !anyScore && /scheduled to begin|starts at/i.test(`${fs.longSummary} ${fs.summary}`)) {
    status = 'scheduled';
  }
  return {
    id: `cr_espn_${ev.id}`,
    sourceId: String(ev.id),
    source: 'espn',
    sport: 'cricket',
    series: { id: String(lg.id), name: lg.name || lg.shortName || null },
    title: ev.name || ev.shortName || null,
    description: ev.description || ev.title || null,
    format: ev.class?.generalClassCard || ev.eventType || null,
    startTime: ev.date,
    endTime: ev.endDate || null,
    status,
    statusText: fs.longSummary || fs.summary || ev.summary || label,
    session: fs.session || null,
    venue: ev.location || null,
    url: ev.link || null,
    teams: (ev.competitors || []).map((c) => ({
      id: c.id,
      name: c.displayName,
      short: c.abbreviation,
      logo: c.logo || null,
      score: c.score || null,
      innings: parseCricketScore(c.score),
      winner: c.winner === true,
      batting: fs.battingTeamId != null && String(fs.battingTeamId) === String(c.id),
    })),
  };
}

async function cricketEspnSync() {
  const d = await http(ESPN_CRICKET_HEADER);
  const leagues = d.sports?.[0]?.leagues || [];
  let count = 0;
  for (const lg of leagues) {
    store.cricketSeries.set(String(lg.id), { id: String(lg.id), name: lg.name || lg.shortName, seenAt: Date.now() });
    for (const ev of lg.events || []) {
      upsert(store.cricketEspn, normCricketEspn(ev, lg));
      count++;
    }
  }
  for (const [id, s] of store.cricketSeries) if (Date.now() - s.seenAt > 3 * 86_400_000) store.cricketSeries.delete(id);
  prune(store.cricketEspn);
  return `${count} matches in ${leagues.length} series`;
}

async function cricketDetail(m) {
  if (!m.sources?.includes('espn')) {
    return { match: m, note: 'Only Cricbuzz has this match — open match.url for the full scorecard.' };
  }
  const espnId = m.id.replace('cr_espn_', '');
  try {
    const { news, article, videos, ...rest } = await http(
      `${ESPN}/site/v2/sports/cricket/${m.series.id}/summary?event=${espnId}`
    );
    return { match: m, espn: rest };
  } catch (e) {
    return { match: m, note: `Scorecard endpoint unavailable (${e.message}). Basic match data above is still live.` };
  }
}

// ═════════════════════ CRICKET — CRICBUZZ ADAPTER ═════════════════════
function sliceBalanced(s, start) {
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < s.length; i++) {
    const ch = s[i];
    if (inStr) {
      if (esc) esc = false;
      else if (ch === '\\') esc = true;
      else if (ch === '"') inStr = false;
      continue;
    }
    if (ch === '"') inStr = true;
    else if (ch === '{') depth++;
    else if (ch === '}' && --depth === 0) return s.slice(start, i + 1);
  }
  return null;
}

// Cricbuzz is a Next.js app: match data is streamed inside self.__next_f.push([...]) chunks
function extractCricbuzz(html) {
  const chunks = [...html.matchAll(/self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)/g)].map((m) => {
    try { return JSON.parse(`"${m[1]}"`); } catch { return ''; }
  });
  const text = html + '\n' + chunks.join('');
  const found = new Map();
  const KEY = '"matchInfo":';
  let idx = 0;
  while ((idx = text.indexOf(KEY, idx)) !== -1) {
    const infoStart = idx + KEY.length;
    idx = infoStart;
    if (text[infoStart] !== '{') continue;
    const infoRaw = sliceBalanced(text, infoStart);
    if (!infoRaw) continue;
    let info, score = null;
    try { info = JSON.parse(infoRaw); } catch { continue; }
    if (!info?.matchId) continue;
    const after = infoStart + infoRaw.length;
    const SCORE = ',"matchScore":';
    if (text.startsWith(SCORE, after)) {
      const scoreRaw = sliceBalanced(text, after + SCORE.length);
      try { score = scoreRaw ? JSON.parse(scoreRaw) : null; } catch {}
    }
    const prev = found.get(info.matchId);
    if (!prev || (!prev.score && score)) found.set(info.matchId, { info, score });
  }
  return [...found.values()];
}

function cbStatus(state = '') {
  const s = String(state).toLowerCase();
  if (s.includes('complete') || s === 'result') return 'finished';
  if (s.includes('abandon') || s.includes('cancel') || s.includes('no result')) return 'cancelled';
  if (s.includes('preview') || s.includes('upcoming')) return 'scheduled';
  return 'live';
}

function normCricbuzz({ info, score }) {
  const innings = (ts) =>
    Object.keys(ts || {})
      .filter((k) => /^inngs\d+$/.test(k))
      .sort()
      .map((k) => ts[k])
      .map((i) => ({
        runs: i.runs ?? 0,
        wickets: i.wickets ?? 0,
        overs: i.overs != null ? String(i.overs) : null,
        declared: !!i.isDeclared,
        display: `${i.runs ?? 0}/${i.wickets ?? 0}${i.isDeclared ? 'd' : ''}${i.overs != null ? ` (${i.overs} ov)` : ''}`,
      }));
  const team = (t, ts) => {
    const inn = innings(ts);
    return { name: t?.teamName, short: t?.teamSName, score: inn.map((i) => i.display).join(' & ') || null, innings: inn };
  };
  const ms = Number(info.startDate);
  return {
    id: `cr_cb_${info.matchId}`,
    sourceId: String(info.matchId),
    source: 'cricbuzz',
    sport: 'cricket',
    series: { id: info.seriesId ? `cb_${info.seriesId}` : null, name: info.seriesName || null },
    title: `${info.team1?.teamName} vs ${info.team2?.teamName}`,
    description: info.matchDesc || null,
    format: info.matchFormat || null,
    startTime: Number.isFinite(ms) ? new Date(ms).toISOString() : null,
    status: cbStatus(info.state),
    statusText: info.status || info.state || '',
    venue: [info.venueInfo?.ground, info.venueInfo?.city].filter(Boolean).join(', ') || null,
    teams: [team(info.team1, score?.team1Score), team(info.team2, score?.team2Score)],
    url: `https://www.cricbuzz.com/live-cricket-scores/${info.matchId}`,
  };
}

async function cricbuzzSync() {
  const html = await http(CRICBUZZ_LIVE, { as: 'text' });
  const items = extractCricbuzz(html);
  if (!items.length) throw new Error('Page loaded but no match data found — Cricbuzz layout may have changed');
  for (const it of items) upsert(store.cricketCb, normCricbuzz(it));
  prune(store.cricketCb);
  return `${items.length} matches`;
}

// ═══════════════════════ CRICKET — MERGE SOURCES ══════════════════════
const balls = (o) => {
  if (o == null) return 0;
  const [a, b = '0'] = String(o).split('.');
  return +a * 6 + +b;
};
const progress = (m) =>
  m.teams.reduce((s, t) => s + (t.innings || []).reduce((x, i) => x + balls(i.overs) * 1000 + (i.runs || 0), 0), 0);

function matchKeys(m) {
  const day = (m.startTime || '').slice(0, 10);
  const pair = (f) => m.teams.map((t) => normName(f(t))).filter(Boolean).sort().join('|');
  return [pair((t) => t.name), pair((t) => t.short)].filter((k) => k.includes('|')).map((k) => `${k}@${day}`);
}

function cricketMatches() {
  const merged = [];
  const byKey = new Map();
  for (const m of store.cricketEspn.values()) {
    const c = { ...clean(m), sources: ['espn'], scoreSource: 'espn' };
    merged.push(c);
    for (const k of matchKeys(m)) byKey.set(k, c);
  }
  for (const cb of store.cricketCb.values()) {
    const target = matchKeys(cb).map((k) => byKey.get(k)).find(Boolean);
    if (!target) {
      merged.push({ ...clean(cb), sources: ['cricbuzz'], scoreSource: 'cricbuzz' });
      continue;
    }
    target.sources.push('cricbuzz');
    target.cricbuzzId = cb.sourceId;
    target.cricbuzzUrl = cb.url;
    // Freshest wins: whichever source shows more of the match played
    if (cb.status !== 'scheduled' && progress(cb) > progress(target)) {
      target.teams = target.teams.map((t) => {
        const alt = cb.teams.find((x) => normName(x.name) === normName(t.name) || normName(x.short) === normName(t.short));
        return alt ? { ...t, score: alt.score, innings: alt.innings } : t;
      });
      target.statusText = cb.statusText || target.statusText;
      target.scoreSource = 'cricbuzz';
      if (cb.status === 'finished' || target.status === 'scheduled') target.status = cb.status;
    }
  }
  return merged;
}

async function cricketSync() {
  const espn = await track('cricket:espn', cricketEspnSync);
  const cb = CONFIG.cricket.useCricbuzz ? await track('cricket:cricbuzz', cricbuzzSync) : 'disabled';
  if (espn == null && cb == null) throw new Error('Both cricket sources failed');
  return `espn: ${espn ?? 'failed'} | cricbuzz: ${cb ?? 'failed'}`;
}

// ═══════════════════════════ QUERY HELPERS ════════════════════════════
function filterMatches(list, q, kind) {
  let out = list;
  if (kind === 'live') out = out.filter((m) => m.status === 'live');
  if (kind === 'fixtures') out = out.filter((m) => m.status === 'scheduled' || m.status === 'postponed');
  if (kind === 'results') out = out.filter((m) => m.status === 'finished' || m.status === 'cancelled');
  const tz = q.tz || CONFIG.timezone;
  if (q.date) out = out.filter((m) => localDate(m.startTime, tz) === q.date);
  if (q.league) out = out.filter((m) => m.league?.slug === q.league);
  if (q.format) out = out.filter((m) => normName(m.format).includes(normName(q.format)));
  if (q.series) {
    const s = String(q.series).toLowerCase();
    out = out.filter((m) => String(m.series?.id) === s || (m.series?.name || '').toLowerCase().includes(s));
  }
  if (q.team) {
    const t = normName(q.team);
    out = out.filter((m) =>
      (m.teams || [m.home, m.away]).some((x) => normName(x?.name).includes(t) || normName(x?.short) === t)
    );
  }
  const dir = kind === 'results' ? -1 : 1;
  return out.map(clean).sort((a, b) => dir * (new Date(a.startTime) - new Date(b.startTime)));
}

const hasLiveCricket = () => [...store.cricketEspn.values(), ...store.cricketCb.values()].some(isHot);
const h = (fn) => (req, res) => Promise.resolve(fn(req, res)).catch((e) => res.status(502).json({ error: e.message }));

// ═══════════════════════════════ ROUTES ═══════════════════════════════
const app = express();
app.use(cors());

app.get('/', (req, res) =>
  res.json({
    endpoints: [
      'GET /api/health',
      'GET /api/live                          (football + cricket live)',
      'GET /api/stream?sport=cricket|football (Server-Sent Events, live push)',
      'GET /api/football/{live|fixtures|results|all}?date=YYYY-MM-DD&league=eng.1&team=arsenal',
      'GET /api/football/leagues',
      'GET /api/football/match/:id',
      'GET /api/football/standings/:league',
      'GET /api/cricket/{live|fixtures|results|all}?date=YYYY-MM-DD&series=asia&team=india&format=t20',
      'GET /api/cricket/series',
      'GET /api/cricket/match/:id',
    ],
  })
);

app.get('/api/health', (req, res) =>
  res.json({
    node: process.version,
    uptimeSec: Math.round(process.uptime()),
    counts: {
      football: store.football.size,
      cricketEspn: store.cricketEspn.size,
      cricketCricbuzz: store.cricketCb.size,
      cricketSeries: store.cricketSeries.size,
      sseClients: sseClients.size,
    },
    sources: health,
  })
);

app.get('/api/live', (req, res) =>
  res.json({
    football: filterMatches([...store.football.values()], req.query, 'live'),
    cricket: filterMatches(cricketMatches(), req.query, 'live'),
  })
);

for (const kind of ['live', 'fixtures', 'results', 'all']) {
  app.get(`/api/football/${kind}`, (req, res) => res.json(filterMatches([...store.football.values()], req.query, kind)));
  app.get(`/api/cricket/${kind}`, (req, res) => res.json(filterMatches(cricketMatches(), req.query, kind)));
}

app.get('/api/football/leagues', (req, res) =>
  res.json(Object.entries(CONFIG.football.leagues).map(([slug, name]) => ({ slug, name })))
);
app.get('/api/football/match/:id', h(async (req, res) => {
  const m = store.football.get(req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  res.json(await cached(`fb:${m.id}`, m.status === 'live' ? 20_000 : 10 * 60_000, () => footballDetail(m)));
}));
app.get('/api/football/standings/:league', h(async (req, res) => {
  res.json(await cached(`st:${req.params.league}`, 30 * 60_000, () => footballStandings(req.params.league)));
}));

app.get('/api/cricket/series', (req, res) => res.json([...store.cricketSeries.values()].map(({ seenAt, ...s }) => s)));
app.get('/api/cricket/match/:id', h(async (req, res) => {
  const id = req.params.id;
  const m = cricketMatches().find((x) => x.id === id || `cr_cb_${x.cricbuzzId}` === id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  res.json(await cached(`cr:${m.id}`, m.status === 'live' ? 20_000 : 10 * 60_000, () => cricketDetail(m)));
}));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  const sport = ['cricket', 'football'].includes(req.query.sport) ? req.query.sport : null;
  res.write(
    `event: snapshot\ndata: ${JSON.stringify({
      football: sport === 'cricket' ? [] : filterMatches([...store.football.values()], {}, 'live'),
      cricket: sport === 'football' ? [] : filterMatches(cricketMatches(), {}, 'live'),
    })}\n\n`
  );
  sseClients.set(res, sport);
  const ping = setInterval(() => res.write(': ping\n\n'), 25_000);
  req.on('close', () => {
    clearInterval(ping);
    sseClients.delete(res);
  });
});

// ═══════════════════════════ SELF-TEST MODE ═══════════════════════════
async function selfTest() {
  console.log(`\nNode ${process.version}`);
  if (typeof fetch !== 'function') {
    console.log('❌ Your Node is too old (no built-in fetch). Install Node 18 or newer.');
    process.exit(1);
  }
  const now = new Date();
  const tests = [
    ['ESPN football (Premier League)', () => fetchFootballLeague('eng.1', addDays(now, -3), addDays(now, 10))],
    ['ESPN football (ISL)', () => fetchFootballLeague('ind.1', addDays(now, -3), addDays(now, 10))],
    ['ESPN cricket live feed', cricketEspnSync],
    ['Cricbuzz live page', cricbuzzSync],
  ];
  for (const [name, fn] of tests) {
    try {
      console.log(`✅ ${name}: ${await fn()}`);
    } catch (e) {
      console.log(`❌ ${name}: ${e.message}`);
    }
  }
  const sample = cricketMatches().find((m) => m.status === 'live') || cricketMatches()[0];
  if (sample) console.log('\nSample cricket match:', JSON.stringify(sample, null, 2).slice(0, 800));
  process.exit(0);
}

// ═══════════════════════════════ START ════════════════════════════════
if (process.argv.includes('--test')) {
  selfTest();
} else {
  if (typeof fetch !== 'function') {
    console.error('❌ Node 18+ required (built-in fetch missing). Your version:', process.version);
    process.exit(1);
  }
  app
    .listen(CONFIG.port, () => {
      console.log(`⚽🏏 Sports server running → http://localhost:${CONFIG.port}`);
      every('football:schedule', CONFIG.every.footballSchedule, footballScheduleSync);
      every('football:live', CONFIG.every.footballLive, footballLiveSync);
      every('cricket', () => (hasLiveCricket() ? CONFIG.every.cricketLive : CONFIG.every.cricketIdle), cricketSync);
    })
    .on('error', (e) => {
      console.error(
        e.code === 'EADDRINUSE'
          ? `❌ Port ${CONFIG.port} is already in use. Stop the other process or run: PORT=4000 node server.js`
          : `❌ ${e.message}`
      );
      process.exit(1);
    });
}