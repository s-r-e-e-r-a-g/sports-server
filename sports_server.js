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
    footballLive: process.env.FOOTBALL_LIVE_MS ? +process.env.FOOTBALL_LIVE_MS : 15_000,
    footballSchedule: 10 * 60_000,
    cricketLive: process.env.CRICKET_LIVE_MS ? +process.env.CRICKET_LIVE_MS : 15_000,   // when any cricket match is live
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
  return JSON.stringify([m.status, m.statusText, m.clock, m.home?.score, m.away?.score, m.teams?.map((t) => t.score)]);
}

function upsert(map, m) {
  const prev = map.get(m.id);
  const fp = fingerprint(m);
  const changed = !prev || prev._fp !== fp;
  const next = { ...prev, ...m, _fp: fp, seenAt: Date.now(), updatedAt: changed ? new Date().toISOString() : prev.updatedAt };
  if (m.status === 'finished') {
    delete next.live;
  }
  map.set(m.id, next);
  if (changed && (m.status === 'live' || (prev && prev.status !== m.status))) broadcast('update', clean(next));
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

function makeShortName(short, name) {
  const s = (short || '').trim();
  const n = (name || '').trim();
  if (s && s.length >= 2 && s.length <= 4 && !/\s/.test(s) && (s.toLowerCase() !== n.toLowerCase() || n.length <= 4)) {
    return s.toUpperCase();
  }
  const src = n || s;
  if (!src) return '?';
  const cleanStr = src.replace(/[^a-zA-Z0-9\s]/g, ' ').trim();
  const words = cleanStr.split(/\s+/).filter((w) => !/^(and|of|the|vs|&)$/i.test(w));
  if (words.length >= 3) {
    return words.slice(0, 4).map((w) => w[0].toUpperCase()).join('');
  }
  if (words.length === 2) {
    return (words[0][0] + words[1][0]).toUpperCase();
  }
  return words[0].slice(0, Math.min(3, words[0].length)).toUpperCase();
}

// ═══════════════════════ FOOTBALL — ESPN ADAPTER ══════════════════════
function normFootball(ev, slug, leagueName) {
  const comp = ev.competitions?.[0] || {};
  const st = comp.status || ev.status || {};
  const status = espnStatus(st.type?.state, st.type?.name);
  const events = (comp.details || []).map((d) => ({
    type: d.type?.text,
    minute: d.clock?.displayValue,
    teamId: d.team?.id,
    player: d.athletesInvolved?.[0]?.displayName || null,
    goal: !!d.scoringPlay,
    penalty: !!d.penaltyKick,
    ownGoal: !!d.ownGoal,
    yellow: !!d.yellowCard,
    red: !!d.redCard,
  }));
  const comps = comp.competitors || [];
  const homeComp = comps.find((c) => c.homeAway === 'home') || comps[0];
  const awayComp = comps.find((c) => c.homeAway === 'away') || comps[1];

  let homeGoalsFromEvents = 0;
  let awayGoalsFromEvents = 0;
  for (const e of events) {
    if (e.goal || (e.scoringPlay && /goal|penalty/i.test(e.type || ''))) {
      if (String(e.teamId) === String(homeComp?.team?.id)) homeGoalsFromEvents++;
      else if (String(e.teamId) === String(awayComp?.team?.id)) awayGoalsFromEvents++;
    }
  }

  const team = (c, goalsFromEvents) => {
    if (!c) return null;
    let s = (status === 'scheduled' || c.score == null || c.score === '') ? null : Number(c.score);
    if (status !== 'scheduled' && (s == null || isNaN(s))) {
      s = goalsFromEvents;
    }
    return {
      id: c.team?.id,
      name: c.team?.displayName,
      short: makeShortName(c.team?.abbreviation, c.team?.displayName),
      logo: c.team?.logo,
      form: c.form || null,
      score: s,
      winner: c.winner === true,
    };
  };

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
    home: team(homeComp, homeGoalsFromEvents),
    away: team(awayComp, awayGoalsFromEvents),
    events,
  };
}

let footballRangeOk = true; // falls back to per-day requests if date ranges are rejected
async function fetchFootballLeague(slug, from, to) {
  const base = `${ESPN}/site/v2/sports/soccer/${slug}/scoreboard`;
  let events = [];
  let name = null;
  if (!from || !to) {
    const d = await http(base);
    events = d.events || [];
    name = d.leagues?.[0]?.name;
  } else if (footballRangeOk) {
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
  if (from && to && !footballRangeOk) {
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
  for (const slug of hot) {
    await track(`football:${slug}`, () => fetchFootballLeague(slug));
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
          short: makeShortName(e.team?.abbreviation, e.team?.displayName),
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

function normalizeOvers(ov) {
  if (ov == null || ov === '') return null;
  const num = parseFloat(ov);
  if (isNaN(num)) return null;
  const whole = Math.floor(num);
  const balls = Math.round((num - whole) * 10);
  if (balls >= 6) {
    const extraOvers = Math.floor(balls / 6);
    const remBalls = balls % 6;
    return remBalls === 0 ? whole + extraOvers : parseFloat(`${whole + extraOvers}.${remBalls}`);
  }
  return balls === 0 ? whole : parseFloat(num.toFixed(1));
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
      if (!m) return { display: p };
      const ov = m[4] != null ? normalizeOvers(m[4]) : null;
      let disp = p;
      if (ov != null && m[4] != null && String(ov) !== m[4]) {
        disp = p.replace(new RegExp(`\\b${m[4]}\\s*ov`, 'i'), `${ov} ov`);
      }
      return {
        runs: +m[1],
        wickets: m[2] != null ? +m[2] : 10, // ESPN omits wickets when all out
        overs: ov,
        declared: !!m[3],
        target: target ? +target[1] : null,
        display: disp,
      };
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
      short: makeShortName(c.abbreviation, c.displayName),
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
      .map((i) => {
        const ov = normalizeOvers(i.overs);
        return {
          runs: i.runs ?? 0,
          wickets: i.wickets ?? 0,
          overs: ov,
          declared: !!i.isDeclared,
          display: `${i.runs ?? 0}/${i.wickets ?? 0}${i.isDeclared ? 'd' : ''}${ov != null ? ` (${ov} ov)` : ''}`,
        };
      });
  const team = (t, ts) => {
    const inn = innings(ts);
    return { name: t?.teamName, short: makeShortName(t?.teamSName, t?.teamName), score: inn.map((i) => i.display).join(' & ') || null, innings: inn };
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

const miniScoreCache = new Map();
async function getCricbuzzLive(matchId) {
  if (!matchId) return null;
  const cached = miniScoreCache.get(matchId);
  if (cached && Date.now() - cached.at < 15_000) return cached.data;
  try {
    const url = `https://www.cricbuzz.com/live-cricket-scores/${matchId}`;
    const res = await http(url, { as: 'text' });
    const chunks = [...res.matchAll(/self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)/g)].map((m) => {
      try { return JSON.parse(`"${m[1]}"`); } catch { return ''; }
    });
    const text = res + '\n' + chunks.join('');
    const idx = text.indexOf('"miniscore":');
    if (idx === -1) return null;
    const raw = sliceBalanced(text, idx + '"miniscore":'.length);
    if (!raw) return null;
    const mini = JSON.parse(raw);
    const fmtBat = (b, star = false) => {
      if (!b || !b.name) return '';
      const parts = b.name.trim().split(/\s+/);
      const short = parts.length > 1 ? `${parts[0][0]}. ${parts.slice(1).join(' ')}` : b.name;
      return `${short} ${b.runs}(${b.balls})${star ? '*' : ''}`;
    };
    const fmtBowl = (b) => {
      if (!b || !b.name) return '';
      const parts = b.name.trim().split(/\s+/);
      const short = parts.length > 1 ? `${parts[0][0]}. ${parts.slice(1).join(' ')}` : b.name;
      return `${short} ${b.wickets}/${b.runs} (${b.overs})`;
    };
    const data = {
      bat1: fmtBat(mini.batsmanStriker, true),
      bat2: fmtBat(mini.batsmanNonStriker, false),
      bowl: fmtBowl(mini.bowlerStriker),
      crr: mini.currentRunRate || (mini.overs > 0 ? +(mini.batTeam?.teamScore / mini.overs).toFixed(2) : null),
      target: mini.target || null,
      status: mini.status || null,
      recent: mini.recentOvsStats || '',
      rrr: mini.requiredRunRate != null ? Number(mini.requiredRunRate) : null,
      remRuns: mini.remRunsToWin != null ? Number(mini.remRunsToWin) : null,
      oversRem: mini.oversRem != null ? Number(mini.oversRem) : null,
      partner: mini.partnerShip ? `${mini.partnerShip.runs || 0}(${mini.partnerShip.balls || 0})` : '',
      batTeamId: mini.batTeam?.teamId != null ? String(mini.batTeam.teamId) : null,
      batTeamName: mini.batTeam?.teamName || null,
      batTeamScore: mini.batTeam?.teamScore != null ? Number(mini.batTeam.teamScore) : null,
      batTeamWkts: mini.batTeam?.teamWkts != null ? Number(mini.batTeam.teamWkts) : null,
      batTeamOvers: mini.overs != null ? normalizeOvers(mini.overs) : null,
      inningsScoreList: (mini.matchScoreDetails?.inningsScoreList || mini.batTeamScoreObj?.teamInningsArray || []).map((i) => ({
        batTeamId: i.batTeamId != null ? String(i.batTeamId) : null,
        batTeamName: i.batTeamName || null,
        score: i.score != null ? Number(i.score) : null,
        wickets: i.wickets != null ? Number(i.wickets) : null,
        overs: normalizeOvers(i.overs),
        isDeclared: !!i.isDeclared,
      })),
    };
    miniScoreCache.set(matchId, { at: Date.now(), data });
    return data;
  } catch (e) {
    return null;
  }
}

function applyLiveScoreToCricketMatch(m, live) {
  if (!m || !live || !m.teams) return;

  // 1. If live has inningsScoreList, sync each innings to matching team
  if (Array.isArray(live.inningsScoreList) && live.inningsScoreList.length > 0) {
    for (const inn of live.inningsScoreList) {
      if (inn.score == null) continue;
      const targetTeam = m.teams.find((t) => {
        if (inn.batTeamId && (String(t.id) === String(inn.batTeamId) || String(t.sourceId) === String(inn.batTeamId))) return true;
        if (inn.batTeamName) {
          const bn = normName(inn.batTeamName);
          const tn = normName(t.name);
          const ts = normName(t.short);
          return (bn && (tn.includes(bn) || bn.includes(tn) || ts === bn));
        }
        return false;
      });
      if (targetTeam) {
        if (!targetTeam.innings) targetTeam.innings = [];
        const existing = targetTeam.innings[targetTeam.innings.length - 1];
        if (existing) {
          existing.runs = inn.score;
          if (inn.wickets != null) existing.wickets = inn.wickets;
          if (inn.overs != null) existing.overs = inn.overs;
          existing.declared = inn.isDeclared;
          existing.display = `${existing.runs}/${existing.wickets ?? 0}${existing.declared ? 'd' : ''}${existing.overs != null ? ` (${existing.overs} ov)` : ''}`;
        } else {
          targetTeam.innings.push({
            runs: inn.score,
            wickets: inn.wickets ?? 0,
            overs: inn.overs ?? null,
            declared: inn.isDeclared,
            display: `${inn.score}/${inn.wickets ?? 0}${inn.isDeclared ? 'd' : ''}${inn.overs != null ? ` (${inn.overs} ov)` : ''}`,
          });
        }
        targetTeam.score = targetTeam.innings.map((i) => i.display).join(' & ');
      }
    }
  }

  // 2. Direct batting team update from live.batTeamScore
  if (live.batTeamScore != null) {
    const batTeam = m.teams.find((t) => {
      if (live.batTeamId && (String(t.id) === String(live.batTeamId) || String(t.sourceId) === String(live.batTeamId))) return true;
      if (live.batTeamName) {
        const bn = normName(live.batTeamName);
        const tn = normName(t.name);
        const ts = normName(t.short);
        return (bn && (tn.includes(bn) || bn.includes(tn) || ts === bn));
      }
      return t.batting === true;
    }) || m.teams[0];

    if (batTeam) {
      if (!batTeam.innings) batTeam.innings = [];
      const curInn = batTeam.innings[batTeam.innings.length - 1];
      if (curInn) {
        if (live.batTeamScore >= (curInn.runs || 0)) {
          curInn.runs = live.batTeamScore;
          if (live.batTeamWkts != null) curInn.wickets = live.batTeamWkts;
          if (live.batTeamOvers != null) curInn.overs = live.batTeamOvers;
          curInn.display = `${curInn.runs}/${curInn.wickets ?? 0}${curInn.declared ? 'd' : ''}${curInn.overs != null ? ` (${curInn.overs} ov)` : ''}`;
        }
      } else {
        batTeam.innings.push({
          runs: live.batTeamScore,
          wickets: live.batTeamWkts ?? 0,
          overs: live.batTeamOvers ?? null,
          declared: false,
          display: `${live.batTeamScore}/${live.batTeamWkts ?? 0}${live.batTeamOvers != null ? ` (${live.batTeamOvers} ov)` : ''}`,
        });
      }
      batTeam.score = batTeam.innings.map((i) => i.display).join(' & ');
    }
  }
}

async function cricbuzzSync() {
  const html = await http(CRICBUZZ_LIVE, { as: 'text' });
  const items = extractCricbuzz(html);
  if (!items.length) throw new Error('Page loaded but no match data found — Cricbuzz layout may have changed');
  for (const it of items) upsert(store.cricketCb, normCricbuzz(it));
  for (const m of store.cricketCb.values()) {
    if (m.status === 'finished' && m.live) delete m.live;
  }
  const liveMatches = [...store.cricketCb.values()].filter((m) => m.status === 'live');
  for (const lm of liveMatches.slice(0, 8)) {
    const live = await getCricbuzzLive(lm.sourceId);
    if (live) {
      lm.live = live;
      applyLiveScoreToCricketMatch(lm, live);
    }
  }
  prune(store.cricketCb);
  return `${items.length} matches`;
}

// ═══════════════════════ CRICKET — MERGE SOURCES ══════════════════════
const balls = (o) => {
  if (o == null) return 0;
  const num = normalizeOvers(o);
  if (num == null) return 0;
  const whole = Math.floor(num);
  const b = Math.round((num - whole) * 10);
  return whole * 6 + b;
};
const progress = (m) =>
  m.teams.reduce((s, t) => s + (t.innings || []).reduce((x, i) => x + balls(i.overs) * 1000 + (i.runs || 0), 0), 0);

function matchKeys(m) {
  const day = (m.startTime || '').slice(0, 10);
  const pair = (f) => (m.teams || []).map((t) => normName(f(t))).filter(Boolean).sort().join('|');
  const pName = pair((t) => t.name);
  const pShort = pair((t) => t.short);
  const keys = [];
  for (const p of [pName, pShort]) {
    if (!p || !p.includes('|')) continue;
    if (day) {
      keys.push(`${p}@${day}`);
      try {
        const d = new Date(m.startTime);
        keys.push(`${p}@${addDays(d, -1).toISOString().slice(0, 10)}`);
        keys.push(`${p}@${addDays(d, 1).toISOString().slice(0, 10)}`);
      } catch {}
    }
    if (m.status === 'live') {
      keys.push(`${p}@live`);
    }
  }
  return [...new Set(keys)];
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
    const cbMoreProgress = progress(cb) > progress(target);
    const cbFinished = cb.status === 'finished';
    if (cb.status !== 'scheduled' && (cbMoreProgress || cbFinished)) {
      target.teams = target.teams.map((t) => {
        const alt = cb.teams.find((x) => normName(x.name) === normName(t.name) || normName(x.short) === normName(t.short));
        return alt ? { ...t, score: alt.score, innings: alt.innings } : t;
      });
      target.statusText = cb.statusText || target.statusText;
      target.scoreSource = 'cricbuzz';
      if (cbFinished || target.status === 'scheduled') target.status = cb.status;
    }
    if (cb.live && target.status !== 'finished') target.live = cb.live;
    if (target.status === 'finished') {
      delete target.live;
      delete cb.live;
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
  const rawTeam = q.teams || q.team || '';
  if (rawTeam) {
    const teamList = String(rawTeam)
      .split(/[,;]/)
      .map((s) => normName(s.trim()))
      .filter(Boolean);
    if (teamList.length > 0) {
      out = out.filter((m) =>
        (m.teams || [m.home, m.away]).some((x) => {
          const xn = normName(x?.name);
          const xs = normName(x?.short);
          return teamList.some((t) => (xn && xn.includes(t)) || (xs && xs === t));
        })
      );
    }
  }
  const dir = kind === 'results' ? -1 : 1;
  const qHasU19 = /u19|under-19|under 19/i.test(rawTeam);
  const qHasWom = /women|wmn/i.test(rawTeam);
  return out.map(clean).sort((a, b) => {
    const aLive = a.status === 'live' ? 1 : 0;
    const bLive = b.status === 'live' ? 1 : 0;
    if (aLive !== bLive) return bLive - aLive;

    const aU19 = /u19|under-19|under 19/i.test(a.title + (a.format || '')) ? 1 : 0;
    const bU19 = /u19|under-19|under 19/i.test(b.title + (b.format || '')) ? 1 : 0;
    if (!qHasU19 && aU19 !== bU19) return aU19 - bU19;

    const aWom = /women|wmn/i.test(a.title + (a.format || '')) ? 1 : 0;
    const bWom = /women|wmn/i.test(b.title + (b.format || '')) ? 1 : 0;
    if (!qHasWom && aWom !== bWom) return aWom - bWom;

    return dir * (new Date(a.startTime) - new Date(b.startTime));
  });
}

const hasLiveCricket = () => [...store.cricketEspn.values(), ...store.cricketCb.values()].some(isHot);
const h = (fn) => (req, res) => Promise.resolve(fn(req, res)).catch((e) => res.status(502).json({ error: e.message }));

// ═══════════════════════ EMO COMPACT TRANSFORMERS ═════════════════════
function calculateWinProb(m) {
  if (!m || m.status !== 'live') return 50;
  const target = m.live?.target || m.target;
  const remRuns = m.live?.remRuns;
  const oversRem = m.live?.oversRem;
  const rrr = m.live?.rrr;
  const crr = m.live?.crr;

  if (target && remRuns != null && oversRem != null && oversRem > 0) {
    const ballsRem = oversRem * 6;
    if (remRuns <= 0) return 2;
    const t2Batting = m.teams?.[1]?.batting === true || m.live?.batTeamId === String(m.teams?.[1]?.id);
    const wicketsLost = m.live?.batTeamWkts ?? 3;
    const wicketsLeft = Math.max(0, 10 - wicketsLost);
    if (wicketsLeft <= 0) return t2Batting ? 98 : 2;

    const baselineRRR = 7.5;
    const diff = (rrr || (remRuns / (ballsRem / 6))) - baselineRRR;
    let chaseProb = 50 - diff * 7 + (wicketsLeft - 5) * 5;
    chaseProb = Math.max(2, Math.min(98, Math.round(chaseProb)));
    return t2Batting ? (100 - chaseProb) : chaseProb;
  }

  if (crr && crr > 0) {
    const fmt = (m.format || '').toUpperCase();
    const par = fmt.includes('T20') ? 8.2 : (fmt.includes('TEST') ? 3.2 : 5.6);
    const t1Batting = m.teams?.[0]?.batting !== false;
    let prob = 50 + (crr - par) * 10;
    prob = Math.max(15, Math.min(85, Math.round(prob)));
    return t1Batting ? prob : (100 - prob);
  }
  return 50;
}

function toEmoCricketMatch(m) {
  if (!m) return null;
  const isFinished = (m.status === 'finished');
  if (!isFinished && m.live) applyLiveScoreToCricketMatch(m, m.live);
  const allInnings = (m.teams || []).flatMap((t) => t.innings || []);
  const currentInnings = allInnings[allInnings.length - 1];
  const hasExplicitBatting = (m.teams || []).some((t) => t.batting === true);
  return {
    id: m.id || '',
    sport: 'cricket',
    title: m.title || '',
    status: m.status || 'scheduled',
    statusText: isFinished ? (m.statusText || 'FT') : (m.live?.status || m.statusText || ''),
    format: m.format || '',
    startTime: m.startTime || '',
    venue: m.venue || '',
    series: m.series?.name || '',
    target: isFinished ? null : (m.live?.target ?? currentInnings?.target ?? null),
    bat1: isFinished ? '' : (m.live?.bat1 || ''),
    bat2: isFinished ? '' : (m.live?.bat2 || ''),
    bowl: isFinished ? '' : (m.live?.bowl || ''),
    crr: isFinished ? null : (m.live?.crr ?? null),
    recent: isFinished ? '' : (m.live?.recent || ''),
    rrr: isFinished ? null : (m.live?.rrr ?? null),
    remRuns: isFinished ? null : (m.live?.remRuns ?? null),
    oversRem: isFinished ? null : (m.live?.oversRem ?? null),
    partner: isFinished ? '' : (m.live?.partner || ''),
    winProb: isFinished ? null : calculateWinProb(m),
    teams: (m.teams || []).slice(0, 2).map((t, idx) => {
      let isBatting = isFinished ? false : (t.batting === true);
      if (!isFinished && !hasExplicitBatting && m.status === 'live') {
        if (m.live?.batTeamId && (String(t.id) === m.live.batTeamId || String(t.sourceId) === m.live.batTeamId)) {
          isBatting = true;
        } else if (m.live?.batTeamName && normName(t.name).includes(normName(m.live.batTeamName))) {
          isBatting = true;
        } else if (!m.live?.batTeamId && !m.live?.batTeamName) {
          const t2HasInn = (m.teams[1]?.innings || []).length > 0;
          isBatting = (idx === 1 && t2HasInn) || (idx === 0 && !t2HasInn);
        }
      }
      return {
        name: t.name || '',
        short: makeShortName(t.short, t.name),
        batting: isBatting,
        score: t.score || '',
        innings: (t.innings || []).slice(0, 2).map((i) => ({
          runs: i.runs ?? 0,
          wickets: i.wickets ?? 0,
          overs: normalizeOvers(i.overs),
          target: i.target ?? null,
        })),
      };
    }),
  };
}

function toEmoFootballMatch(m) {
  if (!m) return null;
  const homeGoals = [];
  const awayGoals = [];
  let homeGoalCount = 0;
  let awayGoalCount = 0;
  for (const e of (m.events || [])) {
    if (e.goal || /goal|penalty/i.test(e.type || '')) {
      const athlete = e.player || '';
      const p = athlete ? athlete.trim().split(/\s+/).pop() : 'Goal';
      const tag = e.ownGoal ? '(OG)' : (e.penalty ? '(P)' : '');
      const min = e.minute ? `${e.minute}'` : '';
      const str = `${p} ${min}${tag}`.trim();
      if (String(e.teamId) === String(m.home?.id)) {
        homeGoalCount++;
        homeGoals.push(str);
      } else if (String(e.teamId) === String(m.away?.id)) {
        awayGoalCount++;
        awayGoals.push(str);
      }
    }
  }

  let hScore = m.home?.score != null && !isNaN(Number(m.home.score)) ? Number(m.home.score) : null;
  let aScore = m.away?.score != null && !isNaN(Number(m.away.score)) ? Number(m.away.score) : null;
  if (m.status !== 'scheduled') {
    if (hScore == null && homeGoalCount > 0) hScore = homeGoalCount;
    if (aScore == null && awayGoalCount > 0) aScore = awayGoalCount;
  }

  return {
    id: m.id || '',
    sport: 'football',
    title: m.title || (m.home && m.away ? `${m.home.name || ''} vs ${m.away.name || ''}` : ''),
    startTime: m.startTime || '',
    status: m.status || 'scheduled',
    statusText: m.statusText || '',
    clock: m.clock || null,
    venue: m.venue || '',
    league: m.league?.name || '',
    leagueSlug: m.league?.slug || '',
    home: {
      name: m.home?.name || '',
      short: makeShortName(m.home?.short, m.home?.name),
      score: hScore,
      form: m.home?.form || '',
      scorers: homeGoals.slice(0, 4).join(', '),
    },
    away: {
      name: m.away?.name || '',
      short: makeShortName(m.away?.short, m.away?.name),
      score: aScore,
      form: m.away?.form || '',
      scorers: awayGoals.slice(0, 4).join(', '),
    },
  };
}

function toEmoStandings(groups) {
  return (groups || []).map((g) => ({
    group: g.group || null,
    table: (g.table || []).slice(0, 20).map((e) => ({
      rank: e.rank,
      short: makeShortName(e.short, e.team),
      played: e.played ?? 0,
      gd: e.gd ?? 0,
      points: e.points ?? 0,
    })),
  }));
}

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
      '--- EmoDesk Compact Endpoints (essential data only) ---',
      'GET /api/emo/health',
      'GET /api/emo/live',
      'GET /api/emo/stream',
      'GET /api/emo/football/{live|fixtures|results|all}',
      'GET /api/emo/football/leagues',
      'GET /api/emo/football/match/:id',
      'GET /api/emo/football/standings/:league',
      'GET /api/emo/cricket/{live|fixtures|results|all}',
      'GET /api/emo/cricket/series',
      'GET /api/emo/cricket/match/:id',
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

app.get('/api/live', async (req, res) => {
  if (store.football.size === 0) { try { await footballLiveSync(); } catch {} }
  if (store.cricketEspn.size === 0 && store.cricketCb.size === 0) { try { await cricketSync(); } catch {} }
  res.json({
    football: filterMatches([...store.football.values()], req.query, 'live'),
    cricket: filterMatches(cricketMatches(), req.query, 'live'),
  });
});

for (const kind of ['live', 'fixtures', 'results', 'all']) {
  app.get(`/api/football/${kind}`, async (req, res) => {
    const league = req.query.league;
    const hasLeague = !league || [...store.football.values()].some((m) => m.league?.slug === league);
    if (store.football.size === 0 || !hasLeague) {
      try {
        if (league && CONFIG.football.leagues[league]) await fetchFootballLeague(league);
        else if (store.football.size === 0) await footballScheduleSync();
      } catch {}
    }
    res.json(filterMatches([...store.football.values()], req.query, kind));
  });
  app.get(`/api/cricket/${kind}`, async (req, res) => {
    if (store.cricketEspn.size === 0 && store.cricketCb.size === 0) {
      try { await cricketSync(); } catch {}
    }
    res.json(filterMatches(cricketMatches(), req.query, kind));
  });
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

app.get('/api/cricket/debug', (req, res) => res.json({ ok: true, server: 'EmoDesk Sports Server', endpoints: '/api' }));
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

// ═══════════════════ EMO COMPACT ROUTES (/api/emo/...) ════════════════
app.get('/api/emo/health', (req, res) =>
  res.json({ ok: true, uptime: Math.round(process.uptime()) })
);

app.get('/api/emo/live', async (req, res) => {
  if (store.football.size === 0) { try { await footballLiveSync(); } catch {} }
  if (store.cricketEspn.size === 0 && store.cricketCb.size === 0) { try { await cricketSync(); } catch {} }
  res.json({
    football: filterMatches([...store.football.values()], req.query, 'live').map(toEmoFootballMatch),
    cricket: filterMatches(cricketMatches(), req.query, 'live').map(toEmoCricketMatch),
  });
});

for (const kind of ['live', 'fixtures', 'results', 'all']) {
  app.get(`/api/emo/football/${kind}`, async (req, res) => {
    const league = req.query.league;
    const hasLeague = !league || [...store.football.values()].some((m) => m.league?.slug === league);
    if (store.football.size === 0 || !hasLeague) {
      try {
        if (league && CONFIG.football.leagues[league]) await fetchFootballLeague(league);
        else if (store.football.size === 0) await footballScheduleSync();
      } catch {}
    }
    res.json(
      filterMatches([...store.football.values()], req.query, kind)
        .slice(0, req.query.limit ? parseInt(req.query.limit) : 12)
        .map(toEmoFootballMatch)
    );
  });
  app.get(`/api/emo/cricket/${kind}`, async (req, res) => {
    if (store.cricketEspn.size === 0 && store.cricketCb.size === 0) {
      try { await cricketSync(); } catch {}
    }
    res.json(
      filterMatches(cricketMatches(), req.query, kind)
        .slice(0, req.query.limit ? parseInt(req.query.limit) : 30)
        .map(toEmoCricketMatch)
    );
  });
}

app.get('/api/emo/football/leagues', (req, res) =>
  res.json(Object.entries(CONFIG.football.leagues).map(([slug, name]) => ({ slug, name })))
);

app.get('/api/emo/football/match/:id', h(async (req, res) => {
  const m = store.football.get(req.params.id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  res.json(toEmoFootballMatch(m));
}));

app.get('/api/emo/football/standings/:league', h(async (req, res) => {
  const data = await cached(`st:${req.params.league}`, 30 * 60_000, () => footballStandings(req.params.league));
  res.json(toEmoStandings(data));
}));

app.get('/api/emo/cricket/debug', (req, res) => res.json({ ok: true, server: 'EmoDesk Sports Server', endpoints: '/api/emo' }));
app.get('/api/emo/cricket/series', (req, res) =>
  res.json([...store.cricketSeries.values()].map(({ id, name }) => ({ id, name })))
);

app.get('/api/emo/cricket/match/:id', h(async (req, res) => {
  const id = req.params.id;
  const m = cricketMatches().find((x) => x.id === id || `cr_cb_${x.cricbuzzId}` === id);
  if (!m) return res.status(404).json({ error: 'Match not found' });
  res.json(toEmoCricketMatch(m));
}));

app.get('/api/emo/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  const sport = ['cricket', 'football'].includes(req.query.sport) ? req.query.sport : null;
  res.write(
    `event: snapshot\ndata: ${JSON.stringify({
      football: sport === 'cricket' ? [] : filterMatches([...store.football.values()], {}, 'live').map(toEmoFootballMatch),
      cricket: sport === 'football' ? [] : filterMatches(cricketMatches(), {}, 'live').map(toEmoCricketMatch),
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