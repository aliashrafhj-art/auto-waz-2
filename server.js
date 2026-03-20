'use strict';
// ============================================================
//  AUTO WAZ 2.0 — Complete Video Automation Tool
//  Sections: Setup → Tokens → Config → Queue → FFmpeg
//            → MovieSlicer → TrollEdit → DriveUtils
//            → Downloader → Scheduler → Routes → OAuth → Startup
// ============================================================

const express  = require('express');
const fs       = require('fs');
const path     = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);

const app  = express();
const PORT = process.env.PORT || 3000;

const DIR = {
  temp:   path.join(__dirname, 'temp'),
  public: path.join(__dirname, 'public'),
  assets: path.join(__dirname, 'assets'),
};
const FILE = {
  tokens:  path.join(__dirname, 'tokens.json'),
  config:  path.join(__dirname, 'config.json'),
  queue:   path.join(__dirname, 'queue.json'),
  phonks:  path.join(__dirname, 'phonks.json'),
};

// Create required directories
[DIR.temp, DIR.assets].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });

app.use(express.json({ limit: '20mb' }));   // Allow skull image base64
app.use(express.static(DIR.public));
app.use('/assets', express.static(DIR.assets));

// ============================================================
// SECTION 1: HELPERS
// ============================================================
const LOGS = [];
function log(msg) {
  const line = `[${bdTime()}] ${msg}`;
  console.log(line);
  LOGS.unshift(line);
  if (LOGS.length > 400) LOGS.length = 400;
}
function bdTime() {
  return new Date(Date.now() + 6 * 3600000).toISOString().replace('T', ' ').slice(0, 19);
}
function bdMinutes() {
  return Math.floor((Date.now() + 6 * 3600000) / 60000) % 1440;
}
function bdDay() {
  return new Date().toLocaleDateString('en-US', { weekday: 'long', timeZone: 'Asia/Dhaka' });
}
function readJSON(file, fallback = {}) {
  try { if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8')); } catch {}
  return fallback;
}
function writeJSON(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}
function tmpPath(prefix, ext = 'mp4') {
  return path.join(DIR.temp, `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}.${ext}`);
}
function cleanFiles(...files) {
  files.forEach(f => { try { if (f && fs.existsSync(f)) fs.unlinkSync(f); } catch {} });
}
function htmlPage(icon, title, sub = '') {
  return `<!DOCTYPE html><html><body style="background:#0a0a0f;color:${icon==='✅'?'#06d6a0':'#ff4444'};font-family:sans-serif;text-align:center;padding:60px">
<div style="font-size:56px">${icon}</div><h2>${title}</h2>${sub ? `<p style="color:#aaa">${sub}</p>` : ''}
<p style="color:#555;font-size:13px;margin-top:20px">পেজ বন্ধ করুন</p>
<script>setTimeout(()=>window.close(),2500)</script></body></html>`;
}

let _fetch = null;
async function gFetch() {
  if (!_fetch) _fetch = (await import('node-fetch')).default;
  return _fetch;
}

// ============================================================
// SECTION 2: TOKEN SYSTEM
// ============================================================
const tokenExpiry = { yt: 0, drive: 0 };
const DRIVE_TOKEN_BACKUP = 'autowaz2_tokens.json';
let tokenDriveFileId = null;

function loadTokens()   { return readJSON(FILE.tokens, {}); }
function saveTokens(t)  {
  writeJSON(FILE.tokens, t);
  if (t.drive_access_token) _backupTokens(t, t.drive_access_token).catch(() => {});
}

async function _backupTokens(tokens, driveToken) {
  try {
    const fetch = await gFetch();
    const body  = JSON.stringify(tokens, null, 2);
    if (tokenDriveFileId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${tokenDriveFileId}?uploadType=media`, {
        method: 'PATCH', headers: { 'Authorization': `Bearer ${driveToken}`, 'Content-Type': 'application/json' }, body
      });
      if (!r.ok) tokenDriveFileId = null;
    } else {
      const bnd = 'tkbnd';
      const meta = JSON.stringify({ name: DRIVE_TOKEN_BACKUP, mimeType: 'application/json' });
      const req  = `--${bnd}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${bnd}\r\nContent-Type: application/json\r\n\r\n${body}\r\n--${bnd}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method: 'POST', headers: { 'Authorization': `Bearer ${driveToken}`, 'Content-Type': `multipart/related; boundary=${bnd}` }, body: req
      });
      if (r.ok) { const d = await r.json(); if (d.id) { tokenDriveFileId = d.id; log('[TOKEN] Drive backup ✅'); } }
    }
  } catch (e) { console.warn('[TOKEN] Backup:', e.message); }
}

async function refreshYT() {
  try {
    const fetch = await gFetch(), t = loadTokens();
    if (!t.refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ refresh_token: t.refresh_token, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, grant_type: 'refresh_token' })
    });
    const d = await r.json();
    if (d.access_token) {
      tokenExpiry.yt = Date.now() + (d.expires_in || 3600) * 1000 - 300000;
      const u = { ...t, access_token: d.access_token };
      writeJSON(FILE.tokens, u);
      if (u.drive_access_token) _backupTokens(u, u.drive_access_token).catch(() => {});
      log('[TOKEN] YT refresh ✅');
      return d.access_token;
    }
    log('[TOKEN] YT refresh fail: ' + JSON.stringify(d));
  } catch (e) { log('[TOKEN] YT error: ' + e.message); }
  return null;
}

async function refreshDrive() {
  try {
    const fetch = await gFetch(), t = loadTokens();
    if (!t.drive_refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ refresh_token: t.drive_refresh_token, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, grant_type: 'refresh_token' })
    });
    const d = await r.json();
    if (d.access_token) {
      tokenExpiry.drive = Date.now() + (d.expires_in || 3600) * 1000 - 300000;
      const u = { ...t, drive_access_token: d.access_token };
      writeJSON(FILE.tokens, u);
      _backupTokens(u, d.access_token).catch(() => {});
      log('[TOKEN] Drive refresh ✅');
      return d.access_token;
    }
    log('[TOKEN] Drive refresh fail: ' + JSON.stringify(d));
  } catch (e) { log('[TOKEN] Drive error: ' + e.message); }
  return null;
}

async function getYTToken() {
  if (Date.now() < tokenExpiry.yt) { const t = loadTokens(); if (t.access_token) return t.access_token; }
  return (await refreshYT()) || loadTokens().access_token || null;
}
async function getDriveToken() {
  if (Date.now() < tokenExpiry.drive) { const t = loadTokens(); if (t.drive_access_token) return t.drive_access_token; }
  return (await refreshDrive()) || loadTokens().drive_access_token || null;
}

async function restoreTokens() {
  try {
    const fetch = await gFetch();
    const t     = loadTokens();
    const bt    = t.drive_access_token || process.env.DRIVE_ACCESS_TOKEN;
    if (!bt) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${DRIVE_TOKEN_BACKUP}' and trashed=false&fields=files(id)&pageSize=1`, { headers: { 'Authorization': `Bearer ${bt}` } });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    tokenDriveFileId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${tokenDriveFileId}?alt=media`, { headers: { 'Authorization': `Bearer ${bt}` } });
    if (!fr.ok) return;
    const saved = await fr.json();
    if (saved?.refresh_token || saved?.drive_refresh_token) { writeJSON(FILE.tokens, saved); log('[TOKEN] Restored ✅'); }
  } catch (e) { console.warn('[TOKEN] Restore:', e.message); }
}

// ============================================================
// SECTION 3: CONFIG SYSTEM
// ============================================================
const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const DRIVE_CONFIG_NAME = 'autowaz2_config.json';
let cfgDriveId = null;

function defaultDayCfg() { return { folderId: '', template: '', enabled: true, target: '' }; }
function defaultCfg() {
  const days = {};
  DAYS.forEach(d => { days[d] = defaultDayCfg(); });
  return { enabled: false, slots: [], privacy: 'public', target: 'youtube', days,
    troll: { enabled: false, colorFilter: 'none', loopAudio: false, textOverlay: 'Wait for it... 💀', skullDriveId: '' },
    movie: { colorGrade: 'teal_orange', headBob: true, headBobIntensity: 2, clipDuration: 60, clipCount: 5, dialogVol: 1.0, musicVol: 0.5 }
  };
}
function loadCfg() {
  const s = readJSON(FILE.config, null);
  if (!s) return defaultCfg();
  const def = defaultCfg();
  const days = { ...def.days };
  if (s.days) DAYS.forEach(d => { days[d] = { ...defaultDayCfg(), ...(s.days[d] || {}) }; });
  return { ...def, ...s, days, troll: { ...def.troll, ...(s.troll || {}) }, movie: { ...def.movie, ...(s.movie || {}) } };
}
function saveCfg(cfg) {
  writeJSON(FILE.config, cfg);
  _backupCfg(cfg).catch(() => {});
}
async function _backupCfg(cfg) {
  try {
    const fetch = await gFetch(), t = loadTokens(), dt = t.drive_access_token;
    if (!dt) return;
    const body = JSON.stringify(cfg, null, 2);
    if (cfgDriveId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${cfgDriveId}?uploadType=media`, {
        method: 'PATCH', headers: { 'Authorization': `Bearer ${dt}`, 'Content-Type': 'application/json' }, body
      });
      if (!r.ok) cfgDriveId = null;
    } else {
      const bnd = 'cfgbnd', meta = JSON.stringify({ name: DRIVE_CONFIG_NAME });
      const req  = `--${bnd}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${bnd}\r\nContent-Type: application/json\r\n\r\n${body}\r\n--${bnd}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method: 'POST', headers: { 'Authorization': `Bearer ${dt}`, 'Content-Type': `multipart/related; boundary=${bnd}` }, body: req
      });
      if (r.ok) { const d = await r.json(); if (d.id) cfgDriveId = d.id; }
    }
  } catch {}
}
async function restoreCfg() {
  try {
    const fetch = await gFetch(), dt = await getDriveToken();
    if (!dt) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${DRIVE_CONFIG_NAME}' and trashed=false&fields=files(id)&pageSize=1`, { headers: { 'Authorization': `Bearer ${dt}` } });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    cfgDriveId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${cfgDriveId}?alt=media`, { headers: { 'Authorization': `Bearer ${dt}` } });
    if (!fr.ok) return;
    const cfg = await fr.json();
    if (cfg && typeof cfg === 'object') { writeJSON(FILE.config, cfg); log('[CONFIG] Restored ✅'); }
  } catch {}
}

// ============================================================
// SECTION 4: QUEUE SYSTEM (Random FIFO, Drive-backed)
// ============================================================
const DRIVE_QUEUE_NAME = 'autowaz2_queue.json';
let queueDriveId = null;

function loadQueue() { return readJSON(FILE.queue, {}); }
function saveQueue(q) {
  writeJSON(FILE.queue, q);
  _backupQueue(q).catch(() => {});
}
async function _backupQueue(q) {
  try {
    const fetch = await gFetch(), t = loadTokens(), dt = t.drive_access_token;
    if (!dt) return;
    const body = JSON.stringify(q, null, 2);
    if (queueDriveId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${queueDriveId}?uploadType=media`, {
        method: 'PATCH', headers: { 'Authorization': `Bearer ${dt}`, 'Content-Type': 'application/json' }, body
      });
      if (!r.ok) queueDriveId = null;
    } else {
      const bnd = 'qbnd', meta = JSON.stringify({ name: DRIVE_QUEUE_NAME });
      const req  = `--${bnd}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${bnd}\r\nContent-Type: application/json\r\n\r\n${body}\r\n--${bnd}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method: 'POST', headers: { 'Authorization': `Bearer ${dt}`, 'Content-Type': `multipart/related; boundary=${bnd}` }, body: req
      });
      if (r.ok) { const d = await r.json(); if (d.id) queueDriveId = d.id; }
    }
  } catch {}
}
async function restoreQueue() {
  try {
    const fetch = await gFetch(), dt = await getDriveToken();
    if (!dt) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${DRIVE_QUEUE_NAME}' and trashed=false&fields=files(id)&pageSize=1`, { headers: { 'Authorization': `Bearer ${dt}` } });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    queueDriveId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${queueDriveId}?alt=media`, { headers: { 'Authorization': `Bearer ${dt}` } });
    if (!fr.ok) return;
    const q = await fr.json();
    if (q && typeof q === 'object') { writeJSON(FILE.queue, q); log('[QUEUE] Restored ✅'); }
  } catch {}
}

// Random FIFO: shuffle on new cycle, no repeats until all played
function nextVideo(folderId, videos) {
  const q = loadQueue();
  if (!q[folderId]) q[folderId] = { remaining: [], used: [] };
  const allIds  = videos.map(v => v.id);
  let remaining = (q[folderId].remaining || []).filter(id => allIds.includes(id));
  if (!remaining.length) {
    remaining = [...allIds].sort(() => Math.random() - 0.5);
    q[folderId].used = [];
    log(`[QUEUE] New cycle: ${remaining.length} videos`);
  }
  const nextId = remaining.shift();
  q[folderId].remaining = remaining;
  q[folderId].used      = [...(q[folderId].used || []).slice(-1000), nextId];
  saveQueue(q);
  return videos.find(v => v.id === nextId) || videos[0];
}

// ============================================================
// SECTION 5: FFMPEG UTILITIES
// ============================================================
async function getduration(filePath) {
  try {
    const { stdout } = await execAsync(`ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${filePath}"`);
    return parseFloat(stdout.trim()) || 0;
  } catch { return 0; }
}

// High-quality cinematic color grades
const GRADES = {
  none:         null,
  teal_orange:  `curves=r='0/0 0.25/0.28 0.5/0.55 0.75/0.8 1/1':g='0/0 0.5/0.5 1/0.97':b='0/0 0.25/0.22 0.5/0.44 0.75/0.68 1/0.85',colorbalance=ss=-0.06:ms=0.04:hs=0.08:bs=0.1,eq=contrast=1.06:saturation=1.15`,
  moody:        `curves=all='0/0.02 0.3/0.23 0.6/0.5 0.8/0.72 1/0.9',colorbalance=bs=0.06:bh=-0.04,eq=contrast=1.2:saturation=0.82:brightness=-0.03`,
  warm_film:    `curves=r='0/0.04 0.5/0.58 1/1':g='0/0 0.5/0.5 1/0.94':b='0/0 0.5/0.4 1/0.78',eq=saturation=1.12:contrast=1.04`,
  cold_blue:    `colorbalance=bs=0.14:bm=0.08:bh=0.05:rs=-0.06:rm=-0.04,eq=saturation=0.88:contrast=1.1`,
  vintage:      `curves=r='0/0.05 0.5/0.54 1/0.94':g='0/0 0.5/0.49 1/0.9':b='0/0.08 0.5/0.44 1/0.73',eq=saturation=0.72:contrast=1.1,vignette=angle=PI/4:mode=backward`,
  neon_noir:    `eq=saturation=1.9:contrast=1.3:brightness=-0.08,colorbalance=rs=-0.12:bs=0.12:rh=0.18:bh=-0.08`,
  golden_hour:  `curves=r='0/0.08 0.5/0.64 1/1':g='0/0.02 0.5/0.51 1/0.92':b='0/0 0.5/0.34 1/0.68',eq=saturation=1.3:contrast=1.06`,
  dream:        `gblur=sigma=0.8,curves=all='0/0.05 0.5/0.52 1/0.92',eq=saturation=1.2:brightness=0.02`,
};

// Build head bob video filter
// Scales up by (intensity)% then uses sinusoidal crop to create up-down motion
function headBobFilter(intensity = 2, bpm = 120) {
  const scale  = 1 + (intensity / 100) * 2;           // e.g. intensity=2 → scale=1.04
  const scaleP = (scale * 100).toFixed(0);
  const freq   = (bpm / 60 / 2).toFixed(3);           // beats per second / 2 for smooth bob
  // Scale up, then sinusoidal crop
  return `scale=trunc(iw*${scale.toFixed(3)}/2)*2:trunc(ih*${scale.toFixed(3)}/2)*2,` +
         `crop=trunc(iw/${scale.toFixed(3)}/2)*2:trunc(ih/${scale.toFixed(3)}/2)*2:` +
         `x='(iw-ow)/2':` +
         `y='(ih-oh)/2+oh*${(intensity/100*0.015).toFixed(4)}*sin(2*PI*t*${freq})'`;
}

// ============================================================
// SECTION 6: MOVIE SLICER
// ============================================================
const movieJobs = {};

async function runMovieSlicer(jobId, opts) {
  const { url, driveFileId, clipDuration, clipCount, colorGrade, useHeadBob, headBobIntensity,
          dialogVol, musicFileId, musicVol, outputFolderId, driveToken } = opts;
  const job = movieJobs[jobId];

  const updateJob = (status, extra = {}) => {
    Object.assign(movieJobs[jobId], { status, ...extra });
    log(`[MOVIE] ${jobId}: ${status}`);
  };

  let srcFile = null, musicFile = null;
  const clipFiles = [];

  try {
    const fetch = await gFetch();

    // === Download source ===
    updateJob('downloading', { step: 'Source download হচ্ছে...' });
    srcFile = tmpPath('movie_src', 'mp4');

    if (url) {
      // Direct URL or yt-dlp
      try {
        // Try direct download first (for direct video URLs)
        const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
        if (r.ok && (r.headers.get('content-type') || '').includes('video')) {
          await new Promise((res, rej) => {
            const dest = fs.createWriteStream(srcFile);
            r.body.pipe(dest);
            dest.on('finish', res); dest.on('error', rej); r.body.on('error', rej);
          });
        } else throw new Error('Not a direct video URL');
      } catch {
        // Fallback to yt-dlp
        const dlId = Date.now();
        const tplBase = path.join(DIR.temp, `yt_src_${dlId}`);
        await execAsync(`yt-dlp -f "bestvideo[ext=mp4]+bestaudio/best[ext=mp4]/best" --merge-output-format mp4 --no-playlist -o "${tplBase}.%(ext)s" "${url}"`, { timeout: 300000 });
        // Find the downloaded file
        const files = fs.readdirSync(DIR.temp).filter(f => f.startsWith(`yt_src_${dlId}`) && f.endsWith('.mp4'));
        if (!files.length) throw new Error('yt-dlp download failed');
        srcFile = path.join(DIR.temp, files[0]);
      }
    } else if (driveFileId) {
      const r = await fetch(`https://www.googleapis.com/drive/v3/files/${driveFileId}?alt=media`, { headers: { 'Authorization': `Bearer ${driveToken}` } });
      if (!r.ok) throw new Error('Drive download failed: ' + r.status);
      await new Promise((res, rej) => {
        const dest = fs.createWriteStream(srcFile);
        r.body.pipe(dest); dest.on('finish', res); dest.on('error', rej); r.body.on('error', rej);
      });
    } else throw new Error('url বা driveFileId দাও');

    const totalDur = await getduration(srcFile);
    if (!totalDur) throw new Error('Video duration পাওয়া যায়নি');
    log(`[MOVIE] Source duration: ${totalDur.toFixed(1)}s`);

    // === Download music if provided ===
    if (musicFileId) {
      updateJob('processing', { step: 'Music download হচ্ছে...' });
      musicFile = tmpPath('music', 'mp3');
      const mr = await fetch(`https://www.googleapis.com/drive/v3/files/${musicFileId}?alt=media`, { headers: { 'Authorization': `Bearer ${driveToken}` } });
      if (!mr.ok) throw new Error('Music Drive download failed');
      await new Promise((res, rej) => { const d = fs.createWriteStream(musicFile); mr.body.pipe(d); d.on('finish', res); d.on('error', rej); mr.body.on('error', rej); });
    }

    // === Calculate clip segments ===
    const actualClipDur = Math.min(clipDuration, totalDur);
    const maxClips      = Math.floor(totalDur / actualClipDur);
    const finalCount    = Math.min(clipCount, maxClips || 1);
    const step          = (totalDur - actualClipDur) / Math.max(finalCount - 1, 1);

    log(`[MOVIE] ${finalCount} clips × ${actualClipDur}s (source: ${totalDur.toFixed(1)}s)`);

    // === Build filter chain ===
    const gradeFilter = GRADES[colorGrade] || null;
    const bobFilter   = useHeadBob ? headBobFilter(headBobIntensity || 2) : null;

    let videoFilters = [];
    if (gradeFilter) videoFilters.push(gradeFilter);
    if (bobFilter)   videoFilters.push(bobFilter);
    const vf = videoFilters.length ? videoFilters.join(',') : null;

    // Audio: dialog volume + optional music mix
    // dialogVol: 0-2 (1 = original), musicVol: 0-2
    const buildAudioFilter = (hasMusicInput) => {
      if (hasMusicInput) {
        // Mix dialog (input 0 audio) + music (input 1 audio)
        return `[0:a]volume=${dialogVol.toFixed(2)}[dia];[1:a]aloop=loop=-1:size=2000000000,atrim=end=${actualClipDur},asetpts=PTS-STARTPTS,volume=${musicVol.toFixed(2)}[mus];[dia][mus]amix=inputs=2:duration=first[outa]`;
      } else {
        return `[0:a]volume=${dialogVol.toFixed(2)}[outa]`;
      }
    };

    // === Process each clip ===
    const uploadedClips = [];
    for (let i = 0; i < finalCount; i++) {
      const startTime = Math.min(step * i, totalDur - actualClipDur);
      const clipFile  = tmpPath(`clip_${i}`, 'mp4');
      clipFiles.push(clipFile);

      updateJob('processing', { step: `Clip ${i + 1}/${finalCount} তৈরি হচ্ছে...`, progress: Math.round((i / finalCount) * 70) });

      const inputs = musicFile
        ? `-ss ${startTime.toFixed(3)} -t ${actualClipDur} -i "${srcFile}" -i "${musicFile}"`
        : `-ss ${startTime.toFixed(3)} -t ${actualClipDur} -i "${srcFile}"`;

      let cmd;
      if (vf && musicFile) {
        const af = buildAudioFilter(true);
        cmd = `ffmpeg -y ${inputs} -filter_complex "${af}" -vf "${vf}" -map 0:v -map "[outa]" -c:v libx264 -preset fast -crf 20 -c:a aac -b:a 192k -movflags +faststart "${clipFile}"`;
      } else if (vf) {
        const af = buildAudioFilter(false);
        cmd = `ffmpeg -y ${inputs} -filter_complex "${af}" -vf "${vf}" -map 0:v -map "[outa]" -c:v libx264 -preset fast -crf 20 -c:a aac -b:a 192k -movflags +faststart "${clipFile}"`;
      } else if (musicFile) {
        const af = buildAudioFilter(true);
        cmd = `ffmpeg -y ${inputs} -filter_complex "${af}" -map 0:v -map "[outa]" -c:v libx264 -preset fast -crf 20 -c:a aac -b:a 192k -movflags +faststart "${clipFile}"`;
      } else {
        const af = buildAudioFilter(false);
        cmd = `ffmpeg -y ${inputs} -filter_complex "${af}" -map 0:v -map "[outa]" -c:v copy -c:a aac -b:a 192k -movflags +faststart "${clipFile}"`;
      }

      await execAsync(cmd, { timeout: 10 * 60 * 1000 });
      if (!fs.existsSync(clipFile)) throw new Error(`Clip ${i + 1} তৈরি হয়নি`);

      // Upload clip to Drive
      updateJob('uploading', { step: `Clip ${i + 1}/${finalCount} Drive এ upload হচ্ছে...`, progress: Math.round(70 + (i / finalCount) * 30) });
      const d = await driveUpload(clipFile, `clip_${i + 1}_${Date.now()}.mp4`, outputFolderId, driveToken);
      uploadedClips.push({ id: d.id, name: d.name, clip: i + 1 });
      log(`[MOVIE] Clip ${i + 1} uploaded: ${d.id}`);
    }

    updateJob('done', { step: 'সম্পন্ন!', progress: 100, clips: uploadedClips, totalClips: finalCount });
  } catch (e) {
    updateJob('error', { error: e.message });
    log(`[MOVIE] ❌ ${e.message}`);
  } finally {
    cleanFiles(srcFile, musicFile, ...clipFiles);
  }
}

// ============================================================
// SECTION 7: TROLL EDIT
// ============================================================
function loadPhonks() { return readJSON(FILE.phonks, []); }
function savePhonks(p) { writeJSON(FILE.phonks, p); }

const phonkJobs = {};

async function downloadPhonk(ytUrl, name, dropTime) {
  const driveToken = await getDriveToken();
  if (!driveToken) throw new Error('Drive connect করো');
  const fetch = await gFetch();

  const id     = Date.now();
  const tmpTpl = path.join(DIR.temp, `phonk_${id}.%(ext)s`);
  const tmpMp3 = path.join(DIR.temp, `phonk_${id}.mp3`);

  log(`[PHONK] Downloading: ${ytUrl}`);
  await execAsync(`yt-dlp -f "bestaudio/best" --extract-audio --audio-format mp3 --audio-quality 192K --no-playlist -o "${tmpTpl}" "${ytUrl}"`, { timeout: 120000 });

  // Find output file (yt-dlp may name it differently)
  const found = fs.readdirSync(DIR.temp).filter(f => f.startsWith(`phonk_${id}`) && /\.(mp3|m4a|ogg|opus|webm)$/.test(f));
  if (!found.length) throw new Error('Phonk download হয়নি');
  const actualFile = path.join(DIR.temp, found[0]);
  if (actualFile !== tmpMp3) fs.renameSync(actualFile, tmpMp3);

  const duration = await getduration(tmpMp3);
  log(`[PHONK] ${name}: ${duration.toFixed(1)}s`);

  // Upload to Drive
  const d = await driveUpload(tmpMp3, `${name}.mp3`, null, driveToken, 'audio/mpeg');
  cleanFiles(tmpMp3);

  const phonkData = { name, driveId: d.id, dropTime: parseFloat(dropTime) || 0, duration, ytUrl };
  const phonks    = loadPhonks();
  const idx       = phonks.findIndex(p => p.name === name);
  if (idx >= 0) phonks[idx] = phonkData; else phonks.push(phonkData);
  savePhonks(phonks);
  log(`[PHONK] ✅ Saved: ${name}`);
  return phonkData;
}

async function processTrollEdit(videoPath, phonkInfo, opts = {}) {
  const { colorFilter = 'none', loopAudio = false, textOverlay = 'Wait for it... 💀', freezeSec = 3, skullPath = null } = opts;

  const phonkFile = tmpPath('phonk_tmp', 'mp3');
  const outFile   = tmpPath('troll_out', 'mp4');

  try {
    // Download phonk from Drive
    await driveDownloadToFile(phonkInfo.driveId, phonkFile);

    const videoDur = await getduration(videoPath);
    const phonkDur = phonkInfo.duration || await getduration(phonkFile);
    const dropTime = phonkInfo.dropTime || 0;

    // Calculate durations
    let videoTrimStart = 0, finalDur = videoDur;
    if (!loopAudio && videoDur > phonkDur) {
      videoTrimStart = videoDur - phonkDur;
      finalDur       = phonkDur;
    }
    const freezeAt    = Math.max(0.5, finalDur - freezeSec);
    const actualFreeze = finalDur - freezeAt;

    log(`[TROLL] Video:${videoDur.toFixed(1)}s Phonk:${phonkDur.toFixed(1)}s Freeze@${freezeAt.toFixed(1)}s`);

    // Color filter
    const cf     = GRADES[colorFilter] || '';
    const cfPart = cf ? `${cf},` : '';

    // Safe text
    const safeText = (textOverlay || '').replace(/[':\\]/g, '').trim();

    // Build video filter_complex
    const trimPart  = videoTrimStart > 0
      ? `[0:v]trim=start=${videoTrimStart},setpts=PTS-STARTPTS,${cfPart}split[vbef][vaft];`
      : `[0:v]${cfPart}split[vbef][vaft];`;
    const befPart   = `[vbef]trim=end=${freezeAt},setpts=PTS-STARTPTS[before];`;
    const aftPart   = `[vaft]trim=start=${freezeAt},setpts=PTS-STARTPTS,select='eq(n\\,0)',loop=loop=-1:size=1,trim=duration=${actualFreeze}[frozen];`;
    const darkPart  = `[frozen]eq=brightness=-0.32:saturation=0.15:contrast=1.5[dark];`;

    // Skull overlay
    const hasSkull = !!(skullPath && fs.existsSync(skullPath));
    const audioIdx = hasSkull ? 2 : 1;   // phonk is input 0 (video) + 1 (skull if any) + N (phonk)

    let midLabel = 'dark';
    let extraParts = '';

    if (hasSkull) {
      extraParts += `;[1:v]scale=270:270[skull];[dark][skull]overlay=(W-w)/2:(H-h)/2[withskull]`;
      midLabel = 'withskull';
    }
    if (safeText) {
      extraParts += `;[${midLabel}]drawtext=text='${safeText}':fontcolor=white:fontsize=44:x=(w-text_w)/2:y=h*0.1:box=1:boxcolor=black@0.55:boxborderw=12[titled]`;
      midLabel = 'titled';
    }

    const concatPart = `;[before][${midLabel}]concat=n=2:v=1:a=0[outv]`;

    const audioFilter = loopAudio
      ? `;[${audioIdx}:a]aloop=loop=-1:size=2000000000,atrim=end=${finalDur},asetpts=PTS-STARTPTS[outa]`
      : `;[${audioIdx}:a]atrim=start=${Math.max(0, dropTime)},asetpts=PTS-STARTPTS,atrim=end=${finalDur},asetpts=PTS-STARTPTS[outa]`;

    // FIX: mute original video audio before phonk
    // Build complete filter_complex (first parts end with ';', extras start with ';')
    const fc = trimPart + befPart + aftPart + darkPart + extraParts + concatPart + audioFilter;

    // Input order: [0]=video [1]=skull(optional) [last]=phonk
    const inputSkull = hasSkull ? `-i "${skullPath}"` : '';
    const cmd = `ffmpeg -y -i "${videoPath}" ${inputSkull} -i "${phonkFile}" -filter_complex "${fc}" -map "[outv]" -map "[outa]" -c:v libx264 -preset fast -crf 22 -c:a aac -b:a 192k -shortest "${outFile}"`;

    log('[TROLL] FFmpeg running...');
    await execAsync(cmd, { timeout: 8 * 60 * 1000 });

    if (!fs.existsSync(outFile)) throw new Error('Output file created হয়নি');
    log(`[TROLL] ✅ Done (${(fs.statSync(outFile).size / 1024 / 1024).toFixed(1)}MB)`);
    return outFile;
  } catch (e) {
    cleanFiles(phonkFile, outFile);
    throw e;
  } finally {
    cleanFiles(phonkFile);
  }
}

// ============================================================
// SECTION 8: DRIVE UTILITIES
// ============================================================
async function driveListVideos(folderId, driveToken) {
  const fetch = await gFetch();
  let all = [], pageToken = null;
  do {
    let url = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+mimeType+contains+'video/'+and+trashed=false&fields=files(id,name,size),nextPageToken&pageSize=1000`;
    if (pageToken) url += '&pageToken=' + pageToken;
    const r = await fetch(url, { headers: { 'Authorization': `Bearer ${driveToken}` } });
    if (!r.ok) throw new Error(`Drive list ${r.status}: ` + (await r.text()).slice(0, 200));
    const data = await r.json();
    if (data.error) throw new Error('Drive: ' + data.error.message);
    all = all.concat(data.files || []);
    pageToken = data.nextPageToken || null;
  } while (pageToken);
  return all;
}

async function driveDownloadToFile(fileId, destPath) {
  const fetch = await gFetch(), driveToken = await getDriveToken();
  const r = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`, { headers: { 'Authorization': `Bearer ${driveToken}` } });
  if (!r.ok) throw new Error(`Drive download ${r.status}`);
  await new Promise((res, rej) => {
    const dest = fs.createWriteStream(destPath);
    r.body.pipe(dest); dest.on('finish', res); dest.on('error', rej); r.body.on('error', rej);
  });
}

async function driveUpload(localPath, name, folderId, driveToken, mimeType = 'video/mp4') {
  const fetch    = await gFetch();
  const stat     = fs.statSync(localPath);
  const meta     = { name, parents: folderId ? [folderId] : [] };
  const initRes  = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${driveToken}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': mimeType, 'X-Upload-Content-Length': String(stat.size) },
    body: JSON.stringify(meta)
  });
  if (!initRes.ok) throw new Error('Drive init: ' + (await initRes.text()).slice(0, 200));
  const uploadUrl = initRes.headers.get('location');
  const upRes = await fetch(uploadUrl, {
    method: 'PUT', headers: { 'Content-Type': mimeType, 'Content-Length': String(stat.size) }, body: fs.createReadStream(localPath)
  });
  if (!upRes.ok) throw new Error('Drive upload: ' + (await upRes.text()).slice(0, 200));
  return upRes.json();
}

// ============================================================
// SECTION 9: URL/KS DOWNLOADER
// ============================================================
const dlJobs = {};

async function runDownload(jobId, url, driveFolderId, driveToken) {
  const update = (status, extra = {}) => { Object.assign(dlJobs[jobId], { status, ...extra }); };
  let tmpFile = null;
  try {
    const fetch = await gFetch();
    update('downloading', { step: 'Download হচ্ছে...' });
    tmpFile = tmpPath('dl', 'mp4');

    // Try direct download first
    let downloaded = false;
    try {
      const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, redirect: 'follow' });
      if (r.ok && (r.headers.get('content-type') || '').includes('video')) {
        await new Promise((res, rej) => { const d = fs.createWriteStream(tmpFile); r.body.pipe(d); d.on('finish', res); d.on('error', rej); r.body.on('error', rej); });
        downloaded = true;
      }
    } catch {}

    if (!downloaded) {
      const tpl = tmpFile.replace('.mp4', '.%(ext)s');
      await execAsync(`yt-dlp -f "best[ext=mp4]/best" --merge-output-format mp4 --no-playlist -o "${tpl}" "${url}"`, { timeout: 180000 });
      const files = fs.readdirSync(DIR.temp).filter(f => f.startsWith(path.basename(tmpFile, '.mp4')) && f.endsWith('.mp4'));
      if (files.length) tmpFile = path.join(DIR.temp, files[0]);
      if (!fs.existsSync(tmpFile)) throw new Error('Download failed');
    }

    update('uploading', { step: 'Drive এ upload হচ্ছে...' });
    const d = await driveUpload(tmpFile, `video_${Date.now()}.mp4`, driveFolderId, driveToken);
    update('done', { driveId: d.id, step: '✅ সম্পন্ন' });
    log(`[DL] ✅ ${url.slice(0, 50)}`);
  } catch (e) {
    update('error', { error: e.message });
    log(`[DL] ❌ ${e.message}`);
  } finally {
    cleanFiles(tmpFile);
  }
}

async function runBulkDownload(jobId, links, folderId, driveToken) {
  const job = dlJobs[jobId];
  for (let i = 0; i < links.length; i++) {
    const url = links[i];
    if (!url) continue;
    log(`[BULK] ${i + 1}/${links.length}: ${url.slice(0, 50)}`);
    Object.assign(dlJobs[jobId], { step: `${i + 1}/${links.length}`, current: i + 1 });
    let tmp = null;
    try {
      tmp = tmpPath('bulk', 'mp4');
      await execAsync(`yt-dlp -f "best[ext=mp4]/best" --merge-output-format mp4 --no-playlist -o "${tmp.replace('.mp4', '.%(ext)s')}" "${url}"`, { timeout: 180000 });
      const files = fs.readdirSync(DIR.temp).filter(f => f.startsWith(path.basename(tmp, '.mp4')) && f.endsWith('.mp4'));
      if (files.length) tmp = path.join(DIR.temp, files[0]);
      if (!fs.existsSync(tmp)) throw new Error('Download failed');
      await driveUpload(tmp, `bulk_${Date.now()}.mp4`, folderId, driveToken);
      dlJobs[jobId].success = (dlJobs[jobId].success || 0) + 1;
    } catch (e) {
      dlJobs[jobId].failed = (dlJobs[jobId].failed || 0) + 1;
      log(`[BULK] ❌ ${e.message}`);
    } finally {
      cleanFiles(tmp);
    }
  }
  dlJobs[jobId].status = 'done';
  log(`[BULK] Done: ${dlJobs[jobId].success || 0} ok, ${dlJobs[jobId].failed || 0} fail`);
}

// ============================================================
// SECTION 10: UPLOAD TO YOUTUBE
// ============================================================
async function uploadToYT(filePath, meta, privacy, ytToken) {
  const fetch   = await gFetch();
  const stat    = fs.statSync(filePath);
  const body    = { snippet: { title: meta.title.slice(0, 100), description: meta.description || '', tags: (meta.tags || []).slice(0, 30), categoryId: '22' }, status: { privacyStatus: privacy || 'public', selfDeclaredMadeForKids: false } };
  const initRes = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
    method: 'POST', headers: { 'Authorization': `Bearer ${ytToken}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': 'video/mp4', 'X-Upload-Content-Length': String(stat.size) },
    body: JSON.stringify(body)
  });
  if (!initRes.ok) throw new Error('YT init: ' + (await initRes.text()).slice(0, 300));
  const uploadUrl = initRes.headers.get('location');
  if (!uploadUrl) throw new Error('YT upload URL missing');
  const upRes = await fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': 'video/mp4', 'Content-Length': String(stat.size) }, body: fs.createReadStream(filePath) });
  if (!upRes.ok) throw new Error('YT upload: ' + (await upRes.text()).slice(0, 300));
  const data = await upRes.json();
  if (!data.id) throw new Error('No video ID in response');
  return data.id;
}

// ============================================================
// SECTION 11: AUTO UPLOAD (Drive → process → YT)
// ============================================================
let uploadRunning = false, uploadStart = 0;
const UPLOAD_TIMEOUT = 20 * 60 * 1000;

async function runAutoUpload(folderId, template, privacy) {
  if (uploadRunning && (Date.now() - uploadStart) < UPLOAD_TIMEOUT) { log('[AUTO] Already running'); return; }
  uploadRunning = true; uploadStart = Date.now();
  let videoTmp = null, outFile = null;

  try {
    const cfg        = loadCfg();
    const driveToken = await getDriveToken();
    if (!driveToken) throw new Error('Drive connect করো');
    const ytToken = await getYTToken();
    if (!ytToken) throw new Error('YouTube connect করো');

    // Get next video from Drive
    const videos = await driveListVideos(folderId, driveToken);
    if (!videos.length) throw new Error('Folder empty: ' + folderId);
    const video = nextVideo(folderId, videos);
    log(`[AUTO] Selected: ${video.name}`);

    // Download video
    videoTmp = tmpPath('auto', 'mp4');
    await driveDownloadToFile(video.id, videoTmp);
    log(`[AUTO] Downloaded (${(fs.statSync(videoTmp).size / 1024 / 1024).toFixed(1)}MB)`);

    // Process if troll enabled
    const trollCfg = cfg.troll || {};
    if (trollCfg.enabled) {
      const phonks = loadPhonks();
      if (!phonks.length) throw new Error('Phonk নেই — Troll tab এ যোগ করো');
      const idx      = (trollCfg.lastPhonkIdx || 0) % phonks.length;
      const phonkInfo = phonks[idx];
      saveCfg({ ...cfg, troll: { ...trollCfg, lastPhonkIdx: (idx + 1) % phonks.length } });

      // Skull path
      const skullFile = path.join(DIR.assets, 'skull.png');
      const skullPath = fs.existsSync(skullFile) ? skullFile : null;

      outFile = await processTrollEdit(videoTmp, phonkInfo, {
        colorFilter:     trollCfg.colorFilter || 'none',
        loopAudio:       trollCfg.loopAudio   || false,
        textOverlay:     trollCfg.textOverlay  || 'Wait for it... 💀',
        freezeSec:       3,
        skullPath,
      });
    } else {
      outFile = videoTmp;
      videoTmp = null; // Don't double-clean
    }

    // Title from template or filename
    const cleanName = video.name.replace(/\.[^.]+$/, '').replace(/[_-]/g, ' ').trim();
    const title     = (template ? template.replace('{{name}}', cleanName) : cleanName).slice(0, 100);

    // Upload to YouTube
    const ytId = await uploadToYT(outFile, { title, tags: ['shorts', 'viral'] }, privacy, ytToken);
    log(`[AUTO] ✅ https://youtu.be/${ytId} | "${title}"`);
  } catch (e) {
    log(`[AUTO] ❌ ${e.message}`);
  } finally {
    cleanFiles(videoTmp, outFile);
    uploadRunning = false;
  }
}

// ============================================================
// SECTION 12: SCHEDULER
// ============================================================
const firedSlots = new Set();
let  lastDay     = '';

setInterval(async () => {
  try {
    const cfg = loadCfg();
    if (!cfg.enabled) return;
    const today = bdDay();
    const bdMin = bdMinutes();
    if (today !== lastDay) { firedSlots.clear(); lastDay = today; }
    const dayCfg = cfg.days?.[today];
    if (!dayCfg?.enabled || !dayCfg?.folderId?.trim()) return;
    for (const slot of (cfg.slots || [])) {
      if (!slot.enabled) continue;
      const [h, m] = slot.time.split(':').map(Number);
      const slotMin = h * 60 + m;
      const key     = `${today}_${slot.time}`;
      if (!firedSlots.has(key) && Math.abs(bdMin - slotMin) <= 1) {
        firedSlots.add(key);
        log(`[SCHED] ⏰ ${slot.time} | ${today}`);
        runAutoUpload(dayCfg.folderId.trim(), dayCfg.template, cfg.privacy).catch(e => log('[SCHED] ' + e.message));
        break;
      }
    }
  } catch (e) { console.error('[SCHED]', e.message); }
}, 30000);

// Token refresh every 45 min
setInterval(async () => {
  const t = loadTokens();
  if (t.refresh_token)       await refreshYT().catch(() => {});
  if (t.drive_refresh_token) await refreshDrive().catch(() => {});
}, 45 * 60 * 1000);

// ============================================================
// SECTION 13: API ROUTES
// ============================================================
app.get('/health', (req, res) => res.json({ ok: true, time: bdTime(), uptime: Math.floor(process.uptime()) + 's', uploadRunning }));

// Status
app.get('/api/status', (req, res) => {
  const t = loadTokens();
  res.json({ youtube: !!t.access_token, drive: !!t.drive_access_token, channelName: t.channel_name || null, uploadRunning });
});

// Config
app.get('/api/config',       (req, res) => res.json(loadCfg()));
app.post('/api/config', (req, res) => {
  try {
    const cur = loadCfg(), inc = req.body;
    const days = { ...cur.days };
    if (inc.days) DAYS.forEach(d => { if (inc.days[d] !== undefined) days[d] = { ...defaultDayCfg(), ...(cur.days[d] || {}), ...inc.days[d] }; });
    saveCfg({ ...cur, ...inc, days, troll: { ...cur.troll, ...(inc.troll || {}) }, movie: { ...cur.movie, ...(inc.movie || {}) } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Logs
app.get('/api/logs', (req, res) => res.json({ logs: LOGS.slice(0, 100) }));

// Queue
app.get('/api/queue', (req, res) => {
  const q = loadQueue(), s = {};
  Object.keys(q).forEach(k => { s[k] = { remaining: (q[k].remaining || []).length, used: (q[k].used || []).length }; });
  res.json(s);
});
app.post('/api/queue/reset', (req, res) => {
  const { folderId } = req.body, q = loadQueue();
  if (folderId) delete q[folderId]; else Object.keys(q).forEach(k => delete q[k]);
  saveQueue(q); res.json({ success: true });
});

// Run Now (manual)
app.post('/api/run-now', async (req, res) => {
  const { folderId, template, privacy } = req.body;
  if (!folderId?.trim()) return res.status(400).json({ error: 'folderId দাও' });
  if (uploadRunning && (Date.now() - uploadStart) < UPLOAD_TIMEOUT) return res.status(409).json({ error: 'Upload চলছে' });
  res.json({ success: true, message: 'শুরু হচ্ছে...' });
  runAutoUpload(folderId.trim(), template, privacy).catch(() => {});
});

// Test folder
app.post('/api/test-folder', async (req, res) => {
  try {
    const { folderId } = req.body;
    if (!folderId) return res.status(400).json({ error: 'folderId দাও' });
    const dt     = await getDriveToken();
    if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
    const videos = await driveListVideos(folderId.trim(), dt);
    res.json({ success: true, count: videos.length, sample: videos.slice(0, 3).map(v => v.name) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Drive token for client-side upload
app.get('/api/drive-token', async (req, res) => {
  try {
    const dt = await getDriveToken();
    if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
    res.json({ token: dt });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Skull upload
app.post('/api/skull/upload', (req, res) => {
  try {
    const { base64 } = req.body;
    if (!base64) return res.status(400).json({ error: 'base64 দাও' });
    const buf = Buffer.from(base64.replace(/^data:image\/\w+;base64,/, ''), 'base64');
    fs.writeFileSync(path.join(DIR.assets, 'skull.png'), buf);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/skull/exists', (req, res) => res.json({ exists: fs.existsSync(path.join(DIR.assets, 'skull.png')) }));

// Phonk routes
app.get('/api/phonk/list', (req, res) => res.json(loadPhonks()));
app.post('/api/phonk/add', async (req, res) => {
  const { ytUrl, name, dropTime } = req.body;
  if (!ytUrl || !name) return res.status(400).json({ error: 'ytUrl, name দাও' });
  const jobId = Date.now().toString();
  phonkJobs[jobId] = { status: 'downloading', name };
  res.json({ success: true, jobId });
  downloadPhonk(ytUrl, name, dropTime || 0)
    .then(p  => { phonkJobs[jobId] = { status: 'done',  phonk: p }; })
    .catch(e => { phonkJobs[jobId] = { status: 'error', error: e.message }; });
});
app.get('/api/phonk/status/:id', (req, res) => {
  const j = phonkJobs[req.params.id];
  if (!j) return res.status(404).json({ error: 'Job not found' });
  res.json(j);
});
app.delete('/api/phonk/:name', (req, res) => {
  savePhonks(loadPhonks().filter(p => p.name !== decodeURIComponent(req.params.name)));
  res.json({ success: true });
});

// Movie Slicer routes
app.post('/api/movie/start', async (req, res) => {
  const { url, driveFileId, clipDuration = 60, clipCount = 5, colorGrade = 'teal_orange',
          headBob = true, headBobIntensity = 2, dialogVol = 1.0, musicFileId = null,
          musicVol = 0.5, outputFolderId } = req.body;
  if (!url && !driveFileId) return res.status(400).json({ error: 'url বা driveFileId দাও' });
  const driveToken = await getDriveToken();
  if (!driveToken) return res.status(401).json({ error: 'Drive connect করো' });
  const jobId = Date.now().toString();
  movieJobs[jobId] = { status: 'starting', progress: 0 };
  res.json({ success: true, jobId });
  runMovieSlicer(jobId, { url, driveFileId, clipDuration, clipCount, colorGrade, useHeadBob: headBob, headBobIntensity, dialogVol, musicFileId, musicVol, outputFolderId, driveToken }).catch(() => {});
});
app.get('/api/movie/status/:id', (req, res) => {
  const j = movieJobs[req.params.id];
  if (!j) return res.status(404).json({ error: 'Job not found' });
  res.json(j);
});

// Download routes
app.post('/api/download/single', async (req, res) => {
  const { url, folderId } = req.body;
  if (!url) return res.status(400).json({ error: 'url দাও' });
  const dt = await getDriveToken();
  if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
  const jobId = Date.now().toString();
  dlJobs[jobId] = { status: 'starting', total: 1 };
  res.json({ success: true, jobId });
  runDownload(jobId, url, folderId, dt).catch(() => {});
});
app.post('/api/download/bulk', async (req, res) => {
  const { links, folderId } = req.body;
  if (!links?.length) return res.status(400).json({ error: 'links দাও' });
  const dt = await getDriveToken();
  if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
  const jobId = Date.now().toString();
  dlJobs[jobId] = { status: 'running', total: links.length, success: 0, failed: 0 };
  res.json({ success: true, jobId, total: links.length });
  runBulkDownload(jobId, links.filter(Boolean), folderId, dt).catch(() => {});
});
app.get('/api/download/status/:id', (req, res) => {
  const j = dlJobs[req.params.id];
  if (!j) return res.status(404).json({ error: 'Job not found' });
  res.json(j);
});

app.get('/api/grades', (req, res) => res.json(Object.keys(GRADES)));

// ============================================================
// SECTION 14: OAUTH
// ============================================================
app.get('/auth/youtube', (req, res) => {
  const scope = encodeURIComponent('https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL + '/auth/youtube/callback')}&response_type=code&scope=${scope}&access_type=offline&prompt=consent`);
});
app.get('/auth/youtube/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.send(htmlPage('❌', 'Error: ' + error));
  if (!code)  return res.send(htmlPage('❌', 'No code'));
  try {
    const fetch = await gFetch();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, redirect_uri: process.env.BASE_URL + '/auth/youtube/callback', grant_type: 'authorization_code' })
    });
    const tokens = await r.json();
    if (!tokens.access_token) throw new Error(JSON.stringify(tokens));
    tokenExpiry.yt = Date.now() + (tokens.expires_in || 3600) * 1000 - 300000;
    let ch = '';
    try { ch = (await (await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers: { 'Authorization': `Bearer ${tokens.access_token}` } })).json()).items?.[0]?.snippet?.title || ''; } catch {}
    saveTokens({ ...loadTokens(), access_token: tokens.access_token, refresh_token: tokens.refresh_token, channel_name: ch });
    log('[AUTH] YouTube: ' + ch);
    res.send(htmlPage('✅', 'YouTube সংযুক্ত!', ch));
  } catch (e) { res.send(htmlPage('❌', e.message)); }
});

app.get('/auth/drive', (req, res) => {
  const scope = encodeURIComponent('https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/drive.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL + '/auth/drive/callback')}&response_type=code&scope=${scope}&access_type=offline&prompt=consent`);
});
app.get('/auth/drive/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.send(htmlPage('❌', 'Error: ' + error));
  if (!code)  return res.send(htmlPage('❌', 'No code'));
  try {
    const fetch = await gFetch();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, redirect_uri: process.env.BASE_URL + '/auth/drive/callback', grant_type: 'authorization_code' })
    });
    const tokens = await r.json();
    if (!tokens.access_token) throw new Error(JSON.stringify(tokens));
    tokenExpiry.drive = Date.now() + (tokens.expires_in || 3600) * 1000 - 300000;
    saveTokens({ ...loadTokens(), drive_access_token: tokens.access_token, drive_refresh_token: tokens.refresh_token });
    log('[AUTH] Drive ✅');
    res.send(htmlPage('✅', 'Google Drive সংযুক্ত!'));
  } catch (e) { res.send(htmlPage('❌', e.message)); }
});

// ============================================================
// SECTION 15: STARTUP
// ============================================================
async function startup() {
  log('🚀 Auto Waz 2.0 starting...');
  try {
    await restoreTokens();
    const t = loadTokens();
    if (t.refresh_token)       await refreshYT().catch(() => {});
    if (t.drive_refresh_token) await refreshDrive().catch(() => {});
    await restoreCfg();
    await restoreQueue();
    log('✅ Ready!');
  } catch (e) { log('⚠️ Startup: ' + e.message); }
}

startup();
app.listen(PORT, () => console.log(`Auto Waz 2.0 on port ${PORT}`));
