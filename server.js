'use strict';
// ================================================================
//  AUTO WAZ 2.0  ·  Video Automation Tool
//  Section 1  : Setup & Helpers
//  Section 2  : Token System
//  Section 3  : Config & Queue (Drive-backed)
//  Section 4  : FFmpeg Utilities
//  Section 5  : Phonk Manager
//  Section 6  : Troll Edit
//  Section 7  : Movie Slicer (Shorts)
//  Section 8  : Drive Utilities
//  Section 9  : Downloader
//  Section 10 : Auto Upload & Scheduler
//  Section 11 : Routes
//  Section 12 : OAuth
//  Section 13 : Startup
// ================================================================

const express   = require('express');
const fs        = require('fs');
const path      = require('path');
const { exec }  = require('child_process');
const { promisify } = require('util');
const multer    = require('multer');
const execAsync = promisify(exec);

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Directories ──────────────────────────────────────────────────
const TEMP    = path.join(__dirname, 'temp');
const ASSETS  = path.join(__dirname, 'assets');
const PUBLIC  = path.join(__dirname, 'public');
[TEMP, ASSETS].forEach(d => fs.existsSync(d) || fs.mkdirSync(d, { recursive: true }));

// ── Middleware ───────────────────────────────────────────────────
app.use(express.json({ limit: '30mb' }));   // skull image only
app.use(express.static(PUBLIC));
app.use('/assets', express.static(ASSETS));

// ── Multer (streaming file upload, no size issue) ────────────────
const storage = multer.diskStorage({
  destination: (_r, _f, cb) => cb(null, TEMP),
  filename:    (_r,  f, cb) => cb(null, `up_${Date.now()}_${Math.random().toString(36).slice(2,6)}${path.extname(f.originalname) || '.mp4'}`)
});
const uploader = multer({ storage, limits: { fileSize: 4 * 1024 * 1024 * 1024 } });

// ── File paths ───────────────────────────────────────────────────
const F = {
  tokens : path.join(__dirname, 'tokens.json'),
  config : path.join(__dirname, 'config.json'),
  queue  : path.join(__dirname, 'queue.json'),
  phonks : path.join(__dirname, 'phonks.json'),
  skull  : path.join(ASSETS, 'skull.png'),
};

// ================================================================
//  SECTION 1 : HELPERS
// ================================================================
const LOGS = [];
const log  = msg => {
  const line = `[${bdTime()}] ${msg}`;
  console.log(line);
  LOGS.unshift(line);
  if (LOGS.length > 500) LOGS.length = 500;
};

function bdTime() {
  return new Date(Date.now() + 6 * 3600000).toISOString().replace('T', ' ').slice(0, 19);
}
function bdMinutes() {
  return Math.floor((Date.now() + 6 * 3600000) / 60000) % 1440;
}
function bdDay() {
  return new Date().toLocaleDateString('en-US', { weekday: 'long', timeZone: 'Asia/Dhaka' });
}
function rj(file, fb = {}) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fb; }
}
function wj(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function tmp(prefix, ext = 'mp4') {
  return path.join(TEMP, `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}.${ext}`);
}
function clean(...files) {
  files.flat().forEach(f => { try { if (f && fs.existsSync(f)) fs.unlinkSync(f); } catch {} });
}
function htmlOK(title, sub = '') {
  return `<html><body style="background:#07080f;color:#00d4aa;font-family:sans-serif;text-align:center;padding:60px">
<div style="font-size:56px">✅</div><h2>${title}</h2>${sub ? `<p style="color:#aaa">${sub}</p>` : ''}
<script>setTimeout(()=>window.close(),2500)</script></body></html>`;
}
function htmlERR(msg) {
  return `<html><body style="background:#07080f;color:#ff4785;font-family:sans-serif;text-align:center;padding:60px">
<div style="font-size:56px">❌</div><h2>${msg}</h2>
<script>setTimeout(()=>window.close(),3000)</script></body></html>`;
}

let _fetch = null;
const gf = async () => { if (!_fetch) _fetch = (await import('node-fetch')).default; return _fetch; };

// ================================================================
//  SECTION 2 : TOKEN SYSTEM  (no circular dependency)
// ================================================================
const tokExp = { yt: 0, drive: 0 };
const TB_NAME = 'autowaz2_tokens.json';
let   tbFileId = null;

function loadTok()   { return rj(F.tokens, {}); }
function saveTok(t)  {
  wj(F.tokens, t);
  if (t.drive_access_token) _tbBackup(t, t.drive_access_token).catch(() => {});
}

async function _tbBackup(tok, dt) {
  try {
    const fetch = await gf();
    const body  = JSON.stringify(tok, null, 2);
    if (tbFileId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${tbFileId}?uploadType=media`, {
        method: 'PATCH', headers: { Authorization: `Bearer ${dt}`, 'Content-Type': 'application/json' }, body
      });
      if (!r.ok) tbFileId = null;
    } else {
      const bnd = 'tkb', meta = JSON.stringify({ name: TB_NAME, mimeType: 'application/json' });
      const req = `--${bnd}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${bnd}\r\nContent-Type: application/json\r\n\r\n${body}\r\n--${bnd}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method: 'POST', headers: { Authorization: `Bearer ${dt}`, 'Content-Type': `multipart/related; boundary=${bnd}` }, body: req
      });
      if (r.ok) { const d = await r.json(); if (d.id) { tbFileId = d.id; log('[TOKEN] Backup ✅'); } }
    }
  } catch (e) { console.warn('[TOKEN] Backup:', e.message); }
}

async function refreshYT() {
  try {
    const fetch = await gf(), t = loadTok();
    if (!t.refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ refresh_token: t.refresh_token, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, grant_type: 'refresh_token' })
    });
    const d = await r.json();
    if (d.access_token) {
      tokExp.yt = Date.now() + (d.expires_in || 3600) * 1000 - 300000;
      const u = { ...t, access_token: d.access_token };
      wj(F.tokens, u);                                           // direct write — no saveTok loop
      if (u.drive_access_token) _tbBackup(u, u.drive_access_token).catch(() => {});
      log('[TOKEN] YT ✅');
      return d.access_token;
    }
    log('[TOKEN] YT fail: ' + JSON.stringify(d));
  } catch (e) { log('[TOKEN] YT err: ' + e.message); }
  return null;
}

async function refreshDrive() {
  try {
    const fetch = await gf(), t = loadTok();
    if (!t.drive_refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ refresh_token: t.drive_refresh_token, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, grant_type: 'refresh_token' })
    });
    const d = await r.json();
    if (d.access_token) {
      tokExp.drive = Date.now() + (d.expires_in || 3600) * 1000 - 300000;
      const u = { ...t, drive_access_token: d.access_token };
      wj(F.tokens, u);
      _tbBackup(u, d.access_token).catch(() => {});
      log('[TOKEN] Drive ✅');
      return d.access_token;
    }
    log('[TOKEN] Drive fail: ' + JSON.stringify(d));
  } catch (e) { log('[TOKEN] Drive err: ' + e.message); }
  return null;
}

async function getYT() {
  if (Date.now() < tokExp.yt) { const t = loadTok(); if (t.access_token) return t.access_token; }
  return (await refreshYT()) || loadTok().access_token || null;
}
async function getDrive() {
  if (Date.now() < tokExp.drive) { const t = loadTok(); if (t.drive_access_token) return t.drive_access_token; }
  return (await refreshDrive()) || loadTok().drive_access_token || null;
}

async function restoreTokens() {
  try {
    const fetch = await gf(), t = loadTok();
    const bt = t.drive_access_token || process.env.DRIVE_ACCESS_TOKEN;
    if (!bt) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${TB_NAME}' and trashed=false&fields=files(id)&pageSize=1`, { headers: { Authorization: `Bearer ${bt}` } });
    if (!r.ok) return;
    const d = await r.json();
    if (!d.files?.length) return;
    tbFileId = d.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${tbFileId}?alt=media`, { headers: { Authorization: `Bearer ${bt}` } });
    if (!fr.ok) return;
    const saved = await fr.json();
    if (saved?.refresh_token || saved?.drive_refresh_token) { wj(F.tokens, saved); log('[TOKEN] Restored ✅'); }
  } catch (e) { console.warn('[TOKEN] Restore:', e.message); }
}

// ================================================================
//  SECTION 3 : CONFIG & QUEUE
// ================================================================
const DAYS   = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
const CB_NAME = 'autowaz2_config.json';
const QB_NAME = 'autowaz2_queue.json';
let cfgDriveId = null, qDriveId = null;

const defaultDay = () => ({ folderId: '', template: '', enabled: true });
const defaultCfg = () => ({
  enabled: false,
  slots:   [],
  privacy: 'public',
  days:    Object.fromEntries(DAYS.map(d => [d, defaultDay()])),
  troll:   { enabled: false, colorFilter: 'none', loopAudio: false, textOverlay: 'Wait for it... 💀' },
});

function loadCfg() {
  const s = rj(F.config, null);
  if (!s) return defaultCfg();
  const def = defaultCfg();
  const days = Object.fromEntries(DAYS.map(d => [d, { ...defaultDay(), ...(s.days?.[d] || {}) }]));
  return { ...def, ...s, days, troll: { ...def.troll, ...(s.troll || {}) } };
}
function saveCfg(cfg) {
  wj(F.config, cfg);
  _driveBackup(cfg, CB_NAME, () => cfgDriveId, id => { cfgDriveId = id; }).catch(() => {});
}

function loadQueue() { return rj(F.queue, {}); }
function saveQueue(q) {
  wj(F.queue, q);
  _driveBackup(q, QB_NAME, () => qDriveId, id => { qDriveId = id; }).catch(() => {});
}

async function _driveBackup(data, name, getId, setId) {
  try {
    const fetch = await gf(), t = loadTok(), dt = t.drive_access_token;
    if (!dt) return;
    const body = JSON.stringify(data, null, 2);
    const id   = getId();
    if (id) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${id}?uploadType=media`, {
        method: 'PATCH', headers: { Authorization: `Bearer ${dt}`, 'Content-Type': 'application/json' }, body
      });
      if (!r.ok) setId(null);
    } else {
      const bnd = 'bk', meta = JSON.stringify({ name });
      const req  = `--${bnd}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${bnd}\r\nContent-Type: application/json\r\n\r\n${body}\r\n--${bnd}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method: 'POST', headers: { Authorization: `Bearer ${dt}`, 'Content-Type': `multipart/related; boundary=${bnd}` }, body: req
      });
      if (r.ok) { const d = await r.json(); if (d.id) setId(d.id); }
    }
  } catch {}
}

async function restoreCfg() {
  const data = await _driveRestore(CB_NAME, () => cfgDriveId, id => { cfgDriveId = id; });
  if (data) { wj(F.config, data); log('[CFG] Restored ✅'); }
}
async function restoreQueue() {
  const data = await _driveRestore(QB_NAME, () => qDriveId, id => { qDriveId = id; });
  if (data) { wj(F.queue, data); log('[QUEUE] Restored ✅'); }
}
async function _driveRestore(name, getId, setId) {
  try {
    const fetch = await gf(), dt = await getDrive();
    if (!dt) return null;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${name}' and trashed=false&fields=files(id)&pageSize=1`, { headers: { Authorization: `Bearer ${dt}` } });
    if (!r.ok) return null;
    const d = await r.json();
    if (!d.files?.length) return null;
    setId(d.files[0].id);
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${getId()}?alt=media`, { headers: { Authorization: `Bearer ${dt}` } });
    if (!fr.ok) return null;
    return await fr.json();
  } catch { return null; }
}

// Video queue — random FIFO, Drive-backed
function nextVideo(folderId, videos) {
  const q    = loadQueue();
  if (!q[folderId]) q[folderId] = { remaining: [], used: [] };
  const all  = videos.map(v => v.id);
  let rem    = (q[folderId].remaining || []).filter(id => all.includes(id));
  if (!rem.length) {
    rem = [...all].sort(() => Math.random() - 0.5);
    q[folderId].used = [];
    log(`[QUEUE] New cycle: ${rem.length} videos`);
  }
  const next = rem.shift();
  q[folderId].remaining = rem;
  q[folderId].used = [...(q[folderId].used || []).slice(-1000), next];
  saveQueue(q);
  return videos.find(v => v.id === next) || videos[0];
}

// ================================================================
//  SECTION 4 : FFMPEG UTILITIES
// ================================================================
async function duration(filePath) {
  try {
    const { stdout } = await execAsync(`ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${filePath}"`);
    return parseFloat(stdout.trim()) || 0;
  } catch { return 0; }
}

// Cinematic color grades — high quality curves-based
const GRADES = {
  none:        null,
  teal_orange: `curves=r='0/0 0.25/0.28 0.5/0.55 0.75/0.8 1/1':g='0/0 0.5/0.5 1/0.97':b='0/0 0.25/0.22 0.5/0.44 0.75/0.68 1/0.85',colorbalance=ss=-0.06:ms=0.04:hs=0.08:bs=0.1,eq=contrast=1.06:saturation=1.15`,
  moody:       `curves=all='0/0.02 0.3/0.23 0.6/0.5 0.8/0.72 1/0.9',colorbalance=bs=0.06:bh=-0.04,eq=contrast=1.2:saturation=0.82:brightness=-0.03`,
  warm_film:   `curves=r='0/0.04 0.5/0.58 1/1':g='0/0 0.5/0.5 1/0.94':b='0/0 0.5/0.4 1/0.78',eq=saturation=1.12:contrast=1.04`,
  cold_blue:   `colorbalance=bs=0.14:bm=0.08:bh=0.05:rs=-0.06:rm=-0.04,eq=saturation=0.88:contrast=1.1`,
  vintage:     `curves=r='0/0.05 0.5/0.54 1/0.94':g='0/0 0.5/0.49 1/0.9':b='0/0.08 0.5/0.44 1/0.73',eq=saturation=0.72:contrast=1.1,vignette=angle=PI/4:mode=backward`,
  neon_noir:   `eq=saturation=1.9:contrast=1.3:brightness=-0.08,colorbalance=rs=-0.12:bs=0.12:rh=0.18:bh=-0.08`,
  golden_hour: `curves=r='0/0.08 0.5/0.64 1/1':g='0/0.02 0.5/0.51 1/0.92':b='0/0 0.5/0.34 1/0.68',eq=saturation=1.3:contrast=1.06`,
  dream:       `gblur=sigma=0.8,curves=all='0/0.05 0.5/0.52 1/0.92',eq=saturation=1.2:brightness=0.02`,
};

// Crop video to 9:16 (Shorts format) — smart center crop
// Output: 1080×1920
function shortsFilter() {
  // Scale to height 1920, then crop center to 1080 wide
  // If landscape: scale height to 1920, crop width
  // If portrait:  scale width to 1080, crop height (or pad)
  return `scale='if(gt(iw/ih,9/16),trunc(oh*9/16/2)*2,1080)':'if(gt(iw/ih,9/16),1920,trunc(ow*16/9/2)*2)',crop=1080:1920`;
}

// Subtle sinusoidal head bob — slight zoom (1.04x), oscillate vertically
// Creates the satisfying head-nod feel without distortion
function headBobFilter(intensity = 2) {
  const scale = 1 + (intensity * 0.02);          // intensity=2 → 1.04x
  const amp   = (intensity * 0.008).toFixed(4);   // amplitude of bob
  return `scale=trunc(iw*${scale.toFixed(3)}/2)*2:trunc(ih*${scale.toFixed(3)}/2)*2,` +
         `crop=iw/${scale.toFixed(3)}:ih/${scale.toFixed(3)}:` +
         `x='(iw-ow)/2':y='(ih-ow*16/9)/2+ow*16/9*${amp}*sin(2*PI*t*1.0)'`;
}

// Small zoom for Troll edits — cinematic, no aspect ratio change
function slightZoomFilter() {
  return `scale=trunc(iw*1.05/2)*2:trunc(ih*1.05/2)*2,crop=iw/1.05:ih/1.05`;
}

// ================================================================
//  SECTION 5 : PHONK MANAGER
// ================================================================
function loadPhonks() { return rj(F.phonks, []); }
function savePhonks(p) { wj(F.phonks, p); }

const pJobs = {};

// Upload mp3 file from multer → Drive
async function savePhonkFromFile(filePath, name, dropTime) {
  const dt = await getDrive();
  if (!dt) throw new Error('Drive connect করো');
  const dur = await duration(filePath);
  log(`[PHONK] File: ${name} (${dur.toFixed(1)}s)`);
  const d   = await driveUpload(filePath, `${name}.mp3`, null, dt, 'audio/mpeg');
  const rec = { name, driveId: d.id, dropTime: parseFloat(dropTime) || 0, duration: dur };
  const all = loadPhonks();
  const i   = all.findIndex(p => p.name === name);
  if (i >= 0) all[i] = rec; else all.push(rec);
  savePhonks(all);
  log(`[PHONK] ✅ Saved: ${name}`);
  return rec;
}

// Download phonk from YouTube URL
async function dlPhonkFromYT(ytUrl, name, dropTime) {
  const dt = await getDrive();
  if (!dt) throw new Error('Drive connect করো');
  const id   = Date.now();
  const tpl  = path.join(TEMP, `phonk_${id}.%(ext)s`);
  const mp3  = path.join(TEMP, `phonk_${id}.mp3`);
  log(`[PHONK] Downloading: ${ytUrl}`);
  await execAsync(
    `yt-dlp -f "bestaudio/best" --extract-audio --audio-format mp3 --audio-quality 192K --no-playlist `+
    `--extractor-args "youtube:player_client=web_embedded" --no-check-certificates `+
    `-o "${tpl}" "${ytUrl}"`,
    { timeout: 120000 }
  );
  const found = fs.readdirSync(TEMP).filter(f => f.startsWith(`phonk_${id}`) && /\.(mp3|m4a|ogg|opus)$/.test(f));
  if (!found.length) throw new Error('Phonk download হয়নি');
  const actual = path.join(TEMP, found[0]);
  if (actual !== mp3) fs.renameSync(actual, mp3);
  try {
    const rec = await savePhonkFromFile(mp3, name, dropTime);
    return rec;
  } finally { clean(mp3); }
}

// ================================================================
//  SECTION 6 : TROLL EDIT
// ================================================================
const trollJobs = {};

async function processTroll(videoPath, phonkInfo, opts = {}) {
  const {
    colorFilter = 'none',
    loopAudio   = false,
    textOverlay = 'Wait for it... 💀',
    freezeSec   = 3,
  } = opts;

  const phonkTmp = tmp('phonk', 'mp3');
  const outFile  = tmp('troll_out', 'mp4');

  try {
    // Download phonk from Drive
    await driveStreamToFile(phonkInfo.driveId, phonkTmp);

    const vDur     = await duration(videoPath);
    const pDur     = phonkInfo.duration || await duration(phonkTmp);
    const drop     = phonkInfo.dropTime || 0;

    // If loop=off and video > phonk: trim video start so it ends with phonk
    let trimStart = 0, finalDur = vDur;
    if (!loopAudio && vDur > pDur) {
      trimStart = vDur - pDur;
      finalDur  = pDur;
    }

    // Freeze = last `freezeSec` seconds
    const freezeAt     = Math.max(0.5, finalDur - freezeSec);
    const actualFreeze = finalDur - freezeAt;
    log(`[TROLL] v:${vDur.toFixed(1)}s p:${pDur.toFixed(1)}s freeze@${freezeAt.toFixed(1)}s`);

    // Build filters
    const grade    = GRADES[colorFilter] || '';
    const gradeStr = grade ? `${grade},` : '';
    const zoomStr  = `${slightZoomFilter()},`;
    const safeText = (textOverlay || '').replace(/[':\\]/g, '').trim();

    // Video filter_complex
    const trimPart  = trimStart > 0
      ? `[0:v]trim=start=${trimStart},setpts=PTS-STARTPTS,${zoomStr}${gradeStr}split[vbf][vaf];`
      : `[0:v]${zoomStr}${gradeStr}split[vbf][vaf];`;
    const bPart     = `[vbf]trim=end=${freezeAt},setpts=PTS-STARTPTS[before];`;
    const fPart     = `[vaf]trim=start=${freezeAt},setpts=PTS-STARTPTS,select='eq(n\\,0)',loop=loop=-1:size=1,trim=duration=${actualFreeze}[frozen];`;
    const darkPart  = `[frozen]eq=brightness=-0.32:saturation=0.15:contrast=1.5[dark];`;

    // Skull overlay
    const hasSkull = fs.existsSync(F.skull);
    const aIdx     = hasSkull ? 2 : 1;
    let   mid      = 'dark', extra = '';

    if (hasSkull) {
      extra += `;[1:v]scale=260:260[skull];[dark][skull]overlay=(W-w)/2:(H-h)/2[ws]`;
      mid    = 'ws';
    }
    if (safeText) {
      extra += `;[${mid}]drawtext=text='${safeText}':fontcolor=white:fontsize=46:x=(w-text_w)/2:y=h*0.1:box=1:boxcolor=black@0.55:boxborderw=14[wt]`;
      mid    = 'wt';
    }

    const concat   = `;[before][${mid}]concat=n=2:v=1:a=0[outv]`;
    const audio    = loopAudio
      ? `;[${aIdx}:a]aloop=loop=-1:size=2000000000,atrim=end=${finalDur},asetpts=PTS-STARTPTS[outa]`
      : `;[${aIdx}:a]atrim=start=${Math.max(0, drop)},asetpts=PTS-STARTPTS,atrim=end=${finalDur},asetpts=PTS-STARTPTS[outa]`;

    const fc       = trimPart + bPart + fPart + darkPart + extra + concat + audio;
    const skIn     = hasSkull ? `-i "${F.skull}"` : '';
    const cmd      = `ffmpeg -y -i "${videoPath}" ${skIn} -i "${phonkTmp}" -filter_complex "${fc}" -map "[outv]" -map "[outa]" -c:v libx264 -preset fast -crf 22 -c:a aac -b:a 192k -shortest "${outFile}"`;

    log('[TROLL] FFmpeg...');
    await execAsync(cmd, { timeout: 10 * 60 * 1000 });
    if (!fs.existsSync(outFile)) throw new Error('Output তৈরি হয়নি');
    log(`[TROLL] ✅ (${(fs.statSync(outFile).size/1024/1024).toFixed(1)}MB)`);
    return outFile;
  } finally {
    clean(phonkTmp);
  }
}

// Job runner — troll edit from server-side tmp file
async function runTrollJob(jobId, videoPath, ownVideo, phonkName, opts) {
  const upd = (status, extra = {}) => { trollJobs[jobId] = { ...trollJobs[jobId], status, ...extra }; };
  let   out = null;
  try {
    const phonks   = loadPhonks();
    const phonkInfo = phonks.find(p => p.name === phonkName);
    if (!phonkInfo) throw new Error('Phonk পাওয়া যায়নি: ' + phonkName);

    upd('processing', { step: 'FFmpeg processing...' });
    out = await processTroll(videoPath, phonkInfo, opts);

    const dt = await getDrive();
    const results = {};

    if (opts.toDrive !== false) {
      upd('uploading', { step: 'Drive upload...' });
      const d = await driveUpload(out, `troll_${Date.now()}.mp4`, opts.driveFolder || null, dt);
      results.driveId = d.id;
      log(`[TROLL JOB] Drive: ${d.id}`);
    }
    if (opts.toYT) {
      upd('uploading_yt', { step: 'YouTube upload...' });
      const ytTok = await getYT();
      if (!ytTok) throw new Error('YouTube connect করো');
      const title = (phonkName + ' 💀 #shorts #viral #waitforit').slice(0, 100);
      const ytId  = await ytUpload(out, { title }, opts.ytPrivacy || 'public', ytTok);
      results.youtubeId  = ytId;
      results.youtubeUrl = `https://youtu.be/${ytId}`;
      log(`[TROLL JOB] YT: ${ytId}`);
    }

    upd('done', { step: '✅ সম্পন্ন!', ...results });
  } catch (e) {
    upd('error', { error: e.message });
    log(`[TROLL JOB] ❌ ${e.message}`);
  } finally {
    if (ownVideo) clean(videoPath);
    clean(out);
  }
}

// ================================================================
//  SECTION 7 : MOVIE SLICER  (Shorts output)
// ================================================================
const movieJobs = {};

async function runMovieJob(jobId, srcPath, ownSrc, opts) {
  const upd = (status, extra = {}) => { movieJobs[jobId] = { ...movieJobs[jobId], status, ...extra }; };
  const clipFiles = [];

  try {
    const srcDur   = await duration(srcPath);
    if (!srcDur) throw new Error('Video duration পাওয়া যায়নি');
    log(`[MOVIE] Source: ${srcDur.toFixed(1)}s`);

    const clipDur   = Math.min(opts.clipDuration || 60, srcDur);
    const maxClips  = Math.floor(srcDur / clipDur);
    const count     = Math.min(opts.clipCount || 5, maxClips || 1);
    const step      = (srcDur - clipDur) / Math.max(count - 1, 1);

    log(`[MOVIE] ${count} clips × ${clipDur}s`);

    // Build video filter chain
    const grade    = GRADES[opts.colorGrade] || '';
    const gradeStr = grade ? `${grade},` : '';
    const cropStr  = `${shortsFilter()},`;  // → 1080×1920
    const bobStr   = opts.headBob !== false
      ? `${headBobFilter(opts.headBobIntensity || 2)},`
      : '';

    // Full video filter: crop to 9:16 → color grade → head bob
    const vf = `${cropStr}${gradeStr}${bobStr}`.slice(0, -1);  // remove trailing comma

    const dt     = await getDrive();
    if (!dt) throw new Error('Drive connect করো');
    const clips  = [];

    for (let i = 0; i < count; i++) {
      const start   = (step * i).toFixed(3);
      const clipOut = tmp(`clip_${i}`, 'mp4');
      clipFiles.push(clipOut);

      upd('processing', { step: `Clip ${i+1}/${count}...`, progress: Math.round(i/count*70) });

      // Audio: dialog volume
      const dVol = opts.dialogVol ?? 1.0;
      let cmd;
      if (opts.phonkInfo) {
        // Phonk audio — mute original, add phonk
        const phonkTmp2 = tmp('ph2', 'mp3');
        await driveStreamToFile(opts.phonkInfo.driveId, phonkTmp2);
        const pDur2  = opts.phonkInfo.duration || await duration(phonkTmp2);
        const drop2  = opts.phonkInfo.dropTime || 0;
        const aFilter = opts.phonkLoop
          ? `[1:a]aloop=loop=-1:size=2000000000,atrim=end=${clipDur},asetpts=PTS-STARTPTS,volume=${(opts.musicVol??0.8).toFixed(2)}[outa]`
          : `[1:a]atrim=start=${drop2},asetpts=PTS-STARTPTS,atrim=end=${clipDur},asetpts=PTS-STARTPTS,volume=${(opts.musicVol??0.8).toFixed(2)}[outa]`;
        cmd = `ffmpeg -y -ss ${start} -t ${clipDur} -i "${srcPath}" -i "${phonkTmp2}" -filter_complex "${aFilter}" -vf "${vf}" -map 0:v -map "[outa]" -c:v libx264 -preset fast -crf 20 -c:a aac -b:a 192k -movflags +faststart "${clipOut}"`;
        await execAsync(cmd, { timeout: 15 * 60 * 1000 });
        clean(phonkTmp2);
      } else {
        // Original dialog audio
        cmd = `ffmpeg -y -ss ${start} -t ${clipDur} -i "${srcPath}" -vf "${vf}" -af "volume=${dVol.toFixed(2)}" -c:v libx264 -preset fast -crf 20 -c:a aac -b:a 192k -movflags +faststart "${clipOut}"`;
        await execAsync(cmd, { timeout: 15 * 60 * 1000 });
      }

      if (!fs.existsSync(clipOut)) throw new Error(`Clip ${i+1} তৈরি হয়নি`);

      upd('uploading', { step: `Clip ${i+1} Drive upload...`, progress: Math.round(70 + i/count*30) });
      const d = await driveUpload(clipOut, `clip_${i+1}_${Date.now()}.mp4`, opts.outputFolder || null, dt);
      clips.push({ clip: i+1, id: d.id, name: d.name });
      log(`[MOVIE] Clip ${i+1}: ${d.id}`);
    }

    upd('done', { step: '✅ সম্পন্ন!', progress: 100, clips, totalClips: count });
  } catch (e) {
    upd('error', { error: e.message });
    log(`[MOVIE] ❌ ${e.message}`);
  } finally {
    if (ownSrc) clean(srcPath);
    clean(clipFiles);
  }
}

// ================================================================
//  SECTION 8 : DRIVE UTILITIES
// ================================================================
async function driveList(folderId, dt) {
  const fetch = await gf();
  let all = [], pt = null;
  do {
    let url = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+mimeType+contains+'video/'+and+trashed=false&fields=files(id,name,size),nextPageToken&pageSize=1000`;
    if (pt) url += '&pageToken=' + pt;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${dt}` } });
    if (!r.ok) throw new Error(`Drive list ${r.status}`);
    const d = await r.json();
    if (d.error) throw new Error('Drive: ' + d.error.message);
    all = all.concat(d.files || []);
    pt  = d.nextPageToken || null;
  } while (pt);
  return all;
}

async function driveStreamToFile(fileId, destPath) {
  const fetch = await gf(), dt = await getDrive();
  const r = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`, { headers: { Authorization: `Bearer ${dt}` } });
  if (!r.ok) throw new Error(`Drive download ${r.status}`);
  await new Promise((res, rej) => {
    const dest = fs.createWriteStream(destPath);
    r.body.pipe(dest);
    dest.on('finish', res); dest.on('error', rej); r.body.on('error', rej);
  });
}

// Resumable upload — handles any file size
async function driveUpload(localPath, name, folderId, dt, mime = 'video/mp4') {
  const fetch = await gf();
  const stat  = fs.statSync(localPath);
  const meta  = { name, parents: folderId ? [folderId] : [] };
  const init  = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable', {
    method: 'POST',
    headers: { Authorization: `Bearer ${dt}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': mime, 'X-Upload-Content-Length': String(stat.size) },
    body: JSON.stringify(meta)
  });
  if (!init.ok) throw new Error('Drive init: ' + (await init.text()).slice(0, 200));
  const uploadUrl = init.headers.get('location');
  const up = await fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': mime, 'Content-Length': String(stat.size) }, body: fs.createReadStream(localPath) });
  if (!up.ok) throw new Error('Drive upload: ' + (await up.text()).slice(0, 200));
  return up.json();
}

// ================================================================
//  SECTION 9 : DOWNLOADER
// ================================================================
const dlJobs = {};

const YT_FLAGS = `--extractor-args "youtube:player_client=web_embedded" --no-check-certificates`;

async function runDlJob(jobId, urls, folderId) {
  const dt  = await getDrive();
  if (!dt) { dlJobs[jobId] = { status: 'error', error: 'Drive connect করো' }; return; }
  let ok = 0, fail = 0;
  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    if (!url) continue;
    Object.assign(dlJobs[jobId], { step: `${i+1}/${urls.length}`, current: i+1 });
    log(`[DL] ${i+1}/${urls.length}: ${url.slice(0,60)}`);
    let f = null;
    try {
      f = tmp('dl', 'mp4');
      // Try direct HTTP first (for CDN direct links like hakunaymatata.com etc.)
      let done = false;
      try {
        const fetch = await gf();
        const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, redirect: 'follow' });
        const ct = r.headers.get('content-type') || '';
        if (r.ok && (ct.includes('video') || ct.includes('octet-stream'))) {
          await new Promise((res, rej) => { const d = fs.createWriteStream(f); r.body.pipe(d); d.on('finish', res); d.on('error', rej); r.body.on('error', rej); });
          done = true;
        }
      } catch {}
      if (!done) {
        const tpl = f.replace('.mp4', '.%(ext)s');
        await execAsync(`yt-dlp -f "best[ext=mp4]/best" --merge-output-format mp4 --no-playlist ${YT_FLAGS} -o "${tpl}" "${url}"`, { timeout: 180000 });
        const files = fs.readdirSync(TEMP).filter(x => x.startsWith(path.basename(f, '.mp4')) && x.endsWith('.mp4'));
        if (files.length) f = path.join(TEMP, files[0]);
      }
      if (!fs.existsSync(f)) throw new Error('File not found after download');
      await driveUpload(f, `video_${Date.now()}.mp4`, folderId || null, dt);
      ok++;
    } catch (e) {
      fail++;
      log(`[DL] ❌ ${e.message}`);
    } finally { clean(f); }
  }
  dlJobs[jobId] = { status: 'done', success: ok, failed: fail, total: urls.length };
  log(`[DL] Done: ${ok} ok, ${fail} fail`);
}

// ================================================================
//  SECTION 10 : AUTO UPLOAD & SCHEDULER
// ================================================================
let uploading = false, uploadAt = 0;
const UPLOAD_TIMEOUT = 20 * 60 * 1000;

async function runAutoUpload(folderId, template, privacy) {
  if (uploading && (Date.now() - uploadAt) < UPLOAD_TIMEOUT) { log('[AUTO] Already running'); return; }
  uploading = true; uploadAt = Date.now();
  let vTmp = null, out = null;
  try {
    const dt = await getDrive(); if (!dt) throw new Error('Drive connect করো');
    const yt = await getYT();   if (!yt) throw new Error('YouTube connect করো');

    const videos = await driveList(folderId, dt);
    if (!videos.length) throw new Error('Folder empty: ' + folderId);
    const video  = nextVideo(folderId, videos);
    log(`[AUTO] Selected: ${video.name}`);

    vTmp = tmp('auto', 'mp4');
    await driveStreamToFile(video.id, vTmp);
    log(`[AUTO] Downloaded (${(fs.statSync(vTmp).size/1024/1024).toFixed(1)}MB)`);

    const cfg = loadCfg();
    if (cfg.troll?.enabled) {
      const phonks = loadPhonks();
      if (!phonks.length) throw new Error('Phonk নেই — Phonk ট্যাবে যোগ করো');
      const idx  = (cfg.troll.lastPhonkIdx || 0) % phonks.length;
      const phInfo = phonks[idx];
      saveCfg({ ...cfg, troll: { ...cfg.troll, lastPhonkIdx: (idx+1) % phonks.length } });
      out = await processTroll(vTmp, phInfo, { colorFilter: cfg.troll.colorFilter, loopAudio: cfg.troll.loopAudio, textOverlay: cfg.troll.textOverlay });
    } else {
      out = vTmp; vTmp = null;
    }

    const cleanName = video.name.replace(/\.[^.]+$/, '').replace(/[_-]/g, ' ').trim();
    const title     = (template ? template.replace('{{name}}', cleanName) : cleanName).slice(0, 100);
    const ytId      = await ytUpload(out, { title, tags: ['shorts', 'viral'] }, privacy, yt);
    log(`[AUTO] ✅ https://youtu.be/${ytId} "${title}"`);
  } catch (e) { log(`[AUTO] ❌ ${e.message}`); }
  finally { clean(vTmp, out); uploading = false; }
}

// YouTube upload (resumable)
async function ytUpload(filePath, meta, privacy, ytTok) {
  const fetch  = await gf();
  const stat   = fs.statSync(filePath);
  const body   = {
    snippet: { title: (meta.title || 'Video').slice(0, 100), description: meta.description || '', tags: (meta.tags || []).slice(0, 30), categoryId: '22' },
    status:  { privacyStatus: privacy || 'public', selfDeclaredMadeForKids: false }
  };
  const init = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
    method: 'POST',
    headers: { Authorization: `Bearer ${ytTok}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': 'video/mp4', 'X-Upload-Content-Length': String(stat.size) },
    body: JSON.stringify(body)
  });
  if (!init.ok) throw new Error('YT init: ' + (await init.text()).slice(0, 300));
  const uploadUrl = init.headers.get('location');
  if (!uploadUrl) throw new Error('YT upload URL নেই');
  const up = await fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': 'video/mp4', 'Content-Length': String(stat.size) }, body: fs.createReadStream(filePath) });
  if (!up.ok) throw new Error('YT upload: ' + (await up.text()).slice(0, 300));
  const d = await up.json();
  if (!d.id) throw new Error('No video ID');
  return d.id;
}

// Scheduler
const fired = new Set(); let lastDay2 = '';
setInterval(async () => {
  try {
    const cfg = loadCfg(); if (!cfg.enabled) return;
    const today = bdDay(), bdMin = bdMinutes();
    if (today !== lastDay2) { fired.clear(); lastDay2 = today; }
    const dc = cfg.days?.[today];
    if (!dc?.enabled || !dc?.folderId?.trim()) return;
    for (const slot of (cfg.slots || [])) {
      if (!slot.enabled) continue;
      const [h, m] = slot.time.split(':').map(Number);
      const key = `${today}_${slot.time}`;
      if (!fired.has(key) && Math.abs(bdMin - (h*60+m)) <= 1) {
        fired.add(key);
        log(`[SCHED] ⏰ ${slot.time} | ${today}`);
        runAutoUpload(dc.folderId.trim(), dc.template, cfg.privacy).catch(e => log('[SCHED] ' + e.message));
        break;
      }
    }
  } catch (e) { console.error('[SCHED]', e.message); }
}, 30000);

// Token refresh every 45 min
setInterval(async () => {
  const t = loadTok();
  if (t.refresh_token)       await refreshYT().catch(() => {});
  if (t.drive_refresh_token) await refreshDrive().catch(() => {});
}, 45 * 60 * 1000);

// ================================================================
//  SECTION 11 : ROUTES
// ================================================================

// Health
app.get('/health', (req, res) => res.json({ ok: true, time: bdTime(), uploading }));

// Status
app.get('/api/status', (req, res) => {
  const t = loadTok();
  res.json({ youtube: !!t.access_token, drive: !!t.drive_access_token, channelName: t.channel_name || null, uploading });
});

// Drive token (for client-side upload)
app.get('/api/drive-token', async (req, res) => {
  const dt = await getDrive();
  if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
  res.json({ token: dt });
});

// Config
app.get('/api/config', (req, res) => res.json(loadCfg()));
app.post('/api/config', (req, res) => {
  try {
    const cur = loadCfg(), inc = req.body;
    const days = Object.fromEntries(DAYS.map(d => [d, { ...defaultDay(), ...(cur.days[d]||{}), ...(inc.days?.[d]||{}) }]));
    saveCfg({ ...cur, ...inc, days, troll: { ...cur.troll, ...(inc.troll||{}) } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Logs
app.get('/api/logs', (req, res) => res.json({ logs: LOGS.slice(0, 120) }));

// Queue
app.get('/api/queue', (req, res) => {
  const q = loadQueue(), s = {};
  Object.keys(q).forEach(k => { s[k] = { remaining: (q[k].remaining||[]).length, used: (q[k].used||[]).length }; });
  res.json(s);
});
app.post('/api/queue/reset', (req, res) => {
  const { folderId } = req.body, q = loadQueue();
  folderId ? delete q[folderId] : Object.keys(q).forEach(k => delete q[k]);
  saveQueue(q); res.json({ success: true });
});

// Test folder
app.post('/api/test-folder', async (req, res) => {
  try {
    const dt = await getDrive(); if (!dt) return res.status(401).json({ error: 'Drive connect করো' });
    const videos = await driveList(req.body.folderId?.trim(), dt);
    res.json({ success: true, count: videos.length, sample: videos.slice(0, 3).map(v => v.name) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Run now
app.post('/api/run-now', async (req, res) => {
  const { folderId, template, privacy } = req.body;
  if (!folderId?.trim()) return res.status(400).json({ error: 'folderId দাও' });
  if (uploading && (Date.now()-uploadAt) < UPLOAD_TIMEOUT) return res.status(409).json({ error: 'Upload চলছে' });
  res.json({ success: true });
  runAutoUpload(folderId.trim(), template, privacy).catch(() => {});
});

// Skull upload
app.post('/api/skull', (req, res) => {
  try {
    const { base64 } = req.body;
    if (!base64) return res.status(400).json({ error: 'base64 দাও' });
    fs.writeFileSync(F.skull, Buffer.from(base64.replace(/^data:[^;]+;base64,/, ''), 'base64'));
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/skull', (req, res) => res.json({ exists: fs.existsSync(F.skull) }));

// Preview — stream Drive file
app.get('/api/preview/:id', async (req, res) => {
  try {
    const fetch = await gf(), dt = await getDrive();
    if (!dt) return res.status(401).send('Drive connect করো');
    const r = await fetch(`https://www.googleapis.com/drive/v3/files/${req.params.id}?alt=media`, { headers: { Authorization: `Bearer ${dt}` } });
    if (!r.ok) return res.status(404).send('Not found');
    res.setHeader('Content-Type', r.headers.get('content-type') || 'video/mp4');
    r.body.pipe(res);
  } catch (e) { res.status(500).send(e.message); }
});

// ── PHONK ROUTES ─────────────────────────────────────────────────
app.get('/api/phonk', (req, res) => res.json(loadPhonks()));

// Upload from file (multipart)
app.post('/api/phonk/file', uploader.single('audio'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'audio file দাও' });
    const { name, dropTime } = req.body;
    if (!name) { clean(req.file.path); return res.status(400).json({ error: 'name দাও' }); }
    const rec = await savePhonkFromFile(req.file.path, name.trim(), parseFloat(dropTime)||0);
    clean(req.file.path);
    res.json({ success: true, phonk: rec });
  } catch (e) { clean(req.file?.path); res.status(500).json({ error: e.message }); }
});

// Download from YouTube URL (async job)
app.post('/api/phonk/yt', async (req, res) => {
  const { ytUrl, name, dropTime } = req.body;
  if (!ytUrl || !name) return res.status(400).json({ error: 'ytUrl, name দাও' });
  const jobId = Date.now().toString();
  pJobs[jobId] = { status: 'downloading' };
  res.json({ success: true, jobId });
  dlPhonkFromYT(ytUrl, name.trim(), parseFloat(dropTime)||0)
    .then(p  => { pJobs[jobId] = { status: 'done', phonk: p }; })
    .catch(e => { pJobs[jobId] = { status: 'error', error: e.message }; log('[PHONK YT] ❌ '+e.message); });
});
app.get('/api/phonk/job/:id', (req, res) => {
  const j = pJobs[req.params.id]; if (!j) return res.status(404).json({ error: 'Not found' });
  res.json(j);
});
app.delete('/api/phonk/:name', (req, res) => {
  savePhonks(loadPhonks().filter(p => p.name !== decodeURIComponent(req.params.name)));
  res.json({ success: true });
});

// ── TROLL ROUTES ─────────────────────────────────────────────────
// Upload video file for troll (multipart streaming)
app.post('/api/troll/upload', uploader.single('video'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'video file দাও' });
  log(`[TROLL] Upload: ${req.file.originalname} (${(req.file.size/1024/1024).toFixed(1)}MB)`);
  res.json({ success: true, tmpPath: req.file.path, size: req.file.size });
});

// Start troll edit job
app.post('/api/troll/start', async (req, res) => {
  const { tmpPath, driveFileId, phonkName, colorFilter, loopAudio, textOverlay, toDrive, driveFolder, toYT, ytPrivacy } = req.body;
  if (!tmpPath && !driveFileId) return res.status(400).json({ error: 'tmpPath বা driveFileId দাও' });
  if (!phonkName) return res.status(400).json({ error: 'phonkName দাও' });

  const jobId = Date.now().toString();
  trollJobs[jobId] = { status: 'starting', step: 'শুরু হচ্ছে...' };
  res.json({ success: true, jobId });

  (async () => {
    let videoPath = null, ownVideo = false;
    try {
      if (tmpPath) {
        if (!fs.existsSync(tmpPath)) throw new Error('Temp file নেই — আবার upload করো');
        videoPath = tmpPath; ownVideo = false;
      } else {
        trollJobs[jobId] = { status: 'downloading', step: 'Drive download...' };
        videoPath = tmp('tv', 'mp4'); ownVideo = true;
        await driveStreamToFile(driveFileId, videoPath);
        log(`[TROLL] Downloaded (${(fs.statSync(videoPath).size/1024/1024).toFixed(1)}MB)`);
      }
      await runTrollJob(jobId, videoPath, ownVideo, phonkName,
        { colorFilter, loopAudio, textOverlay, toDrive: toDrive !== false, driveFolder, toYT: !!toYT, ytPrivacy });
    } catch(e) {
      trollJobs[jobId] = { status: 'error', error: e.message };
      log('[TROLL START] ❌ ' + e.message);
      if (ownVideo) clean(videoPath);
    }
  })();
});
app.get('/api/troll/job/:id', (req, res) => {
  const j = trollJobs[req.params.id]; if (!j) return res.status(404).json({ error: 'Not found' });
  res.json(j);
});

// ── MOVIE ROUTES ─────────────────────────────────────────────────
// Upload video for movie slicer (multipart)
app.post('/api/movie/upload', uploader.single('video'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'video file দাও' });
  log(`[MOVIE] Upload: ${req.file.originalname} (${(req.file.size/1024/1024).toFixed(1)}MB)`);
  res.json({ success: true, tmpPath: req.file.path, size: req.file.size });
});

// Start movie slicer
app.post('/api/movie/start', async (req, res) => {
  const { tmpPath, driveFileId, url, clipDuration, clipCount, colorGrade,
          headBob, headBobIntensity, dialogVol, phonkName, phonkLoop,
          musicVol, outputFolder } = req.body;

  if (!tmpPath && !driveFileId && !url) return res.status(400).json({ error: 'source দাও' });
  const dt = await getDrive();
  if (!dt) return res.status(401).json({ error: 'Drive connect করো' });

  const jobId = Date.now().toString();
  movieJobs[jobId] = { status: 'starting', progress: 0 };
  res.json({ success: true, jobId });

  (async () => {
    let srcPath = null, ownSrc = false;
    try {
      if (tmpPath) {
        if (!fs.existsSync(tmpPath)) throw new Error('Temp file নেই');
        srcPath = tmpPath; ownSrc = false;
      } else if (driveFileId) {
        movieJobs[jobId] = { status: 'downloading', step: 'Drive download...' };
        srcPath = tmp('movie_src', 'mp4'); ownSrc = true;
        await driveStreamToFile(driveFileId, srcPath);
        log(`[MOVIE] Drive downloaded (${(fs.statSync(srcPath).size/1024/1024).toFixed(1)}MB)`);
      } else if (url) {
        movieJobs[jobId] = { status: 'downloading', step: 'URL download...' };
        srcPath = tmp('movie_url', 'mp4'); ownSrc = true;
        // Try direct
        let done = false;
        try {
          const fetch = await gf();
          const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, redirect: 'follow' });
          const ct = r.headers.get('content-type') || '';
          if (r.ok && (ct.includes('video') || ct.includes('octet-stream'))) {
            await new Promise((res2, rej) => { const d = fs.createWriteStream(srcPath); r.body.pipe(d); d.on('finish', res2); d.on('error', rej); r.body.on('error', rej); });
            done = true;
          }
        } catch {}
        if (!done) {
          const tpl = srcPath.replace('.mp4', '.%(ext)s');
          await execAsync(`yt-dlp -f "bestvideo[ext=mp4]+bestaudio/best[ext=mp4]/best" --merge-output-format mp4 --no-playlist ${YT_FLAGS} -o "${tpl}" "${url}"`, { timeout: 300000 });
          const dlId = path.basename(srcPath, '.mp4');
          const files = fs.readdirSync(TEMP).filter(f => f.startsWith(dlId) && f.endsWith('.mp4'));
          if (!files.length) throw new Error('URL download failed');
          srcPath = path.join(TEMP, files[0]);
        }
        log(`[MOVIE] URL downloaded (${(fs.statSync(srcPath).size/1024/1024).toFixed(1)}MB)`);
      }

      // Resolve phonk info
      let phonkInfo = null;
      if (phonkName) {
        phonkInfo = loadPhonks().find(p => p.name === phonkName);
        if (!phonkInfo) log(`[MOVIE] Phonk not found: ${phonkName}`);
      }

      await runMovieJob(jobId, srcPath, ownSrc, {
        clipDuration:    parseInt(clipDuration)    || 60,
        clipCount:       parseInt(clipCount)       || 5,
        colorGrade:      colorGrade || 'teal_orange',
        headBob:         headBob !== false,
        headBobIntensity: parseFloat(headBobIntensity) || 2,
        dialogVol:       parseFloat(dialogVol)     ?? 1.0,
        musicVol:        parseFloat(musicVol)      ?? 0.8,
        phonkInfo,
        phonkLoop:       !!phonkLoop,
        outputFolder:    outputFolder || null,
      });
    } catch(e) {
      movieJobs[jobId] = { status: 'error', error: e.message };
      log('[MOVIE START] ❌ ' + e.message);
      if (ownSrc) clean(srcPath);
    }
  })();
});
app.get('/api/movie/job/:id', (req, res) => {
  const j = movieJobs[req.params.id]; if (!j) return res.status(404).json({ error: 'Not found' });
  res.json(j);
});

// ── DOWNLOAD ROUTES ──────────────────────────────────────────────
app.post('/api/dl/start', async (req, res) => {
  const { urls, folderId } = req.body;
  if (!urls?.length) return res.status(400).json({ error: 'urls দাও' });
  const jobId = Date.now().toString();
  dlJobs[jobId] = { status: 'running', total: urls.length, success: 0, failed: 0 };
  res.json({ success: true, jobId, total: urls.length });
  runDlJob(jobId, urls.filter(Boolean), folderId || null).catch(() => {});
});
app.get('/api/dl/job/:id', (req, res) => {
  const j = dlJobs[req.params.id]; if (!j) return res.status(404).json({ error: 'Not found' });
  res.json(j);
});

app.get('/api/grades', (req, res) => res.json(Object.keys(GRADES)));

// ================================================================
//  SECTION 12 : OAUTH
// ================================================================
app.get('/auth/youtube', (req, res) => {
  const sc = encodeURIComponent('https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/youtube/callback')}&response_type=code&scope=${sc}&access_type=offline&prompt=consent`);
});
app.get('/auth/youtube/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error || !code) return res.send(htmlERR(error || 'No code'));
  try {
    const fetch = await gf();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, redirect_uri: process.env.BASE_URL+'/auth/youtube/callback', grant_type: 'authorization_code' })
    });
    const d = await r.json();
    if (!d.access_token) throw new Error(JSON.stringify(d));
    tokExp.yt = Date.now() + (d.expires_in||3600)*1000 - 300000;
    let ch = '';
    try { ch = (await (await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers: { Authorization: `Bearer ${d.access_token}` } })).json()).items?.[0]?.snippet?.title || ''; } catch {}
    saveTok({ ...loadTok(), access_token: d.access_token, refresh_token: d.refresh_token, channel_name: ch });
    log('[AUTH] YT: ' + ch);
    res.send(htmlOK('YouTube সংযুক্ত!', ch));
  } catch (e) { res.send(htmlERR(e.message)); }
});

app.get('/auth/drive', (req, res) => {
  const sc = encodeURIComponent('https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/drive.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/drive/callback')}&response_type=code&scope=${sc}&access_type=offline&prompt=consent`);
});
app.get('/auth/drive/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error || !code) return res.send(htmlERR(error || 'No code'));
  try {
    const fetch = await gf();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: process.env.YT_CLIENT_ID, client_secret: process.env.YT_CLIENT_SECRET, redirect_uri: process.env.BASE_URL+'/auth/drive/callback', grant_type: 'authorization_code' })
    });
    const d = await r.json();
    if (!d.access_token) throw new Error(JSON.stringify(d));
    tokExp.drive = Date.now() + (d.expires_in||3600)*1000 - 300000;
    saveTok({ ...loadTok(), drive_access_token: d.access_token, drive_refresh_token: d.refresh_token });
    log('[AUTH] Drive ✅');
    res.send(htmlOK('Google Drive সংযুক্ত!'));
  } catch (e) { res.send(htmlERR(e.message)); }
});

// ================================================================
//  SECTION 13 : STARTUP
// ================================================================
async function startup() {
  log('🚀 Auto Waz 2.0 starting...');
  // Ensure yt-dlp
  try { await execAsync('yt-dlp --version'); log('[SETUP] yt-dlp ✅'); }
  catch {
    log('[SETUP] Installing yt-dlp...');
    try { await execAsync('curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && chmod a+rx /usr/local/bin/yt-dlp', { timeout: 60000 }); log('[SETUP] yt-dlp installed ✅'); }
    catch (e2) { log('[SETUP] yt-dlp install fail: ' + e2.message); }
  }
  await restoreTokens();
  const t = loadTok();
  if (t.refresh_token)       await refreshYT().catch(() => {});
  if (t.drive_refresh_token) await refreshDrive().catch(() => {});
  await restoreCfg();
  await restoreQueue();
  log('✅ Ready!');
}

startup();
app.listen(PORT, () => console.log(`Auto Waz 2.0 on port ${PORT}`));
