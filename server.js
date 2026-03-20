const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const TEMP_DIR = path.join(__dirname, 'temp');
const TOKEN_FILE = path.join(__dirname, 'tokens.json');
const CONFIG_FILE = path.join(__dirname, 'config.json');
const QUEUE_FILE = path.join(__dirname, 'queue.json');

if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ========== HELPERS ==========
const logs = [];
function log(msg) {
  const line = `[${getBDTime()}] ${msg}`;
  console.log(line);
  logs.unshift(line);
  if (logs.length > 300) logs.length = 300;
}
function getBDTime() {
  return new Date(Date.now() + 6 * 3600000).toISOString().replace('T',' ').slice(0,19);
}
function getBDMinutes() {
  return Math.floor((Date.now() + 6 * 3600000) / 60000) % 1440;
}
function getBDDay() {
  return new Date().toLocaleDateString('en-US', { weekday:'long', timeZone:'Asia/Dhaka' });
}
function readJSON(file, fallback) {
  try { if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file,'utf8')); } catch {}
  return fallback !== undefined ? fallback : {};
}
function writeJSON(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}
let _fetch = null;
async function getFetch() {
  if (!_fetch) _fetch = (await import('node-fetch')).default;
  return _fetch;
}
function htmlMsg(title, color, sub='') {
  const icon = title.includes('✅') ? '✅' : '❌';
  return `<html><body style="background:#0a0a0f;color:${color};font-family:sans-serif;text-align:center;padding:60px"><div style="font-size:48px">${icon}</div><h2>${title}</h2>${sub?`<p style="color:#fff">${sub}</p>`:''}<p style="color:#666;font-size:13px">পেজ বন্ধ করুন</p><script>setTimeout(()=>window.close(),2500)</script></body></html>`;
}

// ========== TOKEN SYSTEM ==========
// FIX: Track expiry to avoid unnecessary refresh calls
const tokenExpiry = { yt: 0, drive: 0 };
const TOKEN_DRIVE_NAME = 'autowaz2_tokens.json';
let tokenDriveFileId = null;

function loadTokens() { return readJSON(TOKEN_FILE, {}); }

// FIX: saveTokens passes token directly — avoids circular getValidDriveToken call
function saveTokens(t) {
  writeJSON(TOKEN_FILE, t);
  const driveToken = t.drive_access_token;
  if (driveToken) backupTokensToDrive(t, driveToken).catch(()=>{});
}

async function backupTokensToDrive(tokens, driveToken) {
  try {
    const fetch = await getFetch();
    const content = JSON.stringify(tokens, null, 2);
    if (tokenDriveFileId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${tokenDriveFileId}?uploadType=media`, {
        method:'PATCH', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':'application/json'}, body:content
      });
      if (!r.ok) tokenDriveFileId = null;
    } else {
      const boundary = 'tokenbnd';
      const meta = JSON.stringify({name:TOKEN_DRIVE_NAME,mimeType:'application/json'});
      const body = `--${boundary}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${boundary}\r\nContent-Type: application/json\r\n\r\n${content}\r\n--${boundary}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method:'POST', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':`multipart/related; boundary=${boundary}`}, body
      });
      if (r.ok) { const d = await r.json(); if (d.id) { tokenDriveFileId = d.id; log('[TOKEN] Backup তৈরি ✅'); } }
    }
  } catch(e) { console.warn('[TOKEN] Backup:',e.message); }
}

async function refreshYTToken() {
  try {
    const fetch = await getFetch();
    const t = loadTokens();
    if (!t.refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body: new URLSearchParams({refresh_token:t.refresh_token,client_id:process.env.YT_CLIENT_ID,client_secret:process.env.YT_CLIENT_SECRET,grant_type:'refresh_token'})
    });
    const d = await r.json();
    if (d.access_token) {
      tokenExpiry.yt = Date.now() + (d.expires_in||3600)*1000 - 300000;
      const updated = {...t, access_token:d.access_token};
      writeJSON(TOKEN_FILE, updated); // direct write — NOT saveTokens (avoids backup loop)
      if (updated.drive_access_token) backupTokensToDrive(updated, updated.drive_access_token).catch(()=>{});
      log('[TOKEN] YouTube refresh ✅');
      return d.access_token;
    }
    log('[TOKEN] YT refresh failed: '+JSON.stringify(d));
  } catch(e) { log('[TOKEN] YT refresh error: '+e.message); }
  return null;
}

async function refreshDriveToken() {
  try {
    const fetch = await getFetch();
    const t = loadTokens();
    if (!t.drive_refresh_token) return null;
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body: new URLSearchParams({refresh_token:t.drive_refresh_token,client_id:process.env.YT_CLIENT_ID,client_secret:process.env.YT_CLIENT_SECRET,grant_type:'refresh_token'})
    });
    const d = await r.json();
    if (d.access_token) {
      tokenExpiry.drive = Date.now() + (d.expires_in||3600)*1000 - 300000;
      const updated = {...t, drive_access_token:d.access_token};
      writeJSON(TOKEN_FILE, updated);
      backupTokensToDrive(updated, d.access_token).catch(()=>{});
      log('[TOKEN] Drive refresh ✅');
      return d.access_token;
    }
    log('[TOKEN] Drive refresh failed: '+JSON.stringify(d));
  } catch(e) { log('[TOKEN] Drive refresh error: '+e.message); }
  return null;
}

// FIX: Only refresh when expired
async function getValidYTToken() {
  if (Date.now() < tokenExpiry.yt) { const t=loadTokens(); if (t.access_token) return t.access_token; }
  const r = await refreshYTToken();
  if (r) return r;
  return loadTokens().access_token || null;
}
async function getValidDriveToken() {
  if (Date.now() < tokenExpiry.drive) { const t=loadTokens(); if (t.drive_access_token) return t.drive_access_token; }
  const r = await refreshDriveToken();
  if (r) return r;
  return loadTokens().drive_access_token || null;
}

async function restoreTokensFromDrive() {
  try {
    const fetch = await getFetch();
    const local = loadTokens();
    const bootstrapToken = local.drive_access_token || process.env.DRIVE_ACCESS_TOKEN;
    if (!bootstrapToken) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${TOKEN_DRIVE_NAME}' and trashed=false&fields=files(id,name)&pageSize=5`, {
      headers:{'Authorization':`Bearer ${bootstrapToken}`}
    });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    tokenDriveFileId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${tokenDriveFileId}?alt=media`, {
      headers:{'Authorization':`Bearer ${bootstrapToken}`}
    });
    if (!fr.ok) return;
    const tokens = await fr.json();
    if (tokens?.refresh_token || tokens?.drive_refresh_token) {
      writeJSON(TOKEN_FILE, tokens);
      log('[TOKEN] Drive থেকে restore ✅');
    }
  } catch(e) { console.warn('[TOKEN] Restore:',e.message); }
}

// ========== CONFIG SYSTEM ==========
const DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
const CONFIG_DRIVE_NAME = 'autowaz2_config.json';
let configDriveFileId = null;

function defaultDayConfig() { return {folderId:'',template:'',enabled:true}; }
function defaultConfig() {
  const days = {};
  DAYS.forEach(d => { days[d] = defaultDayConfig(); });
  return {enabled:false, slots:[], privacy:'public', seoEnabled:true, grokApiKey:'', uploadTarget:'youtube', days};
}
function loadConfig() {
  const saved = readJSON(CONFIG_FILE, null);
  if (!saved) return defaultConfig();
  const def = defaultConfig();
  // FIX: Deep merge days
  const mergedDays = {...def.days};
  if (saved.days) DAYS.forEach(d => { mergedDays[d] = {...defaultDayConfig(),...(saved.days[d]||{})}; });
  return {...def, ...saved, days:mergedDays};
}
function saveConfig(cfg) {
  writeJSON(CONFIG_FILE, cfg);
  backupConfigToDrive(cfg).catch(()=>{});
}
async function backupConfigToDrive(cfg) {
  try {
    const fetch = await getFetch();
    const t = loadTokens(); // direct load — no refresh here
    const driveToken = t.drive_access_token;
    if (!driveToken) return;
    const content = JSON.stringify(cfg, null, 2);
    if (configDriveFileId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${configDriveFileId}?uploadType=media`, {
        method:'PATCH', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':'application/json'}, body:content
      });
      if (!r.ok) configDriveFileId = null;
    } else {
      const boundary = 'cfgbnd';
      const meta = JSON.stringify({name:CONFIG_DRIVE_NAME});
      const body = `--${boundary}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${boundary}\r\nContent-Type: application/json\r\n\r\n${content}\r\n--${boundary}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method:'POST', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':`multipart/related; boundary=${boundary}`}, body
      });
      if (r.ok) { const d=await r.json(); if(d.id){configDriveFileId=d.id; log('[CONFIG] Drive backup ✅');} }
    }
  } catch {}
}
async function restoreConfigFromDrive() {
  try {
    const fetch = await getFetch();
    const driveToken = await getValidDriveToken();
    if (!driveToken) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${CONFIG_DRIVE_NAME}' and trashed=false&fields=files(id,name)&pageSize=5`, {
      headers:{'Authorization':`Bearer ${driveToken}`}
    });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    configDriveFileId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${configDriveFileId}?alt=media`, {
      headers:{'Authorization':`Bearer ${driveToken}`}
    });
    if (!fr.ok) return;
    const cfg = await fr.json();
    if (cfg && typeof cfg==='object') { writeJSON(CONFIG_FILE, cfg); log('[CONFIG] Drive থেকে restore ✅'); }
  } catch {}
}

// ========== QUEUE SYSTEM ==========
const QUEUE_DRIVE_NAME = 'autowaz2_queue.json';
let queueDriveFileId = null;

function loadQueue() { return readJSON(QUEUE_FILE, {}); }
function saveQueue(q) {
  writeJSON(QUEUE_FILE, q);
  backupQueueToDrive(q).catch(()=>{});
}
async function backupQueueToDrive(q) {
  try {
    const fetch = await getFetch();
    const t = loadTokens();
    const driveToken = t.drive_access_token;
    if (!driveToken) return;
    const content = JSON.stringify(q, null, 2);
    if (queueDriveFileId) {
      const r = await fetch(`https://www.googleapis.com/upload/drive/v3/files/${queueDriveFileId}?uploadType=media`, {
        method:'PATCH', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':'application/json'}, body:content
      });
      if (!r.ok) queueDriveFileId = null;
    } else {
      const boundary='qbnd';
      const meta=JSON.stringify({name:QUEUE_DRIVE_NAME});
      const body=`--${boundary}\r\nContent-Type: application/json\r\n\r\n${meta}\r\n--${boundary}\r\nContent-Type: application/json\r\n\r\n${content}\r\n--${boundary}--`;
      const r = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart', {
        method:'POST', headers:{'Authorization':`Bearer ${driveToken}`,'Content-Type':`multipart/related; boundary=${boundary}`}, body
      });
      if (r.ok) { const d=await r.json(); if(d.id) queueDriveFileId=d.id; }
    }
  } catch {}
}
async function restoreQueueFromDrive() {
  try {
    const fetch = await getFetch();
    const driveToken = await getValidDriveToken();
    if (!driveToken) return;
    const r = await fetch(`https://www.googleapis.com/drive/v3/files?q=name='${QUEUE_DRIVE_NAME}' and trashed=false&fields=files(id,name)&pageSize=5`, {
      headers:{'Authorization':`Bearer ${driveToken}`}
    });
    if (!r.ok) return;
    const data = await r.json();
    if (!data.files?.length) return;
    queueDriveFileId = data.files[0].id;
    const fr = await fetch(`https://www.googleapis.com/drive/v3/files/${queueDriveFileId}?alt=media`, {
      headers:{'Authorization':`Bearer ${driveToken}`}
    });
    if (!fr.ok) return;
    const q = await fr.json();
    if (q && typeof q==='object') { writeJSON(QUEUE_FILE, q); log('[QUEUE] Drive থেকে restore ✅'); }
  } catch {}
}

// FIX: Random FIFO — shuffle new cycle, no double-play, Drive-synced
function getNextVideo(folderId, videos) {
  const q = loadQueue();
  if (!q[folderId]) q[folderId] = {remaining:[], used:[]};
  const allIds = videos.map(v=>v.id);
  let remaining = (q[folderId].remaining||[]).filter(id=>allIds.includes(id));
  if (remaining.length===0) {
    remaining = [...allIds].sort(()=>Math.random()-0.5);
    q[folderId].used = [];
    log(`[QUEUE] নতুন cycle — ${remaining.length}টি ভিডিও random shuffle`);
  }
  const nextId = remaining.shift();
  q[folderId].remaining = remaining;
  q[folderId].used = [...(q[folderId].used||[]).slice(-500), nextId];
  saveQueue(q);
  return videos.find(v=>v.id===nextId) || videos[0];
}

// FIX: Paginated Drive list — handles 1000+ video folders
async function listAllVideos(folderId, driveToken) {
  const fetch = await getFetch();
  let all = [], pageToken = null;
  do {
    let url = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+mimeType+contains+'video/'+and+trashed=false&fields=files(id,name,size),nextPageToken&pageSize=1000`;
    if (pageToken) url += '&pageToken=' + pageToken;
    const r = await fetch(url, {headers:{'Authorization':`Bearer ${driveToken}`}});
    if (!r.ok) throw new Error(`Drive list ${r.status}: ` + (await r.text()).slice(0,200));
    const data = await r.json();
    if (data.error) throw new Error('Drive: '+data.error.message);
    all = all.concat(data.files||[]);
    pageToken = data.nextPageToken||null;
  } while(pageToken);
  return all;
}

// ========== GROK SEO ==========
async function generateSEO(videoName, grokApiKey) {
  try {
    const fetch = await getFetch();
    const apiKey = grokApiKey || process.env.GROK_API_KEY;
    if (!apiKey) throw new Error('Grok API key নেই');
    const prompt = `Islamic YouTube Shorts SEO expert for Bengali Muslim audience.
Video: "${videoName}"
Return ONLY compact JSON no markdown:
{"title":"Bengali/Arabic title max 80 chars with emojis","description":"2-3 lines Bengali Islamic keywords","tags":["waz","bangla waz","islamic","quran","hadith","shorts","viral","allah","iman","muslim","dua","namaz","islamic lecture","bangla islam","motivation"],"hashtags":["#shorts","#viral","#waz","#islamicshorts","#banglaislam","#quran","#hadith","#islamicvideo","#muslimbd","#trending"]}
Rules: 15 tags Bengali+English mix, 10 hashtags must have #shorts, focus waz/lecture/quran.`;
    const r = await fetch('https://api.x.ai/v1/chat/completions', {
      method:'POST', headers:{'Content-Type':'application/json','Authorization':`Bearer ${apiKey}`},
      body: JSON.stringify({model:'grok-3-mini', messages:[{role:'user',content:prompt}], temperature:0.7, max_tokens:400})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error?.message||'Grok '+r.status);
    let text = (d.choices?.[0]?.message?.content||'').replace(/```[\w]*\n?/g,'').trim();
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) throw new Error('No JSON');
    const seo = JSON.parse(m[0]);
    log(`[SEO] ✅ "${seo.title}"`);
    return seo;
  } catch(e) {
    log(`[SEO] ⚠️ Grok: ${e.message}`);
    return null;
  }
}

function getDefaultSEO(cleanName, template) {
  const title = (template ? template.replace('{{name}}',cleanName) : cleanName).substring(0,100);
  return {
    title,
    description: template||'',
    tags:['waz','bangla waz','islamic shorts','shorts','viral','quran','hadith','islamic lecture','bangla islamic','muslim','dua','namaz','iman','allah','rasul'],
    hashtags:['#shorts','#viral','#waz','#islamicshorts','#banglaislam','#quran','#hadith','#islamicvideo','#muslimbd','#trending']
  };
}


// ========== FACEBOOK TOKEN ==========
async function refreshFBToken() {
  try {
    const fetch = await getFetch();
    const t = loadTokens();
    if (!t.fb_access_token) return null;
    // Exchange short-lived for long-lived token (60 days)
    const r = await fetch(
      `https://graph.facebook.com/v19.0/oauth/access_token?grant_type=fb_exchange_token&client_id=${process.env.FB_APP_ID}&client_secret=${process.env.FB_APP_SECRET}&fb_exchange_token=${t.fb_access_token}`
    );
    const d = await r.json();
    if (d.access_token) {
      const updated = {...t, fb_access_token: d.access_token};
      writeJSON(TOKEN_FILE, updated);
      if (updated.drive_access_token) backupTokensToDrive(updated, updated.drive_access_token).catch(()=>{});
      log('[TOKEN] Facebook refresh ✅ (long-lived)');
      return d.access_token;
    }
    log('[TOKEN] FB refresh failed: '+JSON.stringify(d));
  } catch(e) { log('[TOKEN] FB refresh error: '+e.message); }
  return null;
}

async function getFBPageToken(userToken) {
  try {
    const fetch = await getFetch();
    // Get all pages user manages
    const r = await fetch(`https://graph.facebook.com/v19.0/me/accounts?access_token=${userToken}&fields=id,name,access_token`);
    const d = await r.json();
    if (d.data?.length) return d.data; // array of {id, name, access_token}
    return [];
  } catch(e) { log('[FB] Page fetch error: '+e.message); return []; }
}

async function getValidFBToken() {
  const t = loadTokens();
  if (!t.fb_access_token) return null;
  // FB long-lived tokens last 60 days — refresh proactively
  return t.fb_access_token;
}

// ========== FACEBOOK REELS UPLOAD ==========
async function uploadToFacebook(tempFile, fileStat, title, description, pageId, pageToken) {
  const fetch = await getFetch();

  // Step 1: Initialize upload session
  log('[FB] Reels upload শুরু...');
  const initRes = await fetch(`https://graph.facebook.com/v19.0/${pageId}/video_reels`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      upload_phase: 'start',
      access_token: pageToken
    })
  });
  if (!initRes.ok) throw new Error('FB init: ' + (await initRes.text()).slice(0, 300));
  const initData = await initRes.json();
  const uploadSessionId = initData.video_id;
  const uploadUrl = initData.upload_url;
  if (!uploadSessionId || !uploadUrl) throw new Error('FB session ID নেই: ' + JSON.stringify(initData));

  log(`[FB] Session: ${uploadSessionId}`);

  // Step 2: Upload video binary
  const upRes = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      'Authorization': `OAuth ${pageToken}`,
      'offset': '0',
      'file_size': String(fileStat.size),
      'Content-Type': 'application/octet-stream'
    },
    body: fs.createReadStream(tempFile)
  });
  if (!upRes.ok) throw new Error('FB upload: ' + (await upRes.text()).slice(0, 300));

  // Step 3: Finish and publish
  const finishRes = await fetch(`https://graph.facebook.com/v19.0/${pageId}/video_reels`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      upload_phase: 'finish',
      video_id: uploadSessionId,
      access_token: pageToken,
      video_state: 'PUBLISHED',
      description: (title + '\n\n' + description).substring(0, 2200)
    })
  });
  if (!finishRes.ok) throw new Error('FB finish: ' + (await finishRes.text()).slice(0, 300));
  const finishData = await finishRes.json();
  log(`[FB] ✅ Reels published! video_id: ${uploadSessionId}`);
  return uploadSessionId;
}

// ========== UPLOAD CORE ==========
let uploadRunning = false;
let uploadStartTime = 0;
const UPLOAD_TIMEOUT_MS = 15 * 60 * 1000; // 15 min

async function runUpload(folderId, template, privacy, target) {
  // FIX: Timeout on stuck lock
  if (uploadRunning && (Date.now()-uploadStartTime) < UPLOAD_TIMEOUT_MS) {
    log('[UPLOAD] ইতিমধ্যে চলছে, skip');
    return;
  }
  uploadRunning = true;
  uploadStartTime = Date.now();
  let tempFile = null;
  try {
    const fetch = await getFetch();
    const cfg = loadConfig();
    const driveToken = await getValidDriveToken();
    if (!driveToken) throw new Error('Drive সংযুক্ত নয়');
    const ytToken = await getValidYTToken();
    if (!ytToken) throw new Error('YouTube সংযুক্ত নয়');

    // FIX: Paginated list
    log(`[UPLOAD] Drive scan...`);
    const videos = await listAllVideos(folderId, driveToken);
    if (!videos.length) throw new Error(`Folder খালি: ${folderId}`);
    log(`[UPLOAD] ${videos.length}টি ভিডিও`);

    const video = getNextVideo(folderId, videos);
    const sizeMB = ((video.size||0)/1024/1024).toFixed(1);
    log(`[UPLOAD] নির্বাচিত: ${video.name} (${sizeMB}MB)`);

    // FIX: Stream download — not Buffer.from(arrayBuffer) which OOMs
    const dlRes = await fetch(`https://www.googleapis.com/drive/v3/files/${video.id}?alt=media`, {
      headers:{'Authorization':`Bearer ${driveToken}`}
    });
    if (!dlRes.ok) throw new Error(`Drive download ${dlRes.status}: `+(await dlRes.text()).slice(0,200));

    tempFile = path.join(TEMP_DIR, `waz_${Date.now()}.mp4`);
    await new Promise((resolve, reject) => {
      const dest = fs.createWriteStream(tempFile);
      dlRes.body.pipe(dest);
      dest.on('finish', resolve);
      dest.on('error', reject);
      dlRes.body.on('error', reject);
    });
    const fileStat = fs.statSync(tempFile);
    log(`[UPLOAD] Downloaded (${(fileStat.size/1024/1024).toFixed(1)}MB)`);

    // SEO
    const cleanName = video.name.replace(/\.[^.]+$/,'').replace(/[_-]/g,' ').trim();
    let seo = null;
    if (cfg.seoEnabled!==false && (cfg.grokApiKey||process.env.GROK_API_KEY)) {
      seo = await generateSEO(cleanName, cfg.grokApiKey);
    }
    if (!seo) seo = getDefaultSEO(cleanName, template);

    const finalTitle = (seo.title||cleanName).substring(0,100);
    const finalTags = (seo.tags||[]).slice(0,30);
    const finalHashtags = (seo.hashtags||[]).slice(0,15);
    const fullDesc = [seo.description||'', finalHashtags.join(' ')].filter(Boolean).join('\n\n').substring(0,5000);

    log(`[SEO] "${finalTitle}"`);

    const uploadTarget = target || cfg.uploadTarget || 'youtube';
    let ytDone = false, fbDone = false;

    // ===== YOUTUBE UPLOAD =====
    if (uploadTarget === 'youtube' || uploadTarget === 'both') {
      try {
        const ytToken2 = await getValidYTToken();
        if (!ytToken2) throw new Error('YouTube token নেই');
        const meta = {
          snippet:{title:finalTitle, description:fullDesc, categoryId:'22', tags:finalTags, defaultLanguage:'bn', defaultAudioLanguage:'bn'},
          status:{privacyStatus:privacy||'public', selfDeclaredMadeForKids:false, madeForKids:false}
        };
        const initRes = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
          method:'POST',
          headers:{'Authorization':`Bearer ${ytToken2}`,'Content-Type':'application/json','X-Upload-Content-Type':'video/mp4','X-Upload-Content-Length':String(fileStat.size)},
          body: JSON.stringify(meta)
        });
        if (!initRes.ok) throw new Error('YT init: '+(await initRes.text()).slice(0,300));
        const uploadUrl = initRes.headers.get('location');
        if (!uploadUrl) throw new Error('YT upload URL নেই');
        log(`[UPLOAD] YouTube upload...`);
        const upRes = await fetch(uploadUrl, {
          method:'PUT',
          headers:{'Content-Type':'video/mp4','Content-Length':String(fileStat.size)},
          body: fs.createReadStream(tempFile)
        });
        if (!upRes.ok) throw new Error('YT upload: '+(await upRes.text()).slice(0,300));
        const ytData = await upRes.json();
        if (!ytData.id) throw new Error('No video ID');
        log(`[UPLOAD] ✅ YouTube: https://youtu.be/${ytData.id}`);
        ytDone = true;
      } catch(e) { log(`[UPLOAD] ❌ YouTube: ${e.message}`); }
    }

    // ===== FACEBOOK REELS UPLOAD =====
    if (uploadTarget === 'facebook' || uploadTarget === 'both') {
      try {
        const t = loadTokens();
        if (!t.fb_page_id || !t.fb_page_token) throw new Error('Facebook Page connect করো');
        await uploadToFacebook(tempFile, fileStat, finalTitle, fullDesc, t.fb_page_id, t.fb_page_token);
        fbDone = true;
      } catch(e) { log(`[UPLOAD] ❌ Facebook: ${e.message}`); }
    }

    if (!ytDone && !fbDone) throw new Error('সব platform এ upload fail হয়েছে');

  } catch(e) {
    log(`[UPLOAD] ❌ ${e.message}`);
  } finally {
    try { if(tempFile && fs.existsSync(tempFile)) fs.unlinkSync(tempFile); } catch {}
    uploadRunning = false;
  }
}

// ========== SCHEDULER ==========
// FIX: slotKey = "Sunday_09:00" — fires once per slot per day, no double-fire
const firedSlots = new Set();
let lastFiredDay = '';

setInterval(async () => {
  try {
    const cfg = loadConfig();
    if (!cfg.enabled) return;
    const bdMin = getBDMinutes();
    const today = getBDDay();
    if (today !== lastFiredDay) { firedSlots.clear(); lastFiredDay = today; }
    const dayCfg = cfg.days?.[today];
    if (!dayCfg?.enabled || !dayCfg?.folderId?.trim()) return;
    for (const slot of (cfg.slots||[])) {
      if (!slot.enabled) continue;
      const [sh,sm] = slot.time.split(':').map(Number);
      const slotMin = sh*60+sm;
      const slotKey = `${today}_${slot.time}`;
      if (!firedSlots.has(slotKey) && Math.abs(bdMin-slotMin)<=1) {
        firedSlots.add(slotKey);
        log(`[SCHED] ⏰ ${slot.time} | ${today}`);
        runUpload(dayCfg.folderId.trim(), dayCfg.template, cfg.privacy, dayCfg.uploadTarget||cfg.uploadTarget||'youtube')
          .catch(e=>log('[SCHED] Fatal: '+e.message));
        break;
      }
    }
  } catch(e) { console.error('[SCHED]',e.message); }
}, 30000);

// Token refresh every 45 min (tokens expire 60 min)
setInterval(async () => {
  const t = loadTokens();
  if (t.refresh_token) await refreshYTToken().catch(()=>{});
  if (t.drive_refresh_token) await refreshDriveToken().catch(()=>{});
}, 45*60*1000);

// ========== ROUTES ==========
app.get('/health', (req,res) => res.json({status:'ok', time:getBDTime(), uptime:Math.floor(process.uptime())+'s', uploadRunning}));

app.get('/api/status', (req,res) => {
  const t = loadTokens();
  res.json({
    youtube: !!t.access_token,
    drive: !!t.drive_access_token,
    facebook: !!t.fb_page_token,
    channelName: t.channel_name||null,
    fbPageName: t.fb_page_name||null,
    uploadRunning,
    schedulerEnabled: loadConfig().enabled
  });
});

app.get('/api/config', (req,res) => {
  const cfg = loadConfig();
  // Mask API key
  res.json({...cfg, grokApiKey: cfg.grokApiKey ? cfg.grokApiKey.slice(0,8)+'...' : ''});
});
app.get('/api/config/full', (req,res) => res.json(loadConfig()));

app.post('/api/config', (req,res) => {
  try {
    const current = loadConfig();
    const inc = req.body;
    const mergedDays = {...current.days};
    if (inc.days) DAYS.forEach(d => { if(inc.days[d]!==undefined) mergedDays[d]={...defaultDayConfig(),...(current.days[d]||{}),...inc.days[d]}; });
    const updated = {...current, ...inc, days:mergedDays};
    saveConfig(updated);
    res.json({success:true});
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/logs', (req,res) => res.json({logs:logs.slice(0,100)}));

app.post('/api/run-now', async (req,res) => {
  const {folderId,template,privacy} = req.body;
  if (!folderId?.trim()) return res.status(400).json({error:'folderId দাও'});
  if (uploadRunning && (Date.now()-uploadStartTime)<UPLOAD_TIMEOUT_MS) return res.status(409).json({error:'Upload চলছে — লগ দেখো'});
  res.json({success:true, message:'Upload শুরু হচ্ছে...'});
  runUpload(folderId.trim(), template, privacy, req.body.target).catch(()=>{});
});

app.get('/api/queue', (req,res) => {
  const q = loadQueue();
  const s = {};
  Object.keys(q).forEach(k => { s[k]={remaining:(q[k].remaining||[]).length, used:(q[k].used||[]).length}; });
  res.json(s);
});

app.post('/api/queue/reset', (req,res) => {
  const {folderId} = req.body;
  const q = loadQueue();
  if (folderId) { delete q[folderId]; log('[QUEUE] Reset: '+folderId); }
  else { Object.keys(q).forEach(k=>delete q[k]); log('[QUEUE] সব reset'); }
  saveQueue(q);
  res.json({success:true});
});

// Test Drive folder
app.post('/api/test-folder', async (req,res) => {
  try {
    const {folderId} = req.body;
    if (!folderId) return res.status(400).json({error:'folderId দাও'});
    const driveToken = await getValidDriveToken();
    if (!driveToken) return res.status(401).json({error:'Drive connect করো'});
    const videos = await listAllVideos(folderId.trim(), driveToken);
    res.json({success:true, count:videos.length, sample:videos.slice(0,3).map(v=>v.name)});
  } catch(e) { res.status(500).json({error:e.message}); }
});

// ========== OAUTH ==========
app.get('/auth/youtube', (req,res) => {
  const scopes = encodeURIComponent('https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/youtube/callback')}&response_type=code&scope=${scopes}&access_type=offline&prompt=consent`);
});
app.get('/auth/youtube/callback', async (req,res) => {
  const {code,error} = req.query;
  if (error) return res.send(htmlMsg('❌ '+error,'red'));
  if (!code) return res.send(htmlMsg('❌ No code','red'));
  try {
    const fetch = await getFetch();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body: new URLSearchParams({code,client_id:process.env.YT_CLIENT_ID,client_secret:process.env.YT_CLIENT_SECRET,redirect_uri:process.env.BASE_URL+'/auth/youtube/callback',grant_type:'authorization_code'})
    });
    const tokens = await r.json();
    if (!tokens.access_token) throw new Error(JSON.stringify(tokens));
    tokenExpiry.yt = Date.now()+(tokens.expires_in||3600)*1000-300000;
    let ch='';
    try { const c=await (await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true',{headers:{'Authorization':`Bearer ${tokens.access_token}`}})).json(); ch=c.items?.[0]?.snippet?.title||''; } catch{}
    saveTokens({...loadTokens(), access_token:tokens.access_token, refresh_token:tokens.refresh_token, channel_name:ch});
    log('[AUTH] YouTube: '+ch);
    res.send(htmlMsg('✅ YouTube সংযুক্ত!','#06d6a0',ch));
  } catch(e) { res.send(htmlMsg('❌ '+e.message,'red')); }
});

// FIX: drive.file scope — allows creating backup files + reading Drive files
app.get('/auth/drive', (req,res) => {
  const scopes = encodeURIComponent('https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/drive.readonly');
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${process.env.YT_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/drive/callback')}&response_type=code&scope=${scopes}&access_type=offline&prompt=consent`);
});
app.get('/auth/drive/callback', async (req,res) => {
  const {code,error} = req.query;
  if (error) return res.send(htmlMsg('❌ '+error,'red'));
  if (!code) return res.send(htmlMsg('❌ No code','red'));
  try {
    const fetch = await getFetch();
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body: new URLSearchParams({code,client_id:process.env.YT_CLIENT_ID,client_secret:process.env.YT_CLIENT_SECRET,redirect_uri:process.env.BASE_URL+'/auth/drive/callback',grant_type:'authorization_code'})
    });
    const tokens = await r.json();
    if (!tokens.access_token) throw new Error(JSON.stringify(tokens));
    tokenExpiry.drive = Date.now()+(tokens.expires_in||3600)*1000-300000;
    saveTokens({...loadTokens(), drive_access_token:tokens.access_token, drive_refresh_token:tokens.refresh_token});
    log('[AUTH] Drive ✅');
    res.send(htmlMsg('✅ Google Drive সংযুক্ত!','#06d6a0'));
  } catch(e) { res.send(htmlMsg('❌ '+e.message,'red')); }
});


// ========== FACEBOOK OAUTH ==========
app.get('/auth/facebook', (req,res) => {
  if (!process.env.FB_APP_ID) return res.send(htmlMsg('❌ FB_APP_ID env নেই','red'));
  const scopes = 'pages_manage_posts,pages_read_engagement,pages_show_list';
  res.redirect(`https://www.facebook.com/v19.0/dialog/oauth?client_id=${process.env.FB_APP_ID}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/facebook/callback')}&scope=${scopes}&response_type=code`);
});

app.get('/auth/facebook/callback', async (req,res) => {
  const {code, error} = req.query;
  if (error) return res.send(htmlMsg('❌ '+error,'red'));
  if (!code) return res.send(htmlMsg('❌ No code','red'));
  try {
    const fetch = await getFetch();
    // Exchange code for token
    const r = await fetch(`https://graph.facebook.com/v19.0/oauth/access_token?client_id=${process.env.FB_APP_ID}&client_secret=${process.env.FB_APP_SECRET}&redirect_uri=${encodeURIComponent(process.env.BASE_URL+'/auth/facebook/callback')}&code=${code}`);
    const d = await r.json();
    if (!d.access_token) throw new Error(JSON.stringify(d));
    // Exchange for long-lived token
    const llr = await fetch(`https://graph.facebook.com/v19.0/oauth/access_token?grant_type=fb_exchange_token&client_id=${process.env.FB_APP_ID}&client_secret=${process.env.FB_APP_SECRET}&fb_exchange_token=${d.access_token}`);
    const lld = await llr.json();
    const longToken = lld.access_token || d.access_token;
    // Get pages
    const pages = await getFBPageToken(longToken);
    if (!pages.length) throw new Error('কোনো Facebook Page পাওয়া যায়নি। আগে একটা Page বানাও।');
    // Save first page by default (can be changed in UI)
    const page = pages[0];
    saveTokens({...loadTokens(), fb_access_token:longToken, fb_page_id:page.id, fb_page_token:page.access_token, fb_page_name:page.name, fb_all_pages:pages});
    log('[AUTH] Facebook: '+page.name);
    res.send(htmlMsg('✅ Facebook সংযুক্ত!','#1877f2', page.name + (pages.length>1 ? ` (+${pages.length-1} more pages)` : '')));
  } catch(e) { res.send(htmlMsg('❌ '+e.message,'red')); }
});

// Select which FB page to use
app.post('/auth/facebook/select-page', (req,res) => {
  try {
    const {pageId} = req.body;
    const t = loadTokens();
    const pages = t.fb_all_pages || [];
    const page = pages.find(p=>p.id===pageId);
    if (!page) return res.status(404).json({error:'Page পাওয়া যায়নি'});
    saveTokens({...t, fb_page_id:page.id, fb_page_token:page.access_token, fb_page_name:page.name});
    log('[AUTH] FB Page selected: '+page.name);
    res.json({success:true, pageName:page.name});
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/fb/pages', (req,res) => {
  const t = loadTokens();
  res.json({pages: t.fb_all_pages||[], selectedId: t.fb_page_id||null, selectedName: t.fb_page_name||null});
});

// ========== STARTUP ==========
async function startup() {
  log('🚀 Auto Waz 2.0 চালু হচ্ছে...');
  try {
    await restoreTokensFromDrive();
    const t = loadTokens();
    if (t.refresh_token) await refreshYTToken().catch(()=>{});
    if (t.drive_refresh_token) await refreshDriveToken().catch(()=>{});
    await restoreConfigFromDrive();
    await restoreQueueFromDrive();
    log('✅ সব restore সম্পন্ন');
  } catch(e) { log('⚠️ Startup: '+e.message); }
}
startup();
app.listen(PORT, () => console.log(`Auto Waz 2.0 port ${PORT}`));
