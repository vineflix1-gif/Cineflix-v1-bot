require('dotenv').config();
const dns = require('dns');

// Override local Windows DNS SRV lookup issues for MongoDB Atlas
try {
  dns.setServers(['8.8.8.8', '1.1.1.1']);
} catch (dnsErr) {
  console.log('DNS setServers note:', dnsErr.message);
}

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  initAuthCreds,
  BufferJSON,
  proto
} = require('@whiskeysockets/baileys');
const mongoose = require('mongoose');
const pino = require('pino');
const QRCode = require('qrcode');
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const config = require('./config.json');

// Express App Setup
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Global state variables
let sock = null;
let isConnected = false;
let latestQRImage = null;
let dbStatus = 'Firebase Realtime DB (Live Sync)';
let isReconnectScheduled = false; // prevents duplicate startBot() calls stacking up multiple live sockets (was causing every reply, incl. .ping, to be sent twice)

const FIREBASE_DB_URL = 'https://cineflix1-1fdc6-default-rtdb.firebaseio.com';
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Mongoose Schema for Baileys WhatsApp Session Persistence in MongoDB Atlas (Heroku Safe)
const baileysAuthSchema = new mongoose.Schema({
  _id: { type: String, required: true },
  data: { type: Object, required: true }
}, { timestamps: true, collection: 'baileys_auth' });

const BaileysAuth = mongoose.models.BaileysAuth || mongoose.model('BaileysAuth', baileysAuthSchema);

async function useMongoDBAuthState() {
  const readData = async (file) => {
    try {
      const doc = await BaileysAuth.findById(file).lean();
      if (doc && doc.data) {
        return JSON.parse(JSON.stringify(doc.data), BufferJSON.reviver);
      }
    } catch (e) {
      console.error(`MongoDB read note [${file}]:`, e.message);
    }
    return null;
  };

  const writeData = async (file, data) => {
    try {
      const serialized = JSON.parse(JSON.stringify(data, BufferJSON.replacer));
      await BaileysAuth.findByIdAndUpdate(
        file,
        { _id: file, data: serialized },
        { upsert: true, new: true }
      );
    } catch (e) {
      console.error(`MongoDB write note [${file}]:`, e.message);
    }
  };

  const removeData = async (file) => {
    try {
      await BaileysAuth.findByIdAndDelete(file);
    } catch (e) {
      console.error(`MongoDB remove note [${file}]:`, e.message);
    }
  };

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const file = `${category}-${id}`;
              if (value) {
                tasks.push(writeData(file, value));
              } else {
                tasks.push(removeData(file));
              }
            }
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: async () => {
      await writeData('creds', creds);
    }
  };
}

// Helper function to load movies from Firebase RTDB with fallbacks
// ── Movie cache (refresh every 10 mins) ──────────────────────────────────────
let _moviesCache = null;
let _moviesCacheTime = 0;
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes

async function loadMovies() {
  // Return cache if fresh
  if (_moviesCache && (Date.now() - _moviesCacheTime) < CACHE_TTL) {
    return _moviesCache;
  }

  try {
    const res = await fetch(`${FIREBASE_DB_URL}/movies.json`);
    if (res.ok) {
      const data = await res.json();
      if (data && typeof data === 'object') {
        const arr = Object.entries(data).map(([key, val]) => ({
          id: val.id || key,
          ...val
        })).filter(Boolean);
        if (arr.length > 0) {
          _moviesCache = arr;
          _moviesCacheTime = Date.now();
          console.log(`🎬 Movie cache updated: ${arr.length} movies`);
          return arr;
        }
      }
    }
  } catch (rtdbErr) {
    console.warn('Firebase RTDB fetch note:', rtdbErr.message);
  }

  // Return stale cache if fetch failed
  if (_moviesCache) {
    console.warn('Using stale movie cache');
    return _moviesCache;
  }

  // Fallback to local JSON
  try {
    const moviesPath = path.resolve(__dirname, config.moviesJsonPath || '../src/data/initialMovies.json');
    if (fs.existsSync(moviesPath)) {
      const data = fs.readFileSync(moviesPath, 'utf8');
      const parsed = JSON.parse(data);
      _moviesCache = parsed;
      _moviesCacheTime = Date.now();
      return parsed;
    }
  } catch (err) {
    console.error('Error loading local JSON movies:', err.message);
  }
  return [];
}

// ── Fuzzy title match with typo tolerance ────────────────────────────────────
function normalize(str) {
  return str.toLowerCase()
    .replace(/[:\-_'"!?.]/g, ' ')  // punctuation → space
    .replace(/\s+/g, ' ')
    .trim();
}

// Simple char-level similarity (0–1)
function similarity(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const longer = a.length > b.length ? a : b;
  const shorter = a.length > b.length ? b : a;
  if (longer.length === 0) return 1;
  let matches = 0;
  for (let i = 0; i < shorter.length; i++) {
    if (longer.includes(shorter[i])) matches++;
  }
  return matches / longer.length;
}

function fuzzyFindMovie(movies, query) {
  const q = normalize(query);
  const qWords = q.split(' ').filter(w => w.length > 1);

  let bestMovie = null;
  let bestScore = 0;

  for (const m of movies) {
    const title = normalize(m.title || '');
    const titleWords = title.split(' ');

    // Exact include check
    if (title.includes(q) || q.includes(title)) {
      return m; // instant match
    }

    // Word overlap score
    let wordMatches = 0;
    for (const qw of qWords) {
      for (const tw of titleWords) {
        const sim = similarity(qw, tw);
        if (sim >= 0.75) { wordMatches++; break; }
      }
    }

    const score = qWords.length > 0 ? wordMatches / qWords.length : 0;
    if (score > bestScore) {
      bestScore = score;
      bestMovie = m;
    }
  }

  // Only return if at least 60% words matched
  return bestScore >= 0.6 ? bestMovie : null;
}

async function startBot() {
  isReconnectScheduled = false; // this call is happening now, so future close events can schedule again

  // Tear down any previous socket fully before creating a new one — leaving old
  // listeners attached is what was causing every message (incl. .ping) to be handled twice.
  if (sock) {
    try { sock.ev.removeAllListeners(); } catch (e) {}
    try { sock.end(new Error('restarting')); } catch (e) {}
    sock = null;
  }

  const logger = pino({ level: 'silent' });
  const { version } = await fetchLatestBaileysVersion();

  let authState;
  const DEFAULT_MONGO_URI = 'mongodb+srv://ccransika_db_user:Pc1u7xrzGEn4LJvw@cluster0.sntej6n.mongodb.net/cineflix_bot?retryWrites=true&w=majority';
  const mongoUri = process.env.MONGODB_URI || config.mongodbUri || DEFAULT_MONGO_URI;

  if (mongoUri && mongoUri.trim() !== '' && !mongoUri.includes('YOUR_MONGODB_ATLAS_URI')) {
    try {
      if (mongoose.connection.readyState !== 1) {
        await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 5000 });
        console.log('🍃 Connected to MongoDB Atlas for WhatsApp Session Persistence!');
      }
      authState = await useMongoDBAuthState();
      dbStatus = 'MongoDB Atlas Session Storage + Firebase RTDB Movies';
    } catch (mongoErr) {
      console.warn('⚠️ MongoDB connection failed, falling back to local files:', mongoErr.message);
      const authFolder = path.join(__dirname, 'auth_info_baileys');
      authState = await useMultiFileAuthState(authFolder);
    }
  } else {
    console.log('📁 Using local auth_info_baileys folder for session storage.');
    const authFolder = path.join(__dirname, 'auth_info_baileys');
    authState = await useMultiFileAuthState(authFolder);
  }

  const { state, saveCreds } = authState;

  console.log(`\n==================================================`);
  console.log(` 🎬 CINEFLIX WHATSAPP BOT (Live Firebase RTDB & MongoDB Auth) `);
  console.log(` Baileys Version: ${version.join('.')}`);
  console.log(` Database Mode: ${dbStatus}`);
  console.log(`==================================================\n`);

  sock = makeWASocket({
    version,
    logger,
    printQRInTerminal: false,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    generateHighQualityLinkPreview: true,
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      try {
        latestQRImage = await QRCode.toDataURL(qr);
        console.log('⚡ New Web QR Code generated! View live at http://localhost:3001');
      } catch (qrErr) {
        console.error('Error generating web QR image:', qrErr);
      }
    }

    if (connection === 'close') {
      isConnected = false;
      latestQRImage = null;
      const statusCode = lastDisconnect?.error?.output?.statusCode;

      // Code 440 = CONNECTION_REPLACED (same account logged in elsewhere)
      // Code 401/403/405/loggedOut = session invalid
      const isLoggedOut   = statusCode === DisconnectReason.loggedOut || statusCode === 401 || statusCode === 403 || statusCode === 405;
      const isReplaced    = statusCode === 440; // Another device took over
      const shouldReconnect = !isLoggedOut;

      console.log(`⚠️ Connection closed (code ${statusCode}), reconnecting: ${shouldReconnect}`);

      if (isLoggedOut || isReplaced) {
        console.log('🧹 Clearing session (code ' + statusCode + ') to force fresh QR...');
        try {
          if (mongoose.connection.readyState === 1) {
            await BaileysAuth.deleteMany({});
            console.log('🗑️  MongoDB session cleared.');
          }
          const authFolder = path.join(__dirname, 'auth_info_baileys');
          if (fs.existsSync(authFolder)) {
            fs.rmSync(authFolder, { recursive: true, force: true });
          }
        } catch (clearErr) {
          console.error('Error clearing session:', clearErr.message);
        }

        if (isReplaced) {
          // Wait longer before reconnect to avoid immediate conflict loop
          console.log('⏳ Waiting 15s before reconnect (CONNECTION_REPLACED)...');
          if (!isReconnectScheduled) {
            isReconnectScheduled = true;
            setTimeout(startBot, 15000);
          }
          return;
        }
      }

      if (shouldReconnect) {
        if (!isReconnectScheduled) {
          isReconnectScheduled = true;
          setTimeout(startBot, 3000);
        }
      } else {
        console.log('🔴 Logged out permanently. Please restart and scan QR again.');
      }
    } else if (connection === 'open') {
      isConnected = true;
      latestQRImage = null;
      console.log('✅ WhatsApp Bot Connected Successfully!');
      // Pre-load movie cache so first .movie command is instant
      loadMovies().then(m => console.log(`🎬 Pre-loaded ${m.length} movies into cache`)).catch(()=>{});

      // Send connect notification to owner
      const ownerNumber = process.env.OWNER_NUMBER || config.ownerNumber;
      if(ownerNumber){
        try{
          const ownerJid = ownerNumber.replace(/[^0-9]/g,'') + '@s.whatsapp.net';
          const now = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Colombo', hour12: true });
          const connectMsg =
            `🟢 *Cineflix Bot Connected!* ✅\n\n` +
            `🤖 *Bot:* Cineflix WhatsApp Bot\n` +
            `⏰ *Time:* ${now} (SL)\n` +
            `🌐 *Website:* ${process.env.WEBSITE_URL || config.websiteUrl}\n` +
            `📦 *DB:* MongoDB Atlas + Firebase RTDB\n\n` +
            `Bot successfully online and ready! 🚀`;
          await sock.sendMessage(ownerJid, { text: connectMsg });
          console.log(`📲 Connect notification sent to owner: ${ownerNumber}`);
        } catch(e){
          console.warn('Could not send owner connect msg:', e.message);
        }
      }
    }
  });

  sock.ev.on('messages.upsert', async (m) => {
    try {
      const msg = m.messages[0];
      if (!msg || !msg.message || msg.key.fromMe) return;

      const from = msg.key.remoteJid;
      const text =
        msg.message.conversation ||
        msg.message.extendedTextMessage?.text ||
        msg.message.imageMessage?.caption ||
        '';

      const cleanText = text.trim();
      if (!cleanText) return;

      // Ignore standard casual conversational words & punctuation so bot does not reply to normal chat
      // Only block truly irrelevant single words
      const isCasual = /^(na|ha|ok|okay|tnx|gm|gn|yes|no|\?|\!)$/i.test(cleanText);
      if (isCasual) return;
      // Allow hi/hello → show welcome
      if (/^(hi|hello|hey|start)$/i.test(cleanText)) {
        await sock.sendMessage(from, { text: config.messages.welcome }, { quoted: msg });
        return;
      }

      // Only respond if it's a website request or explicit bot command (.movie, .dl, .get, Movie Code:)
      const isWebsiteReq = cleanText.includes('Movie Code:') || cleanText.includes('Hi Cineflix!') || cleanText.includes('via WhatsApp');
      const isCmd = cleanText.startsWith('.movie') || cleanText.startsWith('.dl') || cleanText.startsWith('.get') || cleanText.startsWith('.find') || cleanText.startsWith('.search') || cleanText.startsWith('.cinesubz') || cleanText.startsWith('.cinetv') || cleanText === '.info' || cleanText === '.ping';

      if (!isWebsiteReq && !isCmd) {
        return; // Ignore normal chat messages!
      }

      console.log(`📩 Received Valid WhatsApp Bot Command from [${from}]: "${cleanText}"`);

      if (cleanText.toLowerCase() === '.ping') {
        await sock.sendMessage(from, { text: '🤖 *Cineflix Bot Online!* ✅\n\n⚡ Response: 100ms\n🌐 ' + (process.env.WEBSITE_URL || config.websiteUrl) }, { quoted: msg });
        return;
      }
      if (cleanText.toLowerCase() === '.info' || cleanText.toLowerCase() === '.start' || cleanText.toLowerCase() === 'hi') {
        await sock.sendMessage(from, { text: config.messages.welcome }, { quoted: msg });
        return;
      }

      // .cinesubz / .cinetv — search & download from the external Cinesubz catalog
      if (cleanText.toLowerCase().startsWith('.cinesubz') || cleanText.toLowerCase().startsWith('.cinetv')) {
        const query = cleanText.replace(/^\.(cinesubz|cinetv)\s*/i, '').trim();
        await handleCinesubzRequest(sock, from, query, msg);
        return;
      }

      // Process movie download request
      await handleMovieDownloadRequest(sock, from, cleanText, msg);
    } catch (err) {
      console.error('Error handling message:', err);
    }
  });
}

// Concurrency lock flag to prevent duplicate parallel worker ticks
let isQueueWorkerRunning = false;
let targetGroupJid = config.targetGroupJid || '120363411804070695@g.us';
const GROUP_INVITE_CODE = config.groupInviteCode || 'E81OYFeX3nW9meDUAt9a7B';

async function getOrJoinTargetGroup() {
  if (!sock || !isConnected) return targetGroupJid;
  try {
    if (!targetGroupJid) {
      try {
        const info = await sock.groupGetInviteInfo(GROUP_INVITE_CODE);
        if (info && info.id) targetGroupJid = info.id;
      } catch (e) {}

      try {
        const joinedId = await sock.groupAcceptInvite(GROUP_INVITE_CODE);
        if (joinedId) targetGroupJid = joinedId;
      } catch (e) {}
    }
  } catch (err) {
    console.log('Group join info note:', err.message);
  }
  return targetGroupJid || '120363411804070695@g.us';
}

// Automatic Bot Queue Worker for Website Direct Phone Delivery matching Screenshots
async function processFirebaseBotQueue() {
  if (!sock || !isConnected || isQueueWorkerRunning) return;

  isQueueWorkerRunning = true;
  try {
    const res = await fetch(`${FIREBASE_DB_URL}/bot_requests.json`);
    if (!res.ok) return;

    const data = await res.json();
    if (!data || typeof data !== 'object') return;

    // Filter pending requests in queue and sort chronologically by submission time
    const pendingRequests = Object.entries(data)
      .map(([key, val]) => ({ reqId: key, ...val }))
      .filter(r => r.status === 'pending')
      .sort((a, b) => (a.time || 0) - (b.time || 0));

    // Resolve Target WhatsApp Group JID
    const groupJid = await getOrJoinTargetGroup();

    for (let i = 0; i < pendingRequests.length; i++) {
      const req = pendingRequests[i];
      const userJid = `${req.phone}@s.whatsapp.net`;
      const userName = req.userName || 'Requester';
      const userTag = `@${req.phone}`; // Real WhatsApp mention format
      const movieTitle = req.movieTitle || 'Movie';
      const movieYear = req.movieYear ? `(${req.movieYear})` : '';
      const qualityText = req.quality || '720p';

      // Target JID: Try sending to Group first, fallback to User DM if Group fails/restricted
      let targetSendJid = groupJid || userJid;

      // Calculate actual position in active queue (1st, 2nd, 3rd, etc.)
      const queuePosition = i + 1;

      console.log(`🚀 Processing Direct WhatsApp Request #${queuePosition} for [${userName}] (${req.phone}) -> Target: ${targetSendJid}`);

      // Mark request as processing immediately in Firebase RTDB
      await fetch(`${FIREBASE_DB_URL}/bot_requests/${req.reqId}/status.json`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify('processing')
      });

      // Step 1: Queued Notice with real blue WhatsApp mention
      const queuedMsg = (config.messages.queued || `හලෝ! ⏳ *Request Queued!*\n\n${userTag} ඔයා ඉල්ලූ *{title}* movie එක queue ලෙ!`)
        .replace('{mention}', userTag)
        .replace('{title}', `${movieTitle} ${movieYear}`)
        .replace('{quality}', qualityText);
      try {
        await sock.sendMessage(targetSendJid, { text: queuedMsg, mentions: [userJid] });
      } catch (grpErr) {
        console.warn(`⚠️ Could not send queued notice to Group ${targetSendJid}, falling back to DM: ${grpErr.message}`);
        targetSendJid = userJid;
        await sock.sendMessage(targetSendJid, { text: queuedMsg, mentions: [userJid] }).catch(() => {});
      }
      await delay(2000);

      // Fetch movie download details from database
      const movies = await loadMovies();
      const targetMovie = movies.find(m => m.id === req.movieId || (m.title && m.title.toLowerCase().includes(movieTitle.toLowerCase())));
      
      // Match requested quality fuzzily & case-insensitively (e.g. 720p, 720P, HD 720p)
      const cleanQuality = (qualityText || '720p').toLowerCase().replace(/[^a-z0-9]/g, '');
      const targetDownload = targetMovie?.downloads?.find(d => {
        const dRes = (d.res || d.quality || '').toLowerCase().replace(/[^a-z0-9]/g, '');
        return dRes.includes(cleanQuality) || cleanQuality.includes(dRes);
      }) || targetMovie?.downloads?.[0];

      const isTvSeries = targetMovie?.type === 'tv' || (targetMovie?.seasons && targetMovie?.seasons.length > 0);
      const episodesList = [];

      if (isTvSeries && targetMovie?.seasons) {
        targetMovie.seasons.forEach((season, sIdx) => {
          (season.episodes || []).forEach((ep, eIdx) => {
            episodesList.push({
              seasonNum: season.season || (sIdx + 1),
              epNum: ep.ep || ep.episodeNumber || (eIdx + 1),
              title: ep.title || `Episode ${ep.ep || (eIdx + 1)}`,
              srv1: ep.srv1 || ep.url || ep.srv2 || targetDownload?.srv1 || targetMovie?.downloads?.[0]?.srv1,
              srv2: ep.srv2 || ep.srv1 || targetDownload?.srv2,
              pd720: ep.pd720,
              pd1080: ep.pd1080,
              subUrl: ep.subUrl || targetMovie.subUrl
            });
          });
        });
      }

      // If it's a TV Series with episodes, deliver ALL episodes sequentially!
      if (isTvSeries && episodesList.length > 0) {
        console.log(`📺 TV Series detected [${movieTitle}] with ${episodesList.length} episodes! Delivering all episodes to ${targetSendJid}...`);
        
        const startTvNotice = `📺 *${movieTitle} ${movieYear} (TV Series)*\n👤 *Requested by:* ${userTag}\n📦 *Total Episodes:* ${episodesList.length} Episodes\n\nසියලුම Episodes ඔබගේ WhatsApp වෙත උඩුගත කිරීම ආරම්භ විය. 🚀`;
        await sock.sendMessage(targetSendJid, { text: startTvNotice, mentions: [userJid] }).catch(() => {});
        await delay(2000);

        for (let epIdx = 0; epIdx < episodesList.length; epIdx++) {
          const ep = episodesList[epIdx];
          const epTitle = `${movieTitle} - S${ep.seasonNum}E${ep.epNum} (${ep.title})`;
          const epUrl = ep.srv1 || ep.srv2 || targetMovie?.subUrl;
          const fileSize = targetDownload?.size || '250 MB';

          let epDocSent = false;
          const epCaption = `📺 *${epTitle}*\n👤 *Requested by:* ${userTag}\n📦 *Size:* ${fileSize}\n⚡ *Episode:* ${epIdx + 1} of ${episodesList.length}\n\n🌐 *Downloaded via:* ${process.env.WEBSITE_URL || config.websiteUrl}\n© *POWERED BY CINEFLIX*`;

          if (epUrl && epUrl !== '#' && epUrl.startsWith('http')) {
            const isMkv = epUrl.toLowerCase().includes('.mkv') || epUrl.endsWith('.mkv');
            const fileName = `${epTitle.replace(/[^a-zA-Z0-9]/g, '_')}_${qualityText}.${isMkv ? 'mkv' : 'mp4'}`;

            try {
              console.log(`📡 Sending TV Episode [${epIdx + 1}/${episodesList.length}] ${fileName} to ${targetSendJid}...`);
              await sock.sendMessage(
                targetSendJid,
                {
                  document: { url: epUrl },
                  mimetype: isMkv ? 'video/x-matroska' : 'video/mp4',
                  fileName: fileName,
                  caption: epCaption,
                  mentions: [userJid]
                }
              );
              epDocSent = true;
            } catch (epErr) {
              console.error(`⚠️ Failed primary upload for TV Episode ${fileName}:`, epErr.message);
              try {
                await delay(2000);
                await sock.sendMessage(
                  userJid,
                  {
                    document: { url: epUrl },
                    mimetype: isMkv ? 'video/x-matroska' : 'video/mp4',
                    fileName: fileName,
                    caption: epCaption,
                    mentions: [userJid]
                  }
                );
                epDocSent = true;
              } catch (rErr) {
                console.error(`❌ Retry failed for TV Episode ${fileName}:`, rErr.message);
              }
            }
          }

          if (!epDocSent) {
            const linkNotice = `📺 *${epTitle}*\n👤 *Requested by:* ${userTag}\n📦 *Episode:* ${epIdx + 1} / ${episodesList.length}\n\n📥 *Direct Episode Download Link:*\n🔗 ${epUrl || 'https://t.me/Cineflix_cloud_Bot'}\n\n🍿 *Watch Online:* ${process.env.WEBSITE_URL || config.websiteUrl}`;
            await sock.sendMessage(targetSendJid, { text: linkNotice, mentions: [userJid] });
          }

          await delay(2500); // 2.5s delay between episodes
        }

      } else {
        // Single Movie Delivery
        const downloadUrl = targetDownload?.srv1 || targetDownload?.srv2 || targetDownload?.url || targetMovie?.subUrl;
        const fileSize = targetDownload?.size || '216 MB';

        let documentSent = false;
        const captionText = `🎬 *${movieTitle} ${movieYear}*\n\n👤 *Requested by:* ${userTag}\n📦 *Size:* ${fileSize}\n⚡ *Quality:* ${qualityText}\n\n🌐 *Downloaded via:* ${process.env.WEBSITE_URL || config.websiteUrl}\n© *POWERED BY CINEFLIX*`;

        if (downloadUrl && downloadUrl !== '#' && downloadUrl.startsWith('http')) {
          const isMkv = downloadUrl.toLowerCase().includes('.mkv') || downloadUrl.endsWith('.mkv');
          const fileName = `${movieTitle.replace(/[^a-zA-Z0-9]/g, '_')}_${qualityText}.${isMkv ? 'mkv' : 'mp4'}`;

          try {
            console.log(`📡 Sending document file [${fileName}] to ${targetSendJid} from: ${downloadUrl}`);
            await sock.sendMessage(
              targetSendJid,
              {
                document: { url: downloadUrl },
                mimetype: isMkv ? 'video/x-matroska' : 'video/mp4',
                fileName: fileName,
                caption: captionText,
                mentions: [userJid]
              }
            );
            documentSent = true;
            console.log(`✅ Document file [${fileName}] delivered successfully to ${targetSendJid}`);
          } catch (docErr) {
            console.error('⚠️ Primary document upload failed:', docErr.message);
            try {
              await delay(2000);
              await sock.sendMessage(
                userJid,
                {
                  document: { url: downloadUrl },
                  mimetype: isMkv ? 'video/x-matroska' : 'video/mp4',
                  fileName: fileName,
                  caption: captionText,
                  mentions: [userJid]
                }
              );
              documentSent = true;
              console.log(`✅ Document file delivered on DM fallback to ${userJid}`);
            } catch (retryErr) {
              console.error('❌ Retry document upload failed:', retryErr.message);
            }
          }
        }

        if (!documentSent) {
          const linkNotice = `🎬 *${movieTitle} ${movieYear} (${qualityText})*\n👤 *Requested by:* ${userTag}\n📦 *Size:* ${fileSize}\n🗣 *Subtitle:* Cineflix Sinhala Subtitles (${process.env.WEBSITE_URL || config.websiteUrl})\n\n📥 *Direct High-Speed Server Download Link:*\n🔗 ${downloadUrl || targetDownload?.tgLink || 'https://t.me/Cineflix_cloud_Bot'}\n\n🍿 *Watch Online & Web Download:* ${process.env.WEBSITE_URL || config.websiteUrl}`;
          await sock.sendMessage(targetSendJid, { text: linkNotice, mentions: [userJid] });
        }
      }
      await delay(1500);

      // Step 3: Send Thank You & Subtitle Player Instructions right after file delivery with real blue mention
      const thankYouMsg = (config.messages.thankYou || `✅ *ආයුබෝවන් ${userTag}!* ස්තූතියි! 🎬`)
        .replace('{mention}', userTag)
        .replace('{title}', movieTitle);
      await sock.sendMessage(targetSendJid, { text: thankYouMsg, mentions: [userJid] }).catch(() => {});

      // Delete processed request from Firebase RTDB to keep Database clean & lightweight
      await fetch(`${FIREBASE_DB_URL}/bot_requests/${req.reqId}.json`, {
        method: 'DELETE'
      });
      console.log(`🧹 Cleaned up request [${req.reqId}] from Firebase Database.`);
    }
  } catch (queueErr) {
    console.error('Queue worker error:', queueErr.message);
  } finally {
    isQueueWorkerRunning = false;
  }
}

// Run queue worker every 6 seconds
setInterval(processFirebaseBotQueue, 6000);

// ── Cinesubz external search (Chama Movie API) ──────────────────────────────
// Lets .cinesubz / .cinetv pull movies & TV series that aren't on your own
// site catalog yet, complete with Sinhala-subbed direct download links.
const CINESUBZ_API_BASE = 'https://chama-movie-api.koyeb.app';
const CINESUBZ_API_KEY  = 'chama_api_584829ed7f1b24120633e2c272f9b98d';
const CINESUBZ_DEFAULT_IMAGE = `${CINESUBZ_API_BASE}/logo.png`;
const CINESUBZ_FOOTER = `\n\n🎬 *Cineflix Bot* — ${config.websiteUrl}`;

async function handleCinesubzRequest(sock, from, query, msg) {
  if (!query || !query.trim()) {
    await sock.sendMessage(from, {
      text: `*❪ ERROR ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*\n• .cinesubz spider man\n• .cinetv game of thrones\n\n📝 _Please provide the Movie or TV Series name!_${CINESUBZ_FOOTER}`
    }, { quoted: msg });
    return;
  }

  await sock.sendMessage(from, {
    text: `*❪ SEARCHING ❫*\n\n🔍 *Searching Cinesubz...*\n⚡ _Please wait a moment._`
  }, { quoted: msg });

  try {
    const searchResponse = await axios.get(`${CINESUBZ_API_BASE}/api/v1/movie/cinesubz/search`, {
      params: { q: query, api_key: CINESUBZ_API_KEY },
      timeout: 15000
    });
    const searchData = searchResponse.data;

    if (!searchData.status || !searchData.data || searchData.data.length === 0) {
      await sock.sendMessage(from, {
        text: `*❪ NO RESULTS ❫*\n\n😞 *No Results Found!*\n\n🎬 *Query:* _${query}_\n💡 *Tip:* _Please check the spelling and try again!_${CINESUBZ_FOOTER}`
      }, { quoted: msg });
      return;
    }

    const results = searchData.data.slice(0, 25);
    let listText = `*❪ SEARCH RESULTS ❫*\n\n🎯 *Query:* _${query}_\n📊 *Results:* _${results.length} Items_\n\n*👇 REPLY WITH A NUMBER 👇*\n\n`;
    results.forEach((item, index) => {
      const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
      const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
      listText += `*${num}* ➜ ${typeIcon} _${item.title.substring(0, 40)}_\n`;
    });
    listText += CINESUBZ_FOOTER;

    const sentMsg = await sock.sendMessage(from, { text: listText }, { quoted: msg });
    const searchMsgId = sentMsg.key.id;

    const handleSelection = async ({ messages: replyMessages }) => {
      const replyMsg = replyMessages[0];
      if (!replyMsg?.message) return;

      const replyText = replyMsg.message.conversation || replyMsg.message.extendedTextMessage?.text;
      const isReplyToSearch = replyMsg.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgId;
      if (!isReplyToSearch || from !== replyMsg.key.remoteJid) return;

      const choice = parseInt(replyText) - 1;
      if (isNaN(choice) || choice < 0 || choice >= results.length) {
        await sock.sendMessage(from, {
          text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${results.length}_${CINESUBZ_FOOTER}`
        }, { quoted: replyMsg });
        return;
      }

      sock.ev.off('messages.upsert', handleSelection);
      const selected = results[choice];

      if (selected.type === 'tvshows') {
        await handleCinesubzTvSelection(sock, from, selected, replyMsg);
      } else {
        await handleCinesubzMovieSelection(sock, from, selected, replyMsg);
      }
    };

    sock.ev.on('messages.upsert', handleSelection);

  } catch (err) {
    console.error('Cinesubz search error:', err.message);
    await sock.sendMessage(from, {
      text: `*❪ SYSTEM ERROR ❫*\n\n❌ *Something went wrong!*\n🚫 _${err.message}_\n\n🔄 _Please try again later._${CINESUBZ_FOOTER}`
    }, { quoted: msg });
  }
}

async function handleCinesubzTvSelection(sock, from, selectedItem, quotedMsg) {
  await sock.sendMessage(from, { text: `*❪ FETCHING ❫*\n\n📺 *Fetching TV Series...*\n⚡ _Please wait..._` }, { quoted: quotedMsg });

  try {
    const res = await axios.get(`${CINESUBZ_API_BASE}/api/v1/movie/cinesubz/tv/info`, {
      params: { q: selectedItem.link, api_key: CINESUBZ_API_KEY },
      timeout: 20000
    });
    const tvInfo = res.data?.data;
    if (!res.data?.status || !tvInfo) throw new Error('Failed to fetch TV show details');

    const detailsText =
      `*❪ TV SERIES DETAILS ❫*\n\n📺 *${tvInfo.title}*\n` +
      `⭐ *IMDb:* ${tvInfo.rating || 'N/A'}\n` +
      `📅 *Year:* ${tvInfo.year || 'N/A'}\n` +
      `⏳ *Runtime:* ${tvInfo.duration || 'N/A'}\n` +
      `🌍 *Country:* ${tvInfo.country || 'N/A'}\n` +
      `🎭 *Genres:* ${tvInfo.genres ? tvInfo.genres.join(', ') : 'N/A'}\n` +
      `📝 ${tvInfo.story ? (tvInfo.story.length > 250 ? tvInfo.story.substring(0, 250) + '...' : tvInfo.story) : 'N/A'}` +
      CINESUBZ_FOOTER;

    await sock.sendMessage(from, {
      image: { url: tvInfo.image || selectedItem.image || CINESUBZ_DEFAULT_IMAGE },
      caption: detailsText
    }, { quoted: quotedMsg });

    if (!tvInfo.episodes || tvInfo.episodes.length === 0) {
      await sock.sendMessage(from, { text: `*❪ NO EPISODES ❫*\n\n⚠️ _No episodes found for this series._${CINESUBZ_FOOTER}` }, { quoted: quotedMsg });
      return;
    }

    await sock.sendMessage(from, {
      text: `*❪ DOWNLOADING EPISODES ❫*\n\n📺 *Series:* _${tvInfo.title}_\n🎬 *Episodes:* _${tvInfo.episodes.length}_\n⚡ _This may take a while..._${CINESUBZ_FOOTER}`
    }, { quoted: quotedMsg });

    let successCount = 0, failCount = 0;
    for (let i = 0; i < tvInfo.episodes.length; i++) {
      const episode = tvInfo.episodes[i];
      try {
        const epRes = await axios.get(`${CINESUBZ_API_BASE}/api/v1/movie/cinesubz/tv/dl`, {
          params: { q: episode.episode_url, api_key: CINESUBZ_API_KEY },
          timeout: 20000
        });
        const epLinks = epRes.data?.data;

        if (epRes.data?.status && epLinks && epLinks.length > 0) {
          const nonTelegram = epLinks.filter(l => l.link && !l.link.includes('t.me') && !l.link.includes('telegram'));
          const finalLink = (nonTelegram[0] || epLinks[0]).link;

          await sock.sendMessage(from, {
            document: { url: finalLink },
            mimetype: 'video/mp4',
            fileName: `${tvInfo.title} - ${episode.episode_name}.mp4`,
            caption: `📺 *${tvInfo.title}*\n📌 *Episode:* ${episode.episode_name}${CINESUBZ_FOOTER}`
          }, { quoted: quotedMsg });
          successCount++;
        } else {
          failCount++;
        }
        await delay(2500);
      } catch (epErr) {
        console.error('Cinesubz episode download error:', epErr.message);
        failCount++;
      }
    }

    await sock.sendMessage(from, {
      text: `*❪ SUMMARY ❫*\n\n🎉 *Download Complete!*\n✅ *Success:* _${successCount}_\n❌ *Failed:* _${failCount}_${CINESUBZ_FOOTER}`
    }, { quoted: quotedMsg });

  } catch (err) {
    console.error('Cinesubz TV error:', err.message);
    await sock.sendMessage(from, {
      text: `*❪ ERROR ❫*\n\n❌ *TV Details Error!*\n🚫 _${err.message}_${CINESUBZ_FOOTER}`
    }, { quoted: quotedMsg });
  }
}

async function handleCinesubzMovieSelection(sock, from, selectedItem, quotedMsg) {
  await sock.sendMessage(from, { text: `*❪ FETCHING ❫*\n\n🎬 *Fetching Movie...*\n⚡ _Please wait..._` }, { quoted: quotedMsg });

  try {
    const res = await axios.get(`${CINESUBZ_API_BASE}/api/v1/movie/cinesubz/infodl`, {
      params: { q: selectedItem.link, api_key: CINESUBZ_API_KEY },
      timeout: 20000
    });
    const movieInfo = res.data?.data;
    if (!res.data?.status || !movieInfo) throw new Error('Failed to fetch movie details');

    const downloads = movieInfo.downloads || [];
    if (downloads.length === 0) {
      await sock.sendMessage(from, { text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ _No downloads available for this movie._${CINESUBZ_FOOTER}` }, { quoted: quotedMsg });
      return;
    }

    const detailsText =
      `*❪ MOVIE DETAILS ❫*\n\n🎬 *${movieInfo.title}*\n` +
      `⭐ *IMDb:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n` +
      `📅 *Year:* ${movieInfo.year || 'N/A'}\n` +
      `⏳ *Duration:* ${movieInfo.duration || 'N/A'}\n` +
      `🌍 *Country:* ${movieInfo.country || 'N/A'}\n` +
      `🎭 *Genres:* ${movieInfo.genres ? movieInfo.genres.join(', ') : 'N/A'}\n` +
      `📝 ${movieInfo.story ? (movieInfo.story.length > 250 ? movieInfo.story.substring(0, 250) + '...' : movieInfo.story) : 'N/A'}` +
      CINESUBZ_FOOTER;

    await sock.sendMessage(from, {
      image: { url: movieInfo.image || selectedItem.image || CINESUBZ_DEFAULT_IMAGE },
      caption: detailsText
    }, { quoted: quotedMsg });

    const qualityText = `*❪ DOWNLOADS ❫*\n\n📥 *Select Quality:*\n\n${downloads.map((dl, i) => {
      const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
      const icon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
      return `*${num}* ➜ ${icon} _${dl.quality}_ 💾 _${dl.size || 'N/A'}_`;
    }).join('\n')}\n\n*💬 REPLY WITH A NUMBER TO DOWNLOAD 💬*${CINESUBZ_FOOTER}`;

    const qualityMsg = await sock.sendMessage(from, { text: qualityText }, { quoted: quotedMsg });
    const qualityMsgId = qualityMsg.key.id;

    const handleQualityChoice = async ({ messages: qMessages }) => {
      const qMsg = qMessages[0];
      if (!qMsg?.message) return;

      const qText = qMsg.message.conversation || qMsg.message.extendedTextMessage?.text;
      const isReplyToQuality = qMsg.message.extendedTextMessage?.contextInfo?.stanzaId === qualityMsgId;
      if (!isReplyToQuality || from !== qMsg.key.remoteJid) return;

      const choiceNum = parseInt(qText) - 1;
      if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= downloads.length) {
        await sock.sendMessage(from, {
          text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${downloads.length}_${CINESUBZ_FOOTER}`
        }, { quoted: qMsg });
        return;
      }

      sock.ev.off('messages.upsert', handleQualityChoice);
      const selectedDl = downloads[choiceNum];

      try {
        await sock.sendMessage(from, {
          document: { url: selectedDl.link },
          mimetype: 'video/mp4',
          fileName: `${movieInfo.title} - ${selectedDl.quality}.mp4`,
          caption: `🎬 *${movieInfo.title}*\n📊 *Quality:* ${selectedDl.quality}\n💾 *Size:* ${selectedDl.size || 'N/A'}${CINESUBZ_FOOTER}`
        }, { quoted: qMsg });
      } catch (dlErr) {
        console.error('Cinesubz movie download error:', dlErr.message);
        await sock.sendMessage(from, {
          text: `*❪ ERROR ❫*\n\n❌ *Download Failed!*\n🚫 _${dlErr.message}_${CINESUBZ_FOOTER}`
        }, { quoted: qMsg });
      }
    };

    sock.ev.on('messages.upsert', handleQualityChoice);

  } catch (err) {
    console.error('Cinesubz movie error:', err.message);
    await sock.sendMessage(from, {
      text: `*❪ ERROR ❫*\n\n❌ *Movie Details Error!*\n🚫 _${err.message}_${CINESUBZ_FOOTER}`
    }, { quoted: quotedMsg });
  }
}

async function handleMovieDownloadRequest(sock, from, input, msg) {
  const movies = await loadMovies();
  if (!movies || movies.length === 0) return;

  let targetMovie = null;
  const lowerInput = input.toLowerCase();

  // 1. Extract explicit "Movie Code: [code]" or "-[key]" pattern from website message
  const codeMatch = input.match(/Movie Code:\s*([^\s]+)/i) || input.match(/(-[a-zA-Z0-9_-]{10,})/i);
  if (codeMatch && codeMatch[1]) {
    const matchedCode = codeMatch[1].trim();
    targetMovie = movies.find(m => m.id === matchedCode || m.id === matchedCode.replace(/^-/, ''));
  }

  // 2. Direct ID string match
  if (!targetMovie) {
    targetMovie = movies.find(m => m.id && input.includes(String(m.id)));
  }

  // 3. Title fuzzy match (typo-tolerant)
  if (!targetMovie) {
    const queryClean = normalize(lowerInput
      .replace(/hi cineflix!|i want to download|via whatsapp|movie code:|\./gi, '')
      .replace(/480p|720p|1080p/gi, '')
      .replace(/\(\d{4}\)/g, ''));

    if (queryClean.length >= 2) {
      targetMovie = fuzzyFindMovie(movies, queryClean);
    }
  }

  if (!targetMovie) {
    if (input.startsWith('.')) {
      await sock.sendMessage(from, { text: config.messages.notFound }, { quoted: msg });
    }
    return;
  }

  // Determine requested resolution
  let requestedRes = '720p';
  if (lowerInput.includes('480p')) requestedRes = '480p';
  if (lowerInput.includes('720p')) requestedRes = '720p';
  if (lowerInput.includes('1080p')) requestedRes = '1080p';

  let targetDownload = null;
  if (targetMovie.downloads && targetMovie.downloads.length > 0) {
    targetDownload = targetMovie.downloads.find(d => d.res === requestedRes) || targetMovie.downloads[0];
  }

  const downloadUrl = targetDownload?.srv1 || targetDownload?.srv2 || targetDownload?.url || targetMovie.subUrl;
  const qualityText = targetDownload?.res || requestedRes;
  const movieTitle = targetMovie.title;
  const movieYear = targetMovie.year ? `(${targetMovie.year})` : '';

  // ── Movie Details Card with Poster Image ─────────────────────────────────
  const posterUrl = targetMovie.poster || targetMovie.backdrop || '';
  const genre     = targetMovie.genre  || '';
  const rating    = targetMovie.rating || '';
  const runtime   = targetMovie.runtime || '';
  const desc      = (targetMovie.description || targetMovie.desc || '').substring(0, 180);
  const size      = targetDownload?.size || '';

  const detailCaption =
    `🎬 *${movieTitle} ${movieYear}*\n` +
    `${'─'.repeat(28)}\n` +
    (genre   ? `🎭 *Genre:* ${genre}\n`      : '') +
    (rating  ? `⭐ *Rating:* ${rating}/10\n` : '') +
    (runtime ? `⏱ *Runtime:* ${runtime}\n`  : '') +
    `📁 *Quality:* ${qualityText}\n` +
    (size    ? `📦 *Size:* ${size}\n`        : '') +
    `🗣 *Subtitles:* Sinhala ✅\n` +
    `${'─'.repeat(28)}\n` +
    (desc    ? `📝 ${desc}${desc.length >= 180 ? '...' : ''}\n\n` : '') +
    `⏳ *Sending file... please wait* 🚀\n\n` +
    `🌐 ${process.env.WEBSITE_URL || config.websiteUrl}`;

  try {
    if (posterUrl && posterUrl.startsWith('http')) {
      // Send poster image with details caption
      const posterFetch = await fetch(posterUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0' },
        signal: AbortSignal.timeout(10000)
      });
      if (posterFetch.ok) {
        const posterBuf = Buffer.from(await posterFetch.arrayBuffer());
        await sock.sendMessage(from, {
          image: posterBuf,
          caption: detailCaption
        }, { quoted: msg });
      } else {
        throw new Error('poster fetch failed');
      }
    } else {
      throw new Error('no poster');
    }
  } catch (_) {
    // Fallback: text only card
    await sock.sendMessage(from, { text: detailCaption }, { quoted: msg });
  }

  if (!downloadUrl || downloadUrl === '#' || !downloadUrl.startsWith('http')) {
    // No URL at all — send tgLink or website
    const tgLink = targetDownload?.tgLink || '';
    await sock.sendMessage(from, {
      text: `🎬 *${movieTitle} ${movieYear} (${qualityText})*\n\n🗣 Sinhala Subtitles\n📥 *Download Link:*\n🔗 ${tgLink || (process.env.WEBSITE_URL || config.websiteUrl)}\n\n🍿 ${process.env.WEBSITE_URL || config.websiteUrl}`
    }, { quoted: msg });
    return;
  }

  const isMkv  = downloadUrl.toLowerCase().includes('.mkv');
  const mime   = isMkv ? 'video/x-matroska' : 'video/mp4';
  const ext    = isMkv ? 'mkv' : 'mp4';
  const safeTitle = movieTitle.replace(/[^a-zA-Z0-9 ]/g, '').trim();
  const fileName  = `${safeTitle}_${qualityText}.${ext}`;

  console.log(`📤 Attempting file send: ${fileName} | ${downloadUrl}`);

  try {
    // Method 1: Fetch buffer manually (handles redirects, auth headers)
    const fetchRes = await fetch(downloadUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': process.env.WEBSITE_URL || 'https://cineflix-lk.vercel.app' },
      redirect: 'follow',
      signal: AbortSignal.timeout(30000)
    });

    if (!fetchRes.ok) throw new Error(`HTTP ${fetchRes.status}`);

    const buffer = Buffer.from(await fetchRes.arrayBuffer());
    console.log(`✅ Fetched ${(buffer.length / 1024 / 1024).toFixed(1)}MB`);

    await sock.sendMessage(from, {
      document: buffer,
      mimetype: mime,
      fileName: fileName
    }, { quoted: msg });

    await sock.sendMessage(from, {
      text: config.messages.downloadSuccess
        .replace('{title}', movieTitle)
        .replace('{quality}', qualityText)
    });

  } catch (bufErr) {
    console.warn(`⚠️ Buffer fetch failed (${bufErr.message}), trying URL method...`);

    try {
      // Method 2: Let Baileys fetch directly via URL
      await sock.sendMessage(from, {
        document: { url: downloadUrl },
        mimetype: mime,
        fileName: fileName
      }, { quoted: msg });

      await sock.sendMessage(from, {
        text: config.messages.downloadSuccess
          .replace('{title}', movieTitle)
          .replace('{quality}', qualityText)
      });

    } catch (urlErr) {
      console.error(`❌ Both methods failed: ${urlErr.message}`);
      // Final fallback — send link as text
      const tgLink = targetDownload?.tgLink || '';
      await sock.sendMessage(from, {
        text: `🎬 *${movieTitle} ${movieYear} (${qualityText})*\n\n⚠️ Auto-send fail. Link copy කරලා download කරන්න:\n🔗 ${downloadUrl}\n${tgLink ? `\n📱 Telegram: ${tgLink}` : ''}\n\n🌐 ${process.env.WEBSITE_URL || config.websiteUrl}`
      }, { quoted: msg });
    }
  }
}

// Secure Authorization Middleware for Heroku Bot API Endpoints
const secureBotAuth = (req, res, next) => {
  const apiKey = req.headers['x-api-key'] || req.query.key;
  const SECRET_KEY = process.env.BOT_SECRET_KEY || 'cflx_bot_secure_2026';
  if (!apiKey || apiKey !== SECRET_KEY) {
    return res.status(403).json({ error: 'Unauthorized access to Cineflix Bot API endpoints.' });
  }
  next();
};

// REST API Endpoints for Web Dashboard & Pairing
app.get('/api/status', (req, res) => {
  res.json({
    connected: isConnected,
    database: dbStatus,
    bot: config.botName,
    timestamp: new Date()
  });
});

app.get('/api/qr', (req, res) => {
  res.json({
    connected: isConnected,
    qrImage: latestQRImage
  });
});

// Force Session Reset Endpoint to generate fresh QR code instantly
app.all('/api/reset', async (req, res) => {
  try {
    isConnected = false;
    latestQRImage = null;
    if (mongoose.connection.readyState === 1) {
      await BaileysAuth.deleteMany({});
    }
    const authFolder = path.join(__dirname, 'auth_info_baileys');
    if (fs.existsSync(authFolder)) {
      fs.rmSync(authFolder, { recursive: true, force: true });
    }
    if (sock) {
      try { sock.end(); } catch (e) {}
    }
    setTimeout(startBot, 1000);
    return res.json({ success: true, message: 'Session reset! Generating fresh QR code in 3 seconds...' });
  } catch (err) {
    return res.status(500).json({ error: 'Reset failed: ' + err.message });
  }
});

app.post('/api/pair', async (req, res) => {
  try {
    const { phone } = req.body;
    if (!phone) {
      return res.status(400).json({ error: 'Phone number is required.' });
    }
    const cleanPhone = phone.replace(/[^0-9]/g, '');
    if (!sock) {
      return res.status(503).json({ error: 'Bot socket is initializing, please try again in 5 seconds.' });
    }

    const code = await sock.requestPairingCode(cleanPhone);
    console.log(`📱 Generated Pairing Code [${code}] for phone [${cleanPhone}]`);
    return res.json({ success: true, code });
  } catch (err) {
    console.error('Error generating pairing code:', err.message);
    return res.status(500).json({ error: 'Failed to generate pairing code: ' + err.message });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`🌐 Cineflix Bot Server & Web QR/Pairing Dashboard running on port ${PORT}`);
});

startBot();
