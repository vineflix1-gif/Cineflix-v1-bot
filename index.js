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

const FIREBASE_DB_URL = 'https://sutable-99848-default-rtdb.firebaseio.com';
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
          setTimeout(startBot, 15000);
          return;
        }
      }

      if (shouldReconnect) {
        setTimeout(startBot, 3000);
      } else {
        console.log('🔴 Logged out permanently. Please restart and scan QR again.');
      }
    } else if (connection === 'open') {
      isConnected = true;
      latestQRImage = null;
      console.log('✅ WhatsApp Bot Connected Successfully!');
      // Pre-load movie cache so first .movie command is instant
      loadMovies().then(m => console.log(`🎬 Pre-loaded ${m.length} movies into cache`)).catch(()=>{});
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
      const isCmd = cleanText.startsWith('.movie') || cleanText.startsWith('.dl') || cleanText.startsWith('.get') || cleanText.startsWith('.find') || cleanText.startsWith('.search') || cleanText === '.info' || cleanText === '.ping';

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

  const statusText = `⏳ Sending *${movieTitle} ${movieYear} (${qualityText})* with Sinhala Subtitles | සිංහල උපසිරැසි සමඟ...`;
  
  await sock.sendMessage(from, { text: statusText }, { quoted: msg });
  await sock.sendMessage(from, { text: '✅' });

  try {
    // If direct downloadable video file
    if (downloadUrl && downloadUrl !== '#' && downloadUrl.startsWith('http')) {
      const isDirectFile = downloadUrl.match(/\.(mp4|mkv|avi|mov|zip|rar|srt)$/i) || downloadUrl.includes('/download') || downloadUrl.includes('direct');
      if (isDirectFile) {
        const fileName = `${movieTitle.replace(/[^a-zA-Z0-9]/g, '_')}_${qualityText}.mp4`;
        
        await sock.sendMessage(
          from,
          {
            document: { url: downloadUrl },
            mimetype: 'video/mp4',
            fileName: fileName,
            caption: `🎬 *${movieTitle} ${movieYear}*\n\n📁 Quality: ${qualityText}\n📦 Size: ${targetDownload?.size || 'HD'}\n🗣 Subtitle: Cineflix Sinhala Subtitles (${process.env.WEBSITE_URL || config.websiteUrl})\n\n🌐 Downloaded via Cineflix: ${process.env.WEBSITE_URL || config.websiteUrl}`
          },
          { quoted: msg }
        );

        await sock.sendMessage(from, {
          text: config.messages.downloadSuccess
            .replace('{title}', movieTitle)
            .replace('{quality}', qualityText)
        });
        return;
      }
    }

    // Direct text link delivery for web download links / mirror servers
    const linkNotice = `🎬 *${movieTitle} ${movieYear} (${qualityText})*\n\n🗣 Subtitle: Cineflix Sinhala Subtitles (${process.env.WEBSITE_URL || config.websiteUrl})\n📥 *Direct Server Download Link:*\n🔗 ${downloadUrl || targetDownload?.tgLink || 'https://t.me/Cineflix_cloud_Bot'}\n\n🍿 *Watch Online & Download Web:* ${process.env.WEBSITE_URL || config.websiteUrl}`;
    await sock.sendMessage(from, { text: linkNotice }, { quoted: msg });
  } catch (downloadErr) {
    console.error('Error delivering movie file:', downloadErr.message);
    const fallbackMsg = `🎬 *${movieTitle} ${movieYear} (${qualityText})*\n\n🗣 Subtitle: Cineflix Sinhala Subtitles (${process.env.WEBSITE_URL || config.websiteUrl})\n📥 *Download Link:*\n🔗 ${downloadUrl || targetDownload?.tgLink || config.websiteUrl}\n\n🌐 Cineflix Web: ${process.env.WEBSITE_URL || config.websiteUrl}`;
    await sock.sendMessage(from, { text: fallbackMsg }, { quoted: msg });
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
