const express = require('express');
const fs = require('fs');
const os = require('os');
const path = require('path');
const youtubedl = require('youtube-dl-exec');
const geoip = require('geoip-lite');

const app = express();
const PORT = 3000;
const SERVER_ACCESS_TOKEN = String(process.env.SERVER_ACCESS_TOKEN || '').trim();
const TEMP_DIR = path.join(os.tmpdir(), 'plannet-globe-mp3');
const LOG_DIR = path.join(__dirname, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'server.log');
const serverStartedAt = new Date();
const conversionStats = {
  total: 0,
  success: 0,
  failed: 0,
  active: 0,
  lastSuccessAt: null,
  lastFailureAt: null,
  lastError: null
};
const conversionErrorFeed = [];
const CONVERSION_ERROR_FEED_LIMIT = 300;
const analyticsState = {
  totalRequests: 0,
  totalPageViews: 0,
  users: new Map(),
  countryHits: new Map(),
  pageViews: new Map(),
  recentEvents: []
};
const ANALYTICS_ACTIVE_WINDOW_MS = 5 * 60 * 1000;
const ANALYTICS_HISTORY_WINDOW_MS = 24 * 60 * 60 * 1000;
const ANALYTICS_RECENT_EVENTS_LIMIT = 600;

const getStatusPayload = () => {
  const memory = process.memoryUsage();

  return {
    ok: true,
    service: 'plannet-converter',
    active: true,
    port: PORT,
    pid: process.pid,
    restartCommand: 'npm start',
    controllerCommand: 'npm run controller:app',
    startedAt: serverStartedAt.toISOString(),
    uptimeSec: Math.floor(process.uptime()),
    memory: {
      rss: memory.rss,
      heapUsed: memory.heapUsed,
      heapTotal: memory.heapTotal
    },
    conversions: { ...conversionStats },
    now: new Date().toISOString()
  };
};

const appendLog = async (level, message) => {
  try {
    await fs.promises.mkdir(LOG_DIR, { recursive: true });
    const line = `[${new Date().toISOString()}] [${level}] ${message}\n`;
    await fs.promises.appendFile(LOG_FILE, line, 'utf8');
  } catch (_error) {
    // ignore logging write failures
  }
};

const getClientIp = (req) => {
  const forwardedFor = req.headers['x-forwarded-for'];
  const forwardedIp = typeof forwardedFor === 'string' ? forwardedFor.split(',')[0].trim() : null;
  let ip = forwardedIp || req.socket?.remoteAddress || req.ip || 'unknown';

  if (ip.startsWith('::ffff:')) {
    ip = ip.slice(7);
  }

  if (ip === '::1') {
    return '127.0.0.1';
  }

  return ip;
};

const isLocalOrPrivateIp = (ip) => {
  if (!ip || ip === '127.0.0.1' || ip === '::1' || ip.toLowerCase() === 'localhost') {
    return true;
  }

  return /^10\.|^192\.168\.|^172\.(1[6-9]|2\d|3[0-1])\./.test(ip);
};

const resolveGeo = (ip) => {
  if (isLocalOrPrivateIp(ip)) {
    return { country: 'LOCAL', city: 'Localhost' };
  }

  const geo = geoip.lookup(ip);
  return {
    country: geo?.country || 'UNKNOWN',
    city: geo?.city || 'Inconnue'
  };
};

const shouldTrackRequest = (req) => {
  const requestPath = req.path || '/';
  if (req.method !== 'GET') {
    return false;
  }

  if (requestPath === '/favicon.ico' || requestPath.startsWith('/api/server/logs')) {
    return false;
  }

  return !/\.(css|js|map|png|jpg|jpeg|gif|svg|ico|webp|woff|woff2|ttf|otf)$/i.test(requestPath);
};

const isPageViewPath = (requestPath) => {
  if (!requestPath || requestPath === '/') {
    return true;
  }

  if (requestPath.startsWith('/api/') || requestPath === '/health' || requestPath === '/download') {
    return false;
  }

  if (requestPath.endsWith('.html')) {
    return true;
  }

  return !requestPath.includes('.');
};

const pruneAnalyticsUsers = () => {
  const threshold = Date.now() - ANALYTICS_HISTORY_WINDOW_MS;
  for (const [key, value] of analyticsState.users.entries()) {
    if (value.lastSeenMs < threshold) {
      analyticsState.users.delete(key);
    }
  }
};

const trackAnalyticsRequest = (req, statusCode, durationMs) => {
  const requestPath = req.path || '/';
  const ip = getClientIp(req);
  const geo = resolveGeo(ip);
  const nowMs = Date.now();
  const nowIso = new Date(nowMs).toISOString();
  const userAgent = String(req.headers['user-agent'] || 'Inconnu').slice(0, 300);
  const userKey = `${ip}|${userAgent}`;

  analyticsState.totalRequests += 1;

  if (isPageViewPath(requestPath)) {
    analyticsState.totalPageViews += 1;
    const currentPageCount = analyticsState.pageViews.get(requestPath) || 0;
    analyticsState.pageViews.set(requestPath, currentPageCount + 1);
  }

  const countryCount = analyticsState.countryHits.get(geo.country) || 0;
  analyticsState.countryHits.set(geo.country, countryCount + 1);

  const existingUser = analyticsState.users.get(userKey);
  if (existingUser) {
    existingUser.lastSeen = nowIso;
    existingUser.lastSeenMs = nowMs;
    existingUser.requestCount += 1;
    existingUser.lastPath = requestPath;
    existingUser.lastStatusCode = statusCode;
  } else {
    analyticsState.users.set(userKey, {
      ip,
      country: geo.country,
      city: geo.city,
      userAgent,
      firstSeen: nowIso,
      firstSeenMs: nowMs,
      lastSeen: nowIso,
      lastSeenMs: nowMs,
      requestCount: 1,
      lastPath: requestPath,
      lastStatusCode: statusCode
    });
  }

  analyticsState.recentEvents.push({
    time: nowIso,
    ip,
    country: geo.country,
    city: geo.city,
    path: requestPath,
    method: req.method,
    statusCode,
    durationMs,
    userAgent
  });

  if (analyticsState.recentEvents.length > ANALYTICS_RECENT_EVENTS_LIMIT) {
    analyticsState.recentEvents.splice(0, analyticsState.recentEvents.length - ANALYTICS_RECENT_EVENTS_LIMIT);
  }

  pruneAnalyticsUsers();
};

const getAnalyticsPayload = () => {
  const nowMs = Date.now();
  const activeThreshold = nowMs - ANALYTICS_ACTIVE_WINDOW_MS;
  const recentThreshold = nowMs - ANALYTICS_HISTORY_WINDOW_MS;
  const users = Array.from(analyticsState.users.values());

  const activeUsers = users.filter((user) => user.lastSeenMs >= activeThreshold);
  const users24h = users.filter((user) => user.lastSeenMs >= recentThreshold);

  const countryBreakdown = Array.from(analyticsState.countryHits.entries())
    .map(([country, hits]) => ({ country, hits }))
    .sort((a, b) => b.hits - a.hits)
    .slice(0, 12);

  const topPages = Array.from(analyticsState.pageViews.entries())
    .map(([pathName, views]) => ({ path: pathName, views }))
    .sort((a, b) => b.views - a.views)
    .slice(0, 12);

  const recentUsers = users
    .slice()
    .sort((a, b) => b.lastSeenMs - a.lastSeenMs)
    .slice(0, 30)
    .map((user) => ({
      ip: user.ip,
      country: user.country,
      city: user.city,
      firstSeen: user.firstSeen,
      lastSeen: user.lastSeen,
      requestCount: user.requestCount,
      lastPath: user.lastPath,
      lastStatusCode: user.lastStatusCode,
      userAgent: user.userAgent
    }));

  const recentEvents = analyticsState.recentEvents
    .slice(-40)
    .reverse();

  return {
    generatedAt: new Date().toISOString(),
    summary: {
      activeUsers: activeUsers.length,
      uniqueUsers24h: users24h.length,
      totalUsersTracked: users.length,
      totalRequests: analyticsState.totalRequests,
      totalPageViews: analyticsState.totalPageViews
    },
    countryBreakdown,
    topPages,
    recentUsers,
    recentEvents
  };
};

const pushConversionError = ({ time, ip, country, city, userAgent, url, reason, stage }) => {
  conversionErrorFeed.push({
    time,
    ip,
    country,
    city,
    userAgent,
    url,
    reason,
    stage
  });

  if (conversionErrorFeed.length > CONVERSION_ERROR_FEED_LIMIT) {
    conversionErrorFeed.splice(0, conversionErrorFeed.length - CONVERSION_ERROR_FEED_LIMIT);
  }
};

const getConversionErrorPayload = ({ minutes = 0, limit = 60 } = {}) => {
  const nowMs = Date.now();
  const safeMinutes = Number.isFinite(minutes) ? Math.max(0, minutes) : 0;
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(300, limit)) : 60;

  const filteredErrors = safeMinutes > 0
    ? conversionErrorFeed.filter((item) => {
      const timeMs = Date.parse(item.time);
      if (Number.isNaN(timeMs)) {
        return false;
      }
      return timeMs >= nowMs - (safeMinutes * 60 * 1000);
    })
    : conversionErrorFeed;

  const lastErrors = filteredErrors.slice(-safeLimit).reverse();

  return {
    generatedAt: new Date().toISOString(),
    totalTrackedErrors: conversionErrorFeed.length,
    filteredCount: filteredErrors.length,
    appliedFilterMinutes: safeMinutes,
    recentErrors: lastErrors
  };
};

const readLogTail = async (maxLines = 200) => {
  try {
    const content = await fs.promises.readFile(LOG_FILE, 'utf8');
    const lines = content.split(/\r?\n/).filter(Boolean);
    return lines.slice(-Math.max(1, Math.min(maxLines, 1000)));
  } catch (_error) {
    return [];
  }
};

const isValidYoutubeUrl = (input) => {
  try {
    const parsed = new URL(input);
    return /(^|\.)youtube\.com$|(^|\.)youtu\.be$/i.test(parsed.hostname);
  } catch (_error) {
    return false;
  }
};

const sanitizeFilename = (value) => {
  const cleaned = String(value || 'audio')
    .replace(/[\\/:*?"<>|]/g, '_')
    .replace(/\s+/g, ' ')
    .trim();

  return cleaned || 'audio';
};

const buildContentDispositionValue = (baseName) => {
  const normalized = sanitizeFilename(baseName)
    .replace(/[\r\n]/g, ' ')
    .slice(0, 140);

  const asciiBase = normalized
    .normalize('NFKD')
    .replace(/[^\x20-\x7E]/g, '')
    .replace(/["\\]/g, '')
    .replace(/[;=]/g, '_')
    .trim() || 'audio';

  const fullUtf8Name = `${normalized || 'audio'}.mp3`;
  const fullAsciiName = `${asciiBase}.mp3`;
  const encodedUtf8 = encodeURIComponent(fullUtf8Name)
    .replace(/['()]/g, escape)
    .replace(/\*/g, '%2A');

  return `attachment; filename="${fullAsciiName}"; filename*=UTF-8''${encodedUtf8}`;
};

const normalizeYoutubeUrl = (input) => {
  const parsed = new URL(input);
  const hostname = parsed.hostname.toLowerCase();

  if (hostname === 'youtu.be' || hostname.endsWith('.youtu.be')) {
    const videoId = parsed.pathname.replace(/^\//, '').split('/')[0];
    if (videoId) {
      return `https://www.youtube.com/watch?v=${videoId}`;
    }
  }

  if (hostname === 'youtube.com' || hostname.endsWith('.youtube.com')) {
    const videoId = parsed.searchParams.get('v');
    if (videoId) {
      return `https://www.youtube.com/watch?v=${videoId}`;
    }
  }

  return input;
};

const getRequestToken = (req) => {
  const authHeader = String(req.headers.authorization || '');
  if (authHeader.toLowerCase().startsWith('bearer ')) {
    return authHeader.slice(7).trim();
  }

  const headerToken = req.headers['x-access-token'];
  if (typeof headerToken === 'string' && headerToken.trim()) {
    return headerToken.trim();
  }

  const queryToken = req.query?.token;
  if (typeof queryToken === 'string' && queryToken.trim()) {
    return queryToken.trim();
  }

  return '';
};

const requiresProtectedAccess = (reqPath) => {
  if (!reqPath) {
    return false;
  }

  return reqPath === '/download'
    || reqPath === '/health'
    || reqPath === '/chantier/server-controller.html'
    || reqPath.startsWith('/api/server/');
};

const sendUnauthorized = (req, res) => {
  const acceptsHtml = String(req.headers.accept || '').includes('text/html');
  if (acceptsHtml) {
    res.status(401).send('Accès non autorisé');
    return;
  }

  res.status(401).json({ error: 'Accès non autorisé' });
};

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Access-Token');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});

app.use((req, res, next) => {
  if (!SERVER_ACCESS_TOKEN) {
    next();
    return;
  }

  if (!requiresProtectedAccess(req.path || '')) {
    next();
    return;
  }

  const token = getRequestToken(req);
  if (token !== SERVER_ACCESS_TOKEN) {
    appendLog('WARN', `Accès refusé sur ${req.path || '/'} depuis ${getClientIp(req)}`);
    sendUnauthorized(req, res);
    return;
  }

  next();
});

app.use((req, res, next) => {
  if (!shouldTrackRequest(req)) {
    next();
    return;
  }

  const startedAt = Date.now();
  res.on('finish', () => {
    trackAnalyticsRequest(req, res.statusCode, Date.now() - startedAt);
  });

  next();
});

// Sert tout le workspace PlanNet Globe (TriDoc, PlanNetGlobe, chantier, etc.)
app.use(express.static(path.join(__dirname, '..')));

// Entrée par défaut vers le bloc Services
app.get('/', (_req, res) => {
  res.redirect('/TriDoc/index.html');
});

app.get('/favicon.ico', (_req, res) => {
  res.status(204).end();
});

// Route pour la conversion
app.get('/download', async (req, res) => {
  conversionStats.total += 1;
  conversionStats.active += 1;

  const requesterIp = getClientIp(req);
  const requesterGeo = resolveGeo(requesterIp);
  const requesterUserAgent = String(req.headers['user-agent'] || 'Inconnu').slice(0, 300);
  const requestedAt = new Date().toISOString();
  const attemptedUrlRaw = String(req.query.url || '').trim();
  let conversionErrorReported = false;

  const reportConversionErrorOnce = (stage, reason) => {
    if (conversionErrorReported) {
      return;
    }

    conversionErrorReported = true;
    pushConversionError({
      time: requestedAt,
      ip: requesterIp,
      country: requesterGeo.country,
      city: requesterGeo.city,
      userAgent: requesterUserAgent,
      url: attemptedUrlRaw || 'URL absente',
      reason: String(reason || 'Erreur inconnue').slice(0, 500),
      stage
    });
  };

  let outputFilePath = null;
  let streamClosed = false;
  let requestFinalized = false;

  const finalizeRequest = (isSuccess, errorMessage = null) => {
    if (requestFinalized) {
      return;
    }

    requestFinalized = true;
    conversionStats.active = Math.max(0, conversionStats.active - 1);

    if (isSuccess) {
      conversionStats.success += 1;
      conversionStats.lastSuccessAt = new Date().toISOString();
      conversionStats.lastError = null;
      return;
    }

    conversionStats.failed += 1;
    conversionStats.lastFailureAt = new Date().toISOString();
    if (errorMessage) {
      conversionStats.lastError = String(errorMessage).slice(0, 500);
    }
  };

  const cleanupOutput = async () => {
    if (!outputFilePath) {
      return;
    }

    try {
      await fs.promises.unlink(outputFilePath);
    } catch (_error) {
      // ignore cleanup errors
    }
  };

  try {
    const inputUrl = attemptedUrlRaw;
    if (!inputUrl || !isValidYoutubeUrl(inputUrl)) {
      await appendLog('WARN', 'Requête refusée: URL YouTube invalide');
      reportConversionErrorOnce('validation', 'URL YouTube invalide');
      finalizeRequest(false, 'URL YouTube invalide');
      return res.status(400).json({ error: 'URL YouTube invalide' });
    }
    const url = normalizeYoutubeUrl(inputUrl);

    await fs.promises.mkdir(TEMP_DIR, { recursive: true });

    const token = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const outputTemplate = path.join(TEMP_DIR, `${token}.%(ext)s`);

    const rawTitle = await youtubedl(url, {
      getTitle: true,
      noWarnings: true,
      noCheckCertificates: true
    });
    const title = sanitizeFilename(rawTitle);

    await youtubedl(url, {
      extractAudio: true,
      audioFormat: 'mp3',
      audioQuality: '0',
      format: 'bestaudio/best',
      output: outputTemplate,
      noWarnings: true,
      noCheckCertificates: true
    });

    const generatedFiles = await fs.promises.readdir(TEMP_DIR);
    const outputFile = generatedFiles.find((fileName) => fileName.startsWith(`${token}.`) && fileName.endsWith('.mp3'));

    if (!outputFile) {
      throw new Error('Fichier MP3 introuvable après conversion');
    }

    outputFilePath = path.join(TEMP_DIR, outputFile);

    res.setHeader('Content-Disposition', buildContentDispositionValue(title));
    res.setHeader('Content-Type', 'audio/mpeg');

    const outputStream = fs.createReadStream(outputFilePath);

    outputStream.on('error', async (err) => {
      console.error('Erreur stream fichier:', err);
      await appendLog('ERROR', `Erreur stream fichier: ${err.message}`);
      reportConversionErrorOnce('stream', err.message);
      finalizeRequest(false, err.message);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Erreur lors de l’envoi du fichier MP3' });
      }
      await cleanupOutput();
    });

    outputStream.on('close', async () => {
      if (streamClosed) {
        return;
      }
      streamClosed = true;
      await cleanupOutput();
    });

    res.on('close', async () => {
      if (!res.writableEnded) {
        await appendLog('WARN', 'Connexion client interrompue pendant /download');
        reportConversionErrorOnce('client', 'Connexion client interrompue');
        finalizeRequest(false, 'Connexion client interrompue');
      }
      if (streamClosed) {
        return;
      }
      streamClosed = true;
      outputStream.destroy();
      await cleanupOutput();
    });

    res.on('finish', () => {
      if (res.statusCode >= 200 && res.statusCode < 400) {
        appendLog('INFO', `Conversion terminée avec succès (HTTP ${res.statusCode})`);
        finalizeRequest(true);
        return;
      }

      appendLog('ERROR', `Conversion terminée en erreur (HTTP ${res.statusCode})`);
      reportConversionErrorOnce('http', `HTTP ${res.statusCode}`);
      finalizeRequest(false, `HTTP ${res.statusCode}`);
    });

    outputStream.pipe(res);

  } catch (error) {
    console.error('Erreur générale:', error);
    await appendLog('ERROR', `Erreur générale /download: ${error.message}`);
    reportConversionErrorOnce('exception', error.message);
    finalizeRequest(false, error.message);
    await cleanupOutput();
    if (!res.headersSent) {
      res.status(500).json({ error: 'Erreur lors de la conversion (yt-dlp)' });
    }
  }
});

app.get('/health', (_req, res) => {
  res.status(200).json(getStatusPayload());
});

app.get('/api/server/status', (_req, res) => {
  res.status(200).json(getStatusPayload());
});

app.get('/api/server/analytics', (_req, res) => {
  res.status(200).json(getAnalyticsPayload());
});

app.get('/api/server/conversion-errors', (req, res) => {
  const minutes = Number(req.query.minutes || 0);
  const limit = Number(req.query.limit || 60);
  res.status(200).json(getConversionErrorPayload({ minutes, limit }));
});

app.get('/api/server/logs', async (req, res) => {
  const tail = Number(req.query.tail) || 200;
  const lines = await readLogTail(tail);
  const payload = lines.length ? `${lines.join('\n')}\n` : 'Aucun log disponible pour le moment.\n';
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.status(200).send(payload);
});

const server = app.listen(PORT, () => {
  console.log(`Serveur démarré sur http://localhost:${PORT}`);
  appendLog('INFO', `Serveur démarré sur http://localhost:${PORT}`);
  if (SERVER_ACCESS_TOKEN) {
    appendLog('INFO', 'Protection token activée (SERVER_ACCESS_TOKEN).');
  }
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Le port ${PORT} est déjà utilisé. Fermez l'autre serveur Node puis relancez npm start.`);
    appendLog('ERROR', `Port ${PORT} déjà utilisé au démarrage.`);
    process.exit(1);
  }

  console.error('Erreur au démarrage du serveur:', err);
  appendLog('ERROR', `Erreur au démarrage du serveur: ${err.message}`);
  process.exit(1);
});