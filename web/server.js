const express = require('express');
const fs = require('fs/promises');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const ROOT_CODE = process.env.ROOT_CODE || 'root';
const ARCHIVIST_CODE = process.env.ARCHIVIST_CODE || 'archivist';
const TRUST_PROXY = process.env.TRUST_PROXY || '0';

if (TRUST_PROXY === '1') {
  app.set('trust proxy', 1);
}
app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Cross-Origin-Resource-Policy', 'same-site');
  res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net; style-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self' https://cdn.jsdelivr.net; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

const SAFE_SEGMENT = /^[a-zA-Z0-9._-]+$/;
const AUTH_WINDOW_MS = 60 * 1000;
const AUTH_MAX_ATTEMPTS = 30;
const authAttemptBuckets = new Map();

function validateSegment(value, type) {
  const isUnsafeReserved = value === '.' || value === '..' || value.startsWith('.');

  if (!SAFE_SEGMENT.test(value) || isUnsafeReserved) {
    const error = new Error(`Invalid ${type}.`);
    error.status = 400;
    throw error;
  }
}

function getClientIp(req) {
  return req.ip || 'unknown';
}

function registerFailedAuthAttempt(ip) {
  const now = Date.now();
  const bucket = authAttemptBuckets.get(ip);
  if (!bucket || now - bucket.startedAt > AUTH_WINDOW_MS) {
    authAttemptBuckets.set(ip, { startedAt: now, attempts: 1 });
    return 1;
  }

  bucket.attempts += 1;
  return bucket.attempts;
}

function clearFailedAuthAttempts(ip) {
  authAttemptBuckets.delete(ip);
}

function roleFromCode(req) {
  const accessCode = (req.get('x-access-code') || '').trim();

  if (accessCode === ROOT_CODE) {
    return 'root';
  }

  if (accessCode === ARCHIVIST_CODE) {
    return 'archivist';
  }

  return null;
}

function requireApiAuth(req, res, next) {
  const clientIp = getClientIp(req);
  const role = roleFromCode(req);

  if (!role) {
    const attempts = registerFailedAuthAttempt(clientIp);
    if (attempts >= AUTH_MAX_ATTEMPTS) {
      return res.status(429).json({ error: 'Too many authentication attempts. Please retry later.' });
    }

    return res.status(401).json({ error: 'Unauthorized.' });
  }

  clearFailedAuthAttempts(clientIp);
  req.userRole = role;
  req.clientIp = clientIp;
  return next();
}

async function getLanguages() {
  const entries = await fs.readdir(DATA_DIR, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));
}

async function getChapters(lang) {
  validateSegment(lang, 'language');
  const langDir = path.join(DATA_DIR, lang);
  const entries = await fs.readdir(langDir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith('.md'))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));
}

function contentPath(lang, file) {
  validateSegment(lang, 'language');
  validateSegment(file, 'file');

  if (!file.endsWith('.md')) {
    const error = new Error('Only markdown files are allowed.');
    error.status = 400;
    throw error;
  }

  return path.join(DATA_DIR, lang, file);
}

async function writeMarkdownAtomic(filePath, markdown) {
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;

  try {
    await fs.writeFile(tempPath, markdown, 'utf8');
    await fs.rename(tempPath, filePath);
  } catch (error) {
    await fs.rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
}

const EMAIL_SIGNUPS_FILE = path.join(DATA_DIR, 'more_email_signups.csv');
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const EMAIL_SIGNUP_WINDOW_MS = 60 * 1000;
const EMAIL_SIGNUP_MAX_ATTEMPTS = 6;
const EMAIL_SIGNUP_HEADER = 'timestamp,language,email\n';
const EMAIL_SIGNUPS_FILE_MAX_BYTES = 5 * 1024 * 1024;
const EMAIL_SIGNUP_BUCKET_PRUNE_INTERVAL = 50;
const emailSignupAttemptBuckets = new Map();
let emailSignupAttemptCount = 0;
let signupWriteQueue = Promise.resolve();

function pruneEmailSignupAttempts(now) {
  for (const [ip, bucket] of emailSignupAttemptBuckets.entries()) {
    if (now - bucket.startedAt > EMAIL_SIGNUP_WINDOW_MS * 2) {
      emailSignupAttemptBuckets.delete(ip);
    }
  }
}

function registerEmailSignupAttempt(ip) {
  const now = Date.now();
  emailSignupAttemptCount += 1;

  if (emailSignupAttemptCount % EMAIL_SIGNUP_BUCKET_PRUNE_INTERVAL === 0) {
    pruneEmailSignupAttempts(now);
  }

  const bucket = emailSignupAttemptBuckets.get(ip);

  if (!bucket || now - bucket.startedAt > EMAIL_SIGNUP_WINDOW_MS) {
    emailSignupAttemptBuckets.set(ip, { startedAt: now, attempts: 1 });
    return 1;
  }

  bucket.attempts += 1;
  return bucket.attempts;
}

function normalizeEmail(email) {
  return email.trim().toLowerCase();
}

function sanitizeSpreadsheetFormula(value) {
  if (/^[=+\-@]/.test(value)) {
    return `'${value}`;
  }

  return value;
}

function escapeCsvValue(value) {
  const normalized = String(value ?? '').replaceAll('"', '""');
  return `"${normalized}"`;
}

function parseCsvLine(line) {
  const match = line.match(/^"((?:[^"]|"")*)","((?:[^"]|"")*)","((?:[^"]|"")*)"$/);
  if (!match) return null;

  return {
    timestamp: match[1].replaceAll('""', '"'),
    language: match[2].replaceAll('""', '"'),
    email: match[3].replaceAll('""', '"'),
  };
}

async function queueSignupWrite(action) {
  signupWriteQueue = signupWriteQueue.then(action, action);
  return signupWriteQueue;
}

async function appendEmailSignup(email, lang) {
  const normalizedEmail = normalizeEmail(email);

  if (!EMAIL_REGEX.test(normalizedEmail)) {
    const error = new Error('Please enter a valid email address.');
    error.status = 400;
    error.appCode = 'invalid_email';
    throw error;
  }

  return queueSignupWrite(async () => {
    let existing = '';
    try {
      const signupFileStat = await fs.stat(EMAIL_SIGNUPS_FILE);
      if (signupFileStat.size > EMAIL_SIGNUPS_FILE_MAX_BYTES) {
        const error = new Error('Email signup storage is temporarily unavailable.');
        error.status = 503;
        error.appCode = 'signup_storage_limit';
        throw error;
      }

      existing = await fs.readFile(EMAIL_SIGNUPS_FILE, 'utf8');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }

    if (existing && Buffer.byteLength(existing, 'utf8') > EMAIL_SIGNUPS_FILE_MAX_BYTES) {
      const error = new Error('Email signup storage is temporarily unavailable.');
      error.status = 503;
      error.appCode = 'signup_storage_limit';
      throw error;
    }

    const lines = existing.split('\n').filter(Boolean);
    const hasHeader = lines[0] === EMAIL_SIGNUP_HEADER.trim();
    const dataLines = hasHeader ? lines.slice(1) : lines;

    const alreadyExists = dataLines.some((line) => {
      const parsed = parseCsvLine(line);
      return parsed && parsed.language === lang && parsed.email === normalizedEmail;
    });

    if (alreadyExists) {
      return { status: 'already_exists' };
    }

    const safeEmail = sanitizeSpreadsheetFormula(normalizedEmail);
    const timestamp = new Date().toISOString();
    const row = `${escapeCsvValue(timestamp)},${escapeCsvValue(lang)},${escapeCsvValue(safeEmail)}\n`;

    if (!existing) {
      await fs.writeFile(EMAIL_SIGNUPS_FILE, `${EMAIL_SIGNUP_HEADER}${row}`, 'utf8');
    } else if (!hasHeader) {
      await fs.writeFile(EMAIL_SIGNUPS_FILE, `${EMAIL_SIGNUP_HEADER}${existing}${existing.endsWith('\n') ? '' : '\n'}${row}`, 'utf8');
    } else {
      await fs.appendFile(EMAIL_SIGNUPS_FILE, row, 'utf8');
    }

    return { status: 'created' };
  });
}


app.get('/i18n/:lang', async (req, res, next) => {
  try {
    validateSegment(req.params.lang, 'language');
    const i18nPath = path.join(__dirname, 'i18n', `${req.params.lang}.json`);
    const text = await fs.readFile(i18nPath, 'utf8');
    const dict = JSON.parse(text);
    res.json(dict);
  } catch (error) {
    if (error instanceof SyntaxError) {
      error.status = 500;
    }
    next(error);
  }
});

app.use('/api', requireApiAuth);

app.get('/api/whoami', (req, res) => {
  res.json({ role: req.userRole });
});

app.get('/api/langs', async (_req, res, next) => {
  try {
    const langs = await getLanguages();
    res.json(langs);
  } catch (error) {
    next(error);
  }
});

app.get('/api/chapters/:lang', async (req, res, next) => {
  try {
    const chapters = await getChapters(req.params.lang);
    res.json(chapters);
  } catch (error) {
    next(error);
  }
});

app.get('/api/content/:lang/:file', async (req, res, next) => {
  try {
    const filePath = contentPath(req.params.lang, req.params.file);
    const text = await fs.readFile(filePath, 'utf8');
    res.type('text/plain').send(text);
  } catch (error) {
    next(error);
  }
});

app.post('/api/content/:lang/:file', async (req, res, next) => {
  try {
    if (req.userRole !== 'archivist') {
      return res.status(403).json({ error: 'Forbidden: archivist code required for writes.' });
    }

    const filePath = contentPath(req.params.lang, req.params.file);
    const markdown = typeof req.body?.content === 'string' ? req.body.content : null;

    if (markdown === null) {
      return res.status(400).json({ error: 'Request body must include a string content field.' });
    }

    await writeMarkdownAtomic(filePath, markdown);
    console.log(
      `[AUDIT] write role=${req.userRole} ip=${req.clientIp} lang=${req.params.lang} file=${req.params.file} ts=${new Date().toISOString()}`
    );
    return res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post('/api/more-signups', async (req, res, next) => {
  try {
    const email = typeof req.body?.email === 'string' ? req.body.email.trim() : '';
    const lang = typeof req.body?.lang === 'string' ? req.body.lang.trim() : '';

    if (!lang) {
      return res.status(400).json({ error: 'Language is required.', code: 'missing_language' });
    }

    const signupIp = req.clientIp || req.ip || 'unknown';
    const attempts = registerEmailSignupAttempt(signupIp);
    if (attempts > EMAIL_SIGNUP_MAX_ATTEMPTS) {
      return res.status(429).json({ error: 'Too many email signup attempts. Please retry later.', code: 'signup_rate_limited' });
    }

    validateSegment(lang, 'language');
    const result = await appendEmailSignup(email, lang);
    return res.json({ ok: true, status: result.status });
  } catch (error) {
    if (error.appCode) {
      return res.status(error.status || 400).json({ error: error.message, code: error.appCode });
    }

    if (error.status && error.status < 500) {
      return res.status(error.status).json({ error: error.message, code: 'invalid_request' });
    }

    next(error);
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use((error, _req, res, _next) => {
  console.error(error);

  if (error.code === 'ENOENT') {
    return res.status(404).json({ error: 'Resource not found.' });
  }

  const status = error.status || 500;
  const message = status >= 500 ? 'Server error.' : error.message;
  return res.status(status).json({ error: message });
});

async function start() {
  try {
    const stat = await fs.stat(DATA_DIR);
    if (!stat.isDirectory()) {
      throw new Error(`DATA_DIR is not a directory: ${DATA_DIR}`);
    }

    app.listen(PORT, () => {
      console.log(`Mosaic Terminal listening on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error(`Unable to start server. DATA_DIR check failed for: ${DATA_DIR}`);
    console.error(error);
    process.exit(1);
  }
}

start();
