const express = require('express');
const fs = require('fs/promises');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');

app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const SAFE_SEGMENT = /^[a-zA-Z0-9._-]+$/;

function validateSegment(value, type) {
  if (!SAFE_SEGMENT.test(value)) {
    const error = new Error(`Invalid ${type}.`);
    error.status = 400;
    throw error;
  }
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
    const filePath = contentPath(req.params.lang, req.params.file);
    const markdown = typeof req.body?.content === 'string' ? req.body.content : null;

    if (markdown === null) {
      return res.status(400).json({ error: 'Request body must include a string content field.' });
    }

    await fs.writeFile(filePath, markdown, 'utf8');
    return res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use((error, _req, res, _next) => {
  if (error.code === 'ENOENT') {
    return res.status(404).json({ error: 'Resource not found.' });
  }

  const status = error.status || 500;
  const message = status >= 500 ? 'Server error.' : error.message;
  return res.status(status).json({ error: message });
});

app.listen(PORT, () => {
  console.log(`Mosaic Terminal listening on http://localhost:${PORT}`);
});
