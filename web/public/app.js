const app = document.getElementById('app');

const state = {
  mode: null,
  lang: null,
  file: null,
  pageIndex: 0,
  pages: [],
  editor: null,
};

const rtlLangs = new Set(['ar', 'fa', 'he', 'ur']);

const escapeHtml = (value) =>
  value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');

const api = {
  async getLangs() {
    const res = await fetch('/api/langs');
    if (!res.ok) throw new Error('Failed to load languages.');
    return res.json();
  },
  async getChapters(lang) {
    const res = await fetch(`/api/chapters/${encodeURIComponent(lang)}`);
    if (!res.ok) throw new Error('Failed to load chapters.');
    return res.json();
  },
  async getContent(lang, file) {
    const res = await fetch(`/api/content/${encodeURIComponent(lang)}/${encodeURIComponent(file)}`);
    if (!res.ok) throw new Error('Failed to load content.');
    return res.text();
  },
  async saveContent(lang, file, content) {
    const res = await fetch(`/api/content/${encodeURIComponent(lang)}/${encodeURIComponent(file)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
    if (!res.ok) throw new Error('Failed to save content.');
    return res.json();
  },
};

function setDir(element) {
  if (!state.lang) {
    element.setAttribute('dir', 'auto');
    return;
  }
  element.setAttribute('dir', rtlLangs.has(state.lang) ? 'rtl' : 'auto');
}

function renderLogin() {
  state.mode = null;
  state.lang = null;
  state.file = null;
  app.innerHTML = `
    <section class="prompt">
      <h2>ENTER ACCESS CODE:</h2>
      <form id="login-form" class="access-row">
        <input id="access-code" type="password" autocomplete="off" autofocus />
        <button type="submit">ENTER</button>
      </form>
      <p id="login-error" class="status"></p>
    </section>
  `;

  const form = document.getElementById('login-form');
  const input = document.getElementById('access-code');
  const error = document.getElementById('login-error');

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const code = input.value.trim();

    if (code === 'root') {
      state.mode = 'reader';
      renderLanguageSelection();
      return;
    }

    if (code === 'archivist') {
      state.mode = 'editor';
      renderLanguageSelection();
      return;
    }

    error.textContent = 'INVALID ACCESS CODE';
    input.select();
  });
}

async function renderLanguageSelection() {
  app.innerHTML = '<p>Loading languages...</p>';

  try {
    const langs = await api.getLangs();

    app.innerHTML = `
      <h2>Select Language</h2>
      <div class="selection-grid" id="langs-grid"></div>
      <div class="footer-nav">
        <span class="nav-link" id="back-login">[ &lt; BACK ]</span>
      </div>
    `;

    const grid = document.getElementById('langs-grid');
    langs.forEach((lang) => {
      const node = document.createElement('button');
      node.className = 'option';
      node.textContent = lang;
      node.addEventListener('click', () => {
        state.lang = lang;
        renderChapterSelection();
      });
      grid.appendChild(node);
    });

    document.getElementById('back-login').addEventListener('click', renderLogin);
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function renderChapterSelection() {
  app.innerHTML = '<p>Loading chapters...</p>';

  try {
    const chapters = await api.getChapters(state.lang);

    app.innerHTML = `
      <h2>Language: ${escapeHtml(state.lang)}</h2>
      <h3>Select Chapter</h3>
      <div class="selection-grid" id="chapters-grid"></div>
      <div class="footer-nav">
        <span class="nav-link" id="back-lang">[ &lt; BACK ]</span>
      </div>
    `;

    const grid = document.getElementById('chapters-grid');
    chapters.forEach((chapter) => {
      const node = document.createElement('button');
      node.className = 'option';
      node.textContent = chapter;
      node.addEventListener('click', () => {
        state.file = chapter;
        if (state.mode === 'reader') {
          renderReader();
        } else {
          renderEditor();
        }
      });
      grid.appendChild(node);
    });

    document.getElementById('back-lang').addEventListener('click', renderLanguageSelection);
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

function buildPagesFromHtml(html, viewport) {
  const holder = document.createElement('div');
  holder.style.position = 'absolute';
  holder.style.visibility = 'hidden';
  holder.style.pointerEvents = 'none';
  holder.style.width = `${viewport.clientWidth}px`;
  holder.style.height = `${viewport.clientHeight}px`;
  holder.style.overflow = 'hidden';
  holder.style.lineHeight = getComputedStyle(viewport).lineHeight;
  holder.className = viewport.className;
  document.body.appendChild(holder);

  const source = document.createElement('div');
  source.innerHTML = html;
  const nodes = [...source.children];

  const pages = [];
  let page = document.createElement('div');

  const commitPage = () => {
    pages.push(page.innerHTML || '<p></p>');
    page = document.createElement('div');
  };

  nodes.forEach((node) => {
    const candidate = page.cloneNode(true);
    candidate.appendChild(node.cloneNode(true));
    holder.innerHTML = '';
    holder.appendChild(candidate);

    if (holder.scrollHeight > holder.clientHeight && page.childNodes.length > 0) {
      commitPage();
    }

    page.appendChild(node.cloneNode(true));
  });

  if (page.childNodes.length > 0 || pages.length === 0) {
    commitPage();
  }

  document.body.removeChild(holder);
  return pages;
}

async function renderReader() {
  app.innerHTML = '<p>Loading markdown...</p>';

  try {
    const markdown = await api.getContent(state.lang, state.file);
    const html = marked.parse(markdown);

    app.innerHTML = `
      <h2>${escapeHtml(state.lang)} / ${escapeHtml(state.file)}</h2>
      <section class="terminal-screen" id="reader-screen"></section>
      <div class="footer-nav">
        <span class="nav-link" id="reader-back">[ &lt; BACK ]</span>
        <span id="page-indicator"></span>
        <span class="nav-link" id="reader-next">[ NEXT &gt; ]</span>
      </div>
    `;

    const screen = document.getElementById('reader-screen');
    setDir(screen);
    state.pages = buildPagesFromHtml(html, screen);
    state.pageIndex = 0;

    const indicator = document.getElementById('page-indicator');
    const paint = () => {
      screen.innerHTML = state.pages[state.pageIndex] || '<p></p>';
      indicator.textContent = `PAGE ${state.pageIndex + 1} / ${state.pages.length}`;
    };

    paint();

    document.getElementById('reader-back').addEventListener('click', () => {
      if (state.pageIndex > 0) {
        state.pageIndex -= 1;
        paint();
      } else {
        renderChapterSelection();
      }
    });

    document.getElementById('reader-next').addEventListener('click', () => {
      if (state.pageIndex < state.pages.length - 1) {
        state.pageIndex += 1;
        paint();
      }
    });

    window.onresize = () => {
      state.pages = buildPagesFromHtml(html, screen);
      state.pageIndex = Math.min(state.pageIndex, state.pages.length - 1);
      paint();
    };
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function renderEditor() {
  app.innerHTML = '<p>Loading editor...</p>';

  try {
    const markdown = await api.getContent(state.lang, state.file);

    app.innerHTML = `
      <h2>${escapeHtml(state.lang)} / ${escapeHtml(state.file)}</h2>
      <section class="editor-wrap">
        <textarea id="editor-input"></textarea>
      </section>
      <p class="status" id="editor-status"></p>
      <div class="footer-nav">
        <span class="nav-link" id="editor-back">[ &lt; BACK ]</span>
        <button id="save-btn">[ SAVE TO DISK ]</button>
      </div>
    `;

    if (state.editor) {
      state.editor.toTextArea();
      state.editor = null;
    }

    const textarea = document.getElementById('editor-input');
    textarea.value = markdown;

    state.editor = new EasyMDE({
      element: textarea,
      spellChecker: false,
      status: false,
      autofocus: true,
    });

    const wrapper = state.editor.codemirror.getWrapperElement();
    wrapper.setAttribute('dir', rtlLangs.has(state.lang) ? 'rtl' : 'auto');

    const status = document.getElementById('editor-status');
    document.getElementById('save-btn').addEventListener('click', async () => {
      try {
        await api.saveContent(state.lang, state.file, state.editor.value());
        status.textContent = 'Saved.';
      } catch (error) {
        status.textContent = error.message;
      }
    });

    document.getElementById('editor-back').addEventListener('click', () => {
      state.editor.toTextArea();
      state.editor = null;
      renderChapterSelection();
    });
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

renderLogin();
