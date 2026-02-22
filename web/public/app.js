const app = document.getElementById('app');

const state = {
  mode: null,
  lang: null,
  file: null,
  accessCode: null,
  pageIndex: 0,
  pages: [],
  editor: null,
  readerResizeHandler: null,
  i18nLang: 'en',
  i18nDict: {},
};

const rtlLangPrefixes = ['ar', 'fa', 'he', 'ur'];

function isRtlLanguage(lang) {
  if (!lang) return false;
  const normalizedLang = lang.trim().toLowerCase();
  if (!normalizedLang) return false;

  return rtlLangPrefixes.some(
    (prefix) =>
      normalizedLang === prefix ||
      normalizedLang.startsWith(`${prefix}-`) ||
      normalizedLang.startsWith(`${prefix}_`) ||
      normalizedLang.includes('arab')
  );
}

const escapeHtml = (value) =>
  value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');


const DEFAULT_I18N_LANG = 'en';

function t(key, fallback = key) {
  return state.i18nDict[key] || fallback;
}

function tf(key, fallback, params = {}) {
  let text = t(key, fallback);
  Object.entries(params).forEach(([paramKey, paramValue]) => {
    text = text.replaceAll(`{${paramKey}}`, String(paramValue));
  });
  return text;
}

async function loadI18n(lang) {
  const normalizedLang = (lang || '').trim().toLowerCase() || DEFAULT_I18N_LANG;
  const response = await fetch(`/i18n/${encodeURIComponent(normalizedLang)}`, { headers: authHeaders() });

  if (response.status === 404 && normalizedLang !== DEFAULT_I18N_LANG) {
    return loadI18n(DEFAULT_I18N_LANG);
  }

  if (!response.ok) {
    throw new Error(`Failed to load translations for ${normalizedLang}.`);
  }

  const dict = await response.json();
  state.i18nLang = normalizedLang;
  state.i18nDict = dict;
}

function clearReaderResizeHandler() {
  if (state.readerResizeHandler) {
    window.removeEventListener('resize', state.readerResizeHandler);
    state.readerResizeHandler = null;
  }
}

function authHeaders() {
  return state.accessCode ? { 'x-access-code': state.accessCode } : {};
}

const api = {
  async getLangs() {
    const res = await fetch('/api/langs', { headers: authHeaders() });
    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));
    if (!res.ok) throw new Error(t('error.loadLanguages', 'Failed to load languages.'));
    return res.json();
  },
  async whoami() {
    const res = await fetch('/api/whoami', { headers: authHeaders() });
    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));
    if (!res.ok) throw new Error(t('error.validateCode', 'Failed to validate access code.'));
    return res.json();
  },
  async getChapters(lang) {
    const res = await fetch(`/api/chapters/${encodeURIComponent(lang)}`, { headers: authHeaders() });
    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));
    if (!res.ok) throw new Error(t('error.loadChapters', 'Failed to load chapters.'));
    return res.json();
  },
  async getContent(lang, file) {
    const res = await fetch(`/api/content/${encodeURIComponent(lang)}/${encodeURIComponent(file)}`, {
      headers: authHeaders(),
    });
    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));
    if (!res.ok) throw new Error(t('error.loadContent', 'Failed to load content.'));
    return res.text();
  },
  async saveContent(lang, file, content) {
    const res = await fetch(`/api/content/${encodeURIComponent(lang)}/${encodeURIComponent(file)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders() },
      body: JSON.stringify({ content }),
    });

    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));
    if (res.status === 403) throw new Error(t('error.archivistRequired', 'Archivist access code required.'));
    if (!res.ok) throw new Error(t('error.saveContent', 'Failed to save content.'));
    return res.json();
  },
  async submitMoreSignup(lang, email) {
    const res = await fetch('/api/more-signups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders() },
      body: JSON.stringify({ lang, email }),
    });

    if (res.status === 401) throw new Error(t('error.unauthorized', 'Unauthorized access code.'));
    if (res.status === 429) throw new Error(t('error.rateLimited', 'Too many attempts. Try again soon.'));

    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
      throw new Error(t('error.unexpectedResponse', 'Unexpected server response.'));
    }

    const body = await res.json();
    if (!res.ok) {
      const localizedErrors = {
        missing_language: t('more.signup.languageRequired', 'Please choose a language first.'),
        signup_rate_limited: t('more.signup.rateLimited', 'Too many signup attempts. Please retry later.'),
        invalid_email: t('more.signup.invalidEmail', 'Please enter a valid email address.'),
        signup_storage_limit: t('more.signup.storageUnavailable', 'Signup storage is temporarily unavailable. Please try again later.'),
      };
      throw new Error(localizedErrors[body.code] || body.error || t('error.submitEmail', 'Failed to submit email.'));
    }
    return body;
  },
};

function setDir(element) {
  if (!state.lang) {
    element.setAttribute('dir', 'auto');
    element.removeAttribute('lang');
    element.classList.remove('is-rtl');
    return;
  }
  const isRtl = isRtlLanguage(state.lang);
  element.setAttribute('dir', isRtl ? 'rtl' : 'auto');
  element.setAttribute('lang', state.lang);
  element.classList.toggle('is-rtl', isRtl);
}

async function renderLogin() {
  clearReaderResizeHandler();
  if (state.editor) {
    state.editor.toTextArea();
    state.editor = null;
  }

  state.mode = null;
  state.lang = null;
  state.file = null;
  state.accessCode = null;

  await loadI18n(DEFAULT_I18N_LANG);

  app.innerHTML = `
    <section class="prompt">
      <h2>${escapeHtml(t('login.title', 'ENTER ACCESS CODE:'))}</h2>
      <form id="login-form" class="access-row">
        <input id="access-code" type="password" autocomplete="off" autofocus />
        <button type="submit">${escapeHtml(t('login.enter', 'ENTER'))}</button>
      </form>
      <p id="login-error" class="status"></p>
    </section>
  `;

  const form = document.getElementById('login-form');
  const input = document.getElementById('access-code');
  const error = document.getElementById('login-error');

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const code = input.value.trim();

    if (!code) {
      error.textContent = t('login.enterCode', 'ENTER A CODE');
      input.select();
      return;
    }

    state.accessCode = code;
    error.textContent = t('login.authorizing', 'AUTHORIZING...');

    try {
      const identity = await api.whoami();
      if (identity.role === 'archivist') {
        state.mode = 'editor';
      } else if (identity.role === 'root') {
        state.mode = 'reader';
      } else {
        throw new Error('Access role not supported.');
      }

      renderLanguageSelection();
    } catch (authError) {
      state.mode = null;
      state.accessCode = null;
      error.textContent = authError.message === t('error.rateLimited', 'Too many attempts. Try again soon.') ? authError.message : t('login.rejected', 'ACCESS CODE REJECTED BY SERVER');
      input.select();
    }
  });
}

async function renderLanguageSelection() {
  clearReaderResizeHandler();
  app.innerHTML = `<p>${escapeHtml(t('loading.languages', 'Loading languages...'))}</p>`;

  try {
    const langs = await api.getLangs();

    app.innerHTML = `
      <h2>${escapeHtml(t('language.select', 'Select Language'))}</h2>
      <div class="selection-grid" id="langs-grid"></div>
      <div class="footer-nav">
        <span class="nav-link" id="back-login">[ &lt; ${escapeHtml(t('nav.back', 'BACK'))} ]</span>
      </div>
    `;

    const grid = document.getElementById('langs-grid');
    langs.forEach((lang) => {
      const node = document.createElement('button');
      node.className = 'option';
      node.textContent = lang;
      node.addEventListener('click', async () => {
        state.lang = lang;
        await loadI18n(lang);
        setDir(document.body);
        renderChapterSelection();
      });
      grid.appendChild(node);
    });

    document.getElementById('back-login').addEventListener('click', () => { renderLogin().catch((error) => { app.innerHTML = `<p>${escapeHtml(error.message)}</p>`; }); });
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function renderChapterSelection() {
  clearReaderResizeHandler();
  app.innerHTML = `<p>${escapeHtml(t('loading.chapters', 'Loading chapters...'))}</p>`;

  try {
    const chapters = await api.getChapters(state.lang);

    app.innerHTML = `
      <h2>${escapeHtml(tf('language.current', 'Language: {lang}', { lang: state.lang }))}</h2>
      <h3>${escapeHtml(t('chapter.select', 'Select Chapter'))}</h3>
      <div class="selection-grid" id="chapters-grid"></div>
      <div class="footer-nav">
        <span class="nav-link" id="back-lang">[ &lt; ${escapeHtml(t('nav.back', 'BACK'))} ]</span>
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

    const moreNode = document.createElement('button');
    moreNode.className = 'option';
    moreNode.textContent = t('chapter.more', 'more');
    moreNode.addEventListener('click', renderMoreSignup);
    grid.appendChild(moreNode);

    document.getElementById('back-lang').addEventListener('click', renderLanguageSelection);
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

function renderMoreSignup() {
  clearReaderResizeHandler();

  app.innerHTML = `
    <h2>${escapeHtml(tf('language.current', 'Language: {lang}', { lang: state.lang }))}</h2>
    <h3>${escapeHtml(t('more.signup.title', 'Want more chapters?'))}</h3>
    <p>${escapeHtml(t('more.signup.description', "Enter your email address and we'll notify you when more chapters are available."))}</p>
    <form id="more-signup-form" class="access-row">
      <input id="more-email" type="email" autocomplete="email" placeholder="${escapeHtml(t('more.signup.placeholder', 'you@example.com'))}" required autofocus />
      <button type="submit">${escapeHtml(t('more.signup.submit', 'Submit'))}</button>
    </form>
    <p id="more-signup-status" class="status"></p>
    <div class="footer-nav">
      <span class="nav-link" id="back-chapters">[ &lt; ${escapeHtml(t('nav.back', 'BACK'))} ]</span>
    </div>
  `;

  const form = document.getElementById('more-signup-form');
  const emailInput = document.getElementById('more-email');
  const status = document.getElementById('more-signup-status');
  const submitButton = form.querySelector('button[type="submit"]');

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = emailInput.value.trim();

    if (!email || !emailInput.checkValidity()) {
      status.textContent = t('more.signup.invalidEmail', 'Please enter a valid email address.');
      emailInput.focus();
      return;
    }

    submitButton.disabled = true;
    status.textContent = t('more.signup.submitting', 'Submitting...');

    try {
      const response = await api.submitMoreSignup(state.lang, email);
      if (response.status === 'already_exists') {
        status.textContent = t('more.signup.alreadyExists', 'You are already on the list for this language.');
      } else {
        status.textContent = t('more.signup.success', 'Thanks! You are on the list for updates.');
        form.reset();
      }
    } catch (error) {
      status.textContent = error.message;
    } finally {
      submitButton.disabled = false;
    }
  });

  document.getElementById('back-chapters').addEventListener('click', renderChapterSelection);
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
  holder.setAttribute('dir', viewport.getAttribute('dir') || 'auto');
  const viewportLang = viewport.getAttribute('lang');
  if (viewportLang) holder.setAttribute('lang', viewportLang);
  document.body.appendChild(holder);

  const source = document.createElement('div');
  source.innerHTML = html;
  const nodes = [...source.childNodes].filter(
    (node) => !(node.nodeType === Node.TEXT_NODE && !node.textContent.trim())
  );

  const pages = [];
  let page = document.createElement('div');

  const commitPage = () => {
    if (page.childNodes.length > 0) {
      pages.push(page.innerHTML);
      page = document.createElement('div');
    }
  };

  nodes.forEach((node) => {
    const clone = node.cloneNode(true);
    const candidate = page.cloneNode(true);
    candidate.appendChild(clone);

    holder.innerHTML = '';
    holder.appendChild(candidate);

    const overflow = holder.scrollHeight > holder.clientHeight;

    if (overflow && page.childNodes.length === 0) {
      page.appendChild(clone);
      commitPage();
      return;
    }

    if (overflow) {
      commitPage();
    }

    page.appendChild(clone);
  });

  commitPage();
  if (pages.length === 0) {
    pages.push('<p></p>');
  }

  document.body.removeChild(holder);
  return pages;
}

async function renderReader() {
  app.innerHTML = `<p>${escapeHtml(t('loading.markdown', 'Loading markdown...'))}</p>`;

  try {
    const markdown = await api.getContent(state.lang, state.file);
    const unsafeHtml = marked.parse(markdown);
    const safeHtml = DOMPurify.sanitize(unsafeHtml);

    app.innerHTML = `
      <h2>${escapeHtml(state.lang)} / ${escapeHtml(state.file)}</h2>
      <section class="terminal-screen" id="reader-screen"></section>
      <div class="footer-nav">
        <span class="nav-link" id="reader-back">[ &lt; ${escapeHtml(t('nav.back', 'BACK'))} ]</span>
        <span id="page-indicator"></span>
        <span class="nav-link" id="reader-next">[ ${escapeHtml(t('nav.next', 'NEXT'))} &gt; ]</span>
      </div>
    `;

    const screen = document.getElementById('reader-screen');
    setDir(screen);

    const renderPages = () => {
      state.pages = buildPagesFromHtml(safeHtml, screen);
      state.pageIndex = Math.min(state.pageIndex, state.pages.length - 1);
      screen.innerHTML = state.pages[state.pageIndex] || '<p></p>';
      indicator.textContent = tf('reader.pageIndicator', 'PAGE {current} / {total}', {
        current: state.pageIndex + 1,
        total: state.pages.length,
      });
    };

    const indicator = document.getElementById('page-indicator');
    state.pageIndex = 0;
    renderPages();

    document.getElementById('reader-back').addEventListener('click', () => {
      if (state.pageIndex > 0) {
        state.pageIndex -= 1;
        screen.innerHTML = state.pages[state.pageIndex] || '<p></p>';
        indicator.textContent = tf('reader.pageIndicator', 'PAGE {current} / {total}', {
          current: state.pageIndex + 1,
          total: state.pages.length,
        });
      } else {
        renderChapterSelection();
      }
    });

    document.getElementById('reader-next').addEventListener('click', () => {
      if (state.pageIndex < state.pages.length - 1) {
        state.pageIndex += 1;
        screen.innerHTML = state.pages[state.pageIndex] || '<p></p>';
        indicator.textContent = tf('reader.pageIndicator', 'PAGE {current} / {total}', {
          current: state.pageIndex + 1,
          total: state.pages.length,
        });
      }
    });

    clearReaderResizeHandler();
    state.readerResizeHandler = () => {
      renderPages();
    };
    window.addEventListener('resize', state.readerResizeHandler);
  } catch (error) {
    app.innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function renderEditor() {
  clearReaderResizeHandler();
  app.innerHTML = `<p>${escapeHtml(t('loading.editor', 'Loading editor...'))}</p>`;

  try {
    const markdown = await api.getContent(state.lang, state.file);

    app.innerHTML = `
      <h2>${escapeHtml(state.lang)} / ${escapeHtml(state.file)}</h2>
      <section class="editor-wrap">
        <textarea id="editor-input"></textarea>
      </section>
      <p class="status" id="editor-status"></p>
      <div class="footer-nav">
        <span class="nav-link" id="editor-back">[ &lt; ${escapeHtml(t('nav.back', 'BACK'))} ]</span>
        <button id="save-btn">[ ${escapeHtml(t('editor.save', 'SAVE TO DISK'))} ]</button>
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
    const editorContainer = wrapper.closest('.EasyMDEContainer');
    setDir(wrapper);
    if (editorContainer) {
      setDir(editorContainer);
    }

    const status = document.getElementById('editor-status');
    const saveButton = document.getElementById('save-btn');

    saveButton.addEventListener('click', async () => {
      saveButton.disabled = true;
      status.textContent = t('editor.saving', 'Saving...');

      try {
        await api.saveContent(state.lang, state.file, state.editor.value());
        status.textContent = t('editor.saved', 'Saved.');
      } catch (error) {
        status.textContent = error.message;
        if (error.message === t('error.unauthorized', 'Unauthorized access code.')) {
          setTimeout(() => {
            renderLogin().catch((error) => { app.innerHTML = `<p>${escapeHtml(error.message)}</p>`; });
          }, 700);
        }
      } finally {
        saveButton.disabled = false;
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

renderLogin().catch((error) => { app.innerHTML = `<p>${escapeHtml(error.message)}</p>`; });
