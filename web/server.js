const path = require('path');
const fs = require('fs');
const http = require('http');
const express = require('express');
const WebSocket = require('ws');
const pty = require('node-pty');

const app = express();
const port = Number(process.env.WEB_PORT || 3000);
const sshHost = process.env.SSH_HOST || '127.0.0.1';
const sshPort = process.env.SSH_PORT || '2222';
const gatewayBaseUrl = (process.env.GATEWAY_BASE_URL || 'http://127.0.0.1:8080').replace(/\/$/, '');
const gatewayTargetHost = process.env.GATEWAY_TARGET_HOST || sshHost;
const gatewayTargetPort = Number(process.env.GATEWAY_TARGET_PORT || sshPort || 22);

const commonSshPaths = ['/usr/bin/ssh', '/bin/ssh', '/usr/local/bin/ssh'];
const configuredSshPath = process.env.SSH_BIN;
const sshCommand = configuredSshPath && configuredSshPath.trim()
  ? configuredSshPath.trim()
  : commonSshPaths.find((candidate) => fs.existsSync(candidate)) || 'ssh';

const homeDirectory = process.env.HOME;
const shellCwd = homeDirectory && fs.existsSync(homeDirectory)
  ? homeDirectory
  : process.cwd();

function shellEscape(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function buildGatewayBaseUrlCandidates(baseUrl) {
  const candidates = [baseUrl];

  let parsed;
  try {
    parsed = new URL(baseUrl);
  } catch {
    return candidates;
  }

  if (parsed.hostname === '127.0.0.1') {
    parsed.hostname = 'localhost';
    candidates.push(parsed.toString().replace(/\/$/, ''));
  } else if (parsed.hostname === 'localhost') {
    parsed.hostname = '127.0.0.1';
    candidates.push(parsed.toString().replace(/\/$/, ''));
  }

  return candidates;
}

function describeGatewayFetchError(error) {
  if (!error) {
    return 'failed to contact gateway';
  }

  if (error.cause && error.cause.code) {
    return `${error.message} (${error.cause.code})`;
  }

  return error.message || 'failed to contact gateway';
}

function summarizeGatewayRequest(init) {
  return {
    method: (init && init.method) || 'GET',
    hasBody: Boolean(init && init.body),
    headers: init && init.headers ? Object.keys(init.headers) : [],
  };
}

function formatGatewayAttemptDiagnostics(attempts) {
  if (!Array.isArray(attempts) || attempts.length === 0) {
    return 'no gateway request attempts recorded';
  }

  return attempts.map((attempt) => {
    if (attempt.ok) {
      return `${attempt.url} -> HTTP ${attempt.status} (${attempt.durationMs}ms)`;
    }

    return `${attempt.url} -> ERROR ${attempt.error} (${attempt.durationMs}ms)`;
  }).join('; ');
}

async function fetchGateway(pathname, init) {
  const attempts = [];
  const baseUrlCandidates = buildGatewayBaseUrlCandidates(gatewayBaseUrl);
  const requestSummary = summarizeGatewayRequest(init);

  console.log(`[gateway] request ${requestSummary.method} ${pathname} candidates=${baseUrlCandidates.join(',')} body=${requestSummary.hasBody} headers=${requestSummary.headers.join(',') || '(none)'}`);

  for (const baseUrlCandidate of baseUrlCandidates) {
    const url = `${baseUrlCandidate}${pathname}`;
    const startedAt = Date.now();

    try {
      const response = await fetch(url, init);
      const durationMs = Date.now() - startedAt;
      attempts.push({ url, ok: true, status: response.status, durationMs });
      console.log(`[gateway] response ${url} status=${response.status} durationMs=${durationMs}`);
      response.gatewayDiagnostics = { attempts };
      return response;
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      const errorDescription = describeGatewayFetchError(error);
      attempts.push({ url, ok: false, error: errorDescription, durationMs });
      console.error(`[gateway] request failure ${url} durationMs=${durationMs} error=${errorDescription}`);
    }
  }

  const requestError = new Error(`gateway request failed (${formatGatewayAttemptDiagnostics(attempts)})`);
  requestError.gatewayDiagnostics = { attempts };
  throw requestError;
}

async function openGatewaySession(username) {
  const response = await fetchGateway('/gateway/sessions', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      user: username,
      host: gatewayTargetHost,
      port: gatewayTargetPort,
    }),
  });

  let payload;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    const message = payload && payload.error && payload.error.message
      ? payload.error.message
      : `gateway open session failed with status ${response.status}`;
    throw new Error(message);
  }

  if (payload && response.gatewayDiagnostics) {
    payload.gateway_diagnostics = response.gatewayDiagnostics;
  }

  return payload;
}

function buildGatewayDiagnostics(error) {
  const diagnostics = [
    `gatewayBaseUrl=${gatewayBaseUrl}`,
    `gatewayCandidates=${buildGatewayBaseUrlCandidates(gatewayBaseUrl).join(',')}`,
    `targetHost=${gatewayTargetHost}`,
    `targetPort=${gatewayTargetPort}`,
  ];

  if (error && error.gatewayDiagnostics && error.gatewayDiagnostics.attempts) {
    diagnostics.push(`attempts=${formatGatewayAttemptDiagnostics(error.gatewayDiagnostics.attempts)}`);
  }

  return diagnostics.join(', ');
}

async function closeGatewaySession(sessionId, resumeToken) {
  if (!sessionId || !resumeToken) {
    return;
  }

  await fetchGateway(`/gateway/sessions/${encodeURIComponent(sessionId)}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${resumeToken}` },
  });
}

function launchSshPty(username) {
  const sshArgs = [`${username}@${sshHost}`, '-p', `${sshPort}`];

  try {
    return {
      shell: pty.spawn(sshCommand, sshArgs, {
        name: 'xterm-color',
        cols: 120,
        rows: 36,
        cwd: shellCwd,
        env: process.env,
      }),
      mode: 'direct',
    };
  } catch (directError) {
    const shellBinary = process.env.SHELL || '/bin/sh';
    const shellCommand = `exec ${shellEscape(sshCommand)} ${sshArgs.map(shellEscape).join(' ')}`;

    try {
      return {
        shell: pty.spawn(shellBinary, ['-lc', shellCommand], {
          name: 'xterm-color',
          cols: 120,
          rows: 36,
          cwd: shellCwd,
          env: process.env,
        }),
        mode: 'shell-fallback',
      };
    } catch (fallbackError) {
      fallbackError.previousError = directError;
      throw fallbackError;
    }
  }
}

function buildSshLaunchDiagnostics(error, username) {
  const diagnostics = [];
  const sshCommandIsAbsolutePath = path.isAbsolute(sshCommand);
  const sshPathExists = sshCommandIsAbsolutePath ? fs.existsSync(sshCommand) : null;
  const cwdExists = fs.existsSync(shellCwd);

  diagnostics.push(`sshCommand=${sshCommand}`);
  diagnostics.push(`sshCommandIsAbsolutePath=${sshCommandIsAbsolutePath}`);
  if (sshCommandIsAbsolutePath) {
    diagnostics.push(`sshPathExists=${sshPathExists}`);
  }
  diagnostics.push(`cwd=${shellCwd}`);
  diagnostics.push(`cwdExists=${cwdExists}`);
  diagnostics.push(`home=${homeDirectory || '(unset)'}`);
  diagnostics.push(`path=${process.env.PATH || '(unset)'}`);
  diagnostics.push(`username=${username}`);

  if (error && error.previousError && error.previousError.message) {
    diagnostics.push(`directSpawnError=${error.previousError.message}`);
  }

  const reason = error && error.message ? error.message : 'Unable to launch ssh process.';
  return `Failed to start SSH session: ${reason}. Diagnostics: ${diagnostics.join(', ')}`;
}

app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/terminal' });

wss.on('connection', (ws) => {
  let shell;
  let gatewaySession;

  async function finalizeGatewaySession() {
    if (!gatewaySession) {
      return;
    }

    const sessionToClose = gatewaySession;
    gatewaySession = null;

    await closeGatewaySession(sessionToClose.session_id, sessionToClose.resume_token);
  }

  ws.on('message', async (rawMessage) => {
    const text = rawMessage.toString();
    let message;

    try {
      message = JSON.parse(text);
    } catch {
      return;
    }

    if (message.type === 'start') {
      const username = (message.username || '').trim();
      if (!username || /\s/.test(username)) {
        ws.send(JSON.stringify({ type: 'error', payload: 'A valid SSH username is required.' }));
        return;
      }

      if (shell) {
        return;
      }

      try {
        gatewaySession = await openGatewaySession(username);
      } catch (error) {
        const reason = error && error.message ? error.message : 'failed to contact gateway';
        const gatewayDiagnostics = buildGatewayDiagnostics(error);
        console.error(`[gateway] unable to open session for user=${username}: ${reason}; ${gatewayDiagnostics}`);
        ws.send(JSON.stringify({ type: 'error', payload: `Unable to open gateway session: ${reason}. Diagnostics: ${gatewayDiagnostics}` }));
        return;
      }

      try {
        const launchResult = launchSshPty(username);
        shell = launchResult.shell;

        ws.send(JSON.stringify({
          type: 'output',
          payload: `[Gateway session ${gatewaySession.session_id || 'created'}]\\r\\n`,
        }));

        if (gatewaySession.gateway_diagnostics && gatewaySession.gateway_diagnostics.attempts) {
          ws.send(JSON.stringify({
            type: 'output',
            payload: `[Gateway diagnostics: ${formatGatewayAttemptDiagnostics(gatewaySession.gateway_diagnostics.attempts)}]\\r\\n`,
          }));
        }

        if (launchResult.mode === 'shell-fallback') {
          ws.send(JSON.stringify({
            type: 'output',
            payload: '[Direct PTY launch failed; using shell fallback]\\r\\n',
          }));
        }
      } catch (error) {
        const diagnosticError = buildSshLaunchDiagnostics(error, username);
        console.error(diagnosticError);
        ws.send(JSON.stringify({ type: 'error', payload: diagnosticError }));
        await finalizeGatewaySession();
        return;
      }

      shell.onData((data) => {
        ws.send(JSON.stringify({ type: 'output', payload: data }));
      });

      shell.onExit(async ({ exitCode }) => {
        ws.send(JSON.stringify({
          type: 'output',
          payload: `\\r\\n\\r\\n[SSH session ended with code ${exitCode}]\\r\\n`,
        }));
        await finalizeGatewaySession();
        ws.close();
      });

      return;
    }

    if (message.type === 'input' && shell) {
      shell.write(message.payload);
      return;
    }

    if (message.type === 'resize' && shell) {
      const cols = Number(message.cols);
      const rows = Number(message.rows);
      if (Number.isInteger(cols) && Number.isInteger(rows) && cols > 0 && rows > 0) {
        shell.resize(cols, rows);
      }
    }
  });

  ws.on('close', async () => {
    if (shell) {
      shell.kill();
      shell = null;
    }

    await finalizeGatewaySession();
  });
});

server.listen(port, () => {
  console.log(`Web terminal listening on http://0.0.0.0:${port}`);
  console.log(`SSH target: ${sshHost}:${sshPort}`);
  console.log(`Gateway API: ${gatewayBaseUrl}`);
  console.log(`Gateway SSH target: ${gatewayTargetHost}:${gatewayTargetPort}`);
  console.log(`SSH binary: ${sshCommand}`);
  console.log(`Shell cwd: ${shellCwd}`);
});
