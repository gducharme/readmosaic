const http = require('http');
const express = require('express');
const WebSocket = require('ws');

const app = express();
const port = Number(process.env.WEB_PORT || 3000);
const sshHost = process.env.SSH_HOST || '127.0.0.1';
const sshPort = process.env.SSH_PORT || '2222';
const gatewayBaseUrl = (process.env.GATEWAY_BASE_URL || 'http://127.0.0.1:8080').replace(/\/$/, '');
const gatewayTargetHost = process.env.GATEWAY_TARGET_HOST || sshHost;
const gatewayTargetPort = Number(process.env.GATEWAY_TARGET_PORT || sshPort || 22);
const gatewayFatalStatuses = new Set([401, 403, 404, 410]);
const maxGatewayOperationFailures = 5;

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



async function streamGatewayOutput(session, ws) {
  if (!session || !session.session_id || !session.resume_token) {
    return;
  }

  let response;
  try {
    response = await fetchGateway(`/gateway/sessions/${encodeURIComponent(session.session_id)}/output`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${session.resume_token}` },
    });
  } catch (error) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'error', payload: `Unable to stream gateway output: ${describeGatewayFetchError(error)}` }));
    }
    return;
  }

  if (!response.ok || !response.body) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'error', payload: `Unable to stream gateway output: HTTP ${response.status}` }));
    }
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (ws.readyState === WebSocket.OPEN) {
    const { done, value } = await reader.read();
    if (done) {
      return;
    }
    buffer += decoder.decode(value, { stream: true });

    for (;;) {
      const frameEnd = buffer.indexOf("\n\n");
      if (frameEnd === -1) {
        break;
      }
      const frame = buffer.slice(0, frameEnd);
      buffer = buffer.slice(frameEnd + 2);
      const lines = frame.split("\n");
      let eventType = '';
      let data = '';
      for (const line of lines) {
        if (line.startsWith('event:')) {
          eventType = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          data += line.slice(5).trim();
        }
      }
      if (eventType === 'output' && data && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'output', payload: data, encoding: 'base64' }));
      }
    }
  }

  try {
    await reader.cancel();
  } catch (error) {
    const reason = error && error.message ? error.message : 'unknown error';
    console.debug(`[gateway] output stream cancel failed for session ${session.session_id}: ${reason}`);
  }

}

async function closeGatewaySession(sessionId, resumeToken) {
  if (!sessionId || !resumeToken) {
    return;
  }

  try {
    const response = await fetchGateway(`/gateway/sessions/${encodeURIComponent(sessionId)}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${resumeToken}` },
    });

    if (!response.ok) {
      console.warn(`[gateway] unable to close session ${sessionId}: HTTP ${response.status}`);
    }
  } catch (error) {
    const reason = error && error.message ? error.message : 'unknown error';
    console.warn(`[gateway] unable to close session ${sessionId}: ${reason}`);
  }
}

function buildGatewayIoDiagnostics(error, username) {
  const diagnostics = [];
  diagnostics.push(`gatewayBaseUrl=${gatewayBaseUrl}`);
  diagnostics.push(`targetHost=${gatewayTargetHost}`);
  diagnostics.push(`targetPort=${gatewayTargetPort}`);
  diagnostics.push(`username=${username}`);

  if (error && error.code) {
    diagnostics.push(`errorCode=${error.code}`);
  }

  if (error && error.errno) {
    diagnostics.push(`spawnErrno=${error.errno}`);
  }

  if (error && error.syscall) {
    diagnostics.push(`spawnSyscall=${error.syscall}`);
  }

  const reason = error && error.message ? error.message : 'Unable to send terminal data through gateway.';
  return `Gateway terminal I/O failed: ${reason}. Diagnostics: ${diagnostics.join(', ')}`;
}

function extractGatewayErrorMessage(payload) {
  if (!payload || typeof payload !== 'object') {
    return '';
  }

  if (payload.error && typeof payload.error.message === 'string') {
    return payload.error.message;
  }

  if (typeof payload.message === 'string') {
    return payload.message;
  }

  return '';
}

async function parseGatewayErrorResponse(response, fallbackMessage) {
  let payload;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  const structured = extractGatewayErrorMessage(payload);
  if (structured) {
    return structured;
  }

  return fallbackMessage;
}

function isFatalGatewayStatus(status) {
  return gatewayFatalStatuses.has(status);
}

app.use(express.static(`${__dirname}/public`));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/terminal' });

wss.on('connection', (ws) => {
  let gatewaySession;
  let outputReaderPromise = null;
  let finalizePromise = null;
  let gatewayFailureCount = 0;
  let lastGatewayErrorKey = '';
  let lastGatewayErrorRepeat = 0;

  async function finalizeGatewaySession() {
    if (finalizePromise) {
      return finalizePromise;
    }

    finalizePromise = (async () => {
      const sessionToClose = gatewaySession;
      gatewaySession = null;

      if (sessionToClose) {
        await closeGatewaySession(sessionToClose.session_id, sessionToClose.resume_token);
      }
      if (outputReaderPromise) {
        try {
          await outputReaderPromise;
        } catch {
        }
        outputReaderPromise = null;
      }
    })();

    return finalizePromise;
  }

  function noteGatewayFailure(errorKey) {
    gatewayFailureCount += 1;
    if (errorKey === lastGatewayErrorKey) {
      lastGatewayErrorRepeat += 1;
      return;
    }

    lastGatewayErrorKey = errorKey;
    lastGatewayErrorRepeat = 1;
  }

  function resetGatewayFailureTracking() {
    gatewayFailureCount = 0;
    lastGatewayErrorKey = '';
    lastGatewayErrorRepeat = 0;
  }

  async function handleGatewayActionFailure(response, actionLabel, fallbackMessage) {
    const message = await parseGatewayErrorResponse(response, fallbackMessage);
    const errorKey = `${actionLabel}:${response.status}:${message}`;
    noteGatewayFailure(errorKey);

    if (ws.readyState === WebSocket.OPEN && (lastGatewayErrorRepeat <= 3 || lastGatewayErrorRepeat % 10 === 0)) {
      ws.send(JSON.stringify({ type: 'error', payload: `${actionLabel} failed: ${message} (HTTP ${response.status})` }));
    }

    if (isFatalGatewayStatus(response.status) || gatewayFailureCount >= maxGatewayOperationFailures) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', payload: 'Gateway session is no longer valid; closing terminal session.' }));
      }
      await finalizeGatewaySession();
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    }
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

      if (gatewaySession) {
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

      resetGatewayFailureTracking();
      outputReaderPromise = streamGatewayOutput(gatewaySession, ws);
      return;
    }

    if (message.type === 'input' && gatewaySession) {
      const payload = Buffer.from(String(message.payload || ''), 'utf8').toString('base64');
      try {
        const response = await fetchGateway(`/gateway/sessions/${encodeURIComponent(gatewaySession.session_id)}/stdin`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${gatewaySession.resume_token}`,
            'content-type': 'application/json',
          },
          body: JSON.stringify({ data: payload }),
        });

        if (!response.ok) {
          await handleGatewayActionFailure(response, 'Gateway terminal input', 'gateway rejected terminal input');
        } else {
          resetGatewayFailureTracking();
        }
      } catch (error) {
        const diagnosticError = buildGatewayIoDiagnostics(error, gatewaySession.user || 'unknown');
        ws.send(JSON.stringify({ type: 'error', payload: diagnosticError }));
      }
      return;
    }

    if (message.type === 'resize' && gatewaySession) {
      const cols = Number(message.cols);
      const rows = Number(message.rows);
      if (Number.isInteger(cols) && Number.isInteger(rows) && cols > 0 && rows > 0) {
        try {
          const response = await fetchGateway(`/gateway/sessions/${encodeURIComponent(gatewaySession.session_id)}/resize`, {
            method: 'POST',
            headers: {
              Authorization: `Bearer ${gatewaySession.resume_token}`,
              'content-type': 'application/json',
            },
            body: JSON.stringify({ cols, rows }),
          });

          if (!response.ok) {
            await handleGatewayActionFailure(response, 'Gateway resize', 'gateway rejected resize request');
          } else {
            resetGatewayFailureTracking();
          }
        } catch (error) {
          const diagnosticError = buildGatewayIoDiagnostics(error, gatewaySession.user || 'unknown');
          ws.send(JSON.stringify({ type: 'error', payload: diagnosticError }));
        }
      }
    }
  });

  ws.on('close', async () => {
    await finalizeGatewaySession();
  });

  ws.on('error', async () => {
    await finalizeGatewaySession();
  });
});

server.listen(port, () => {
  console.log(`Web terminal listening on http://0.0.0.0:${port}`);
  console.log(`SSH target: ${sshHost}:${sshPort}`);
  console.log(`Gateway API: ${gatewayBaseUrl}`);
  console.log(`Gateway SSH target: ${gatewayTargetHost}:${gatewayTargetPort}`);
  console.log('Local SSH spawning: disabled (gateway-managed)');
});
