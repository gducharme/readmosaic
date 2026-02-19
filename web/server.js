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

const commonSshPaths = ['/usr/bin/ssh', '/bin/ssh', '/usr/local/bin/ssh'];
const configuredSshPath = process.env.SSH_BIN;
const sshCommand = configuredSshPath && configuredSshPath.trim()
  ? configuredSshPath.trim()
  : commonSshPaths.find((candidate) => fs.existsSync(candidate)) || 'ssh';

const homeDirectory = process.env.HOME;
const shellCwd = homeDirectory && fs.existsSync(homeDirectory)
  ? homeDirectory
  : process.cwd();

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

  const reason = error && error.message ? error.message : 'Unable to launch ssh process.';
  return `Failed to start SSH session: ${reason}. Diagnostics: ${diagnostics.join(', ')}`;
}

app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/terminal' });

wss.on('connection', (ws) => {
  let shell;

  ws.on('message', (rawMessage) => {
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
        shell = pty.spawn(sshCommand, [`${username}@${sshHost}`, '-p', `${sshPort}`], {
          name: 'xterm-color',
          cols: 120,
          rows: 36,
          cwd: shellCwd,
          env: process.env,
        });
      } catch (error) {
        const diagnosticError = buildSshLaunchDiagnostics(error, username);
        console.error(diagnosticError);
        ws.send(JSON.stringify({ type: 'error', payload: diagnosticError }));
        return;
      }

      shell.onData((data) => {
        ws.send(JSON.stringify({ type: 'output', payload: data }));
      });

      shell.onExit(({ exitCode }) => {
        ws.send(JSON.stringify({
          type: 'output',
          payload: `\r\n\r\n[SSH session ended with code ${exitCode}]\r\n`,
        }));
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

  ws.on('close', () => {
    if (shell) {
      shell.kill();
      shell = null;
    }
  });
});

server.listen(port, () => {
  console.log(`Web terminal listening on http://0.0.0.0:${port}`);
  console.log(`SSH target: ${sshHost}:${sshPort}`);
  console.log(`SSH binary: ${sshCommand}`);
  console.log(`Shell cwd: ${shellCwd}`);
});
