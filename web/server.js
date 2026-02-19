const path = require('path');
const http = require('http');
const express = require('express');
const WebSocket = require('ws');
const pty = require('node-pty');

const app = express();
const port = Number(process.env.WEB_PORT || 3000);
const sshHost = process.env.SSH_HOST || '127.0.0.1';
const sshPort = process.env.SSH_PORT || '2222';

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

      shell = pty.spawn('ssh', [`${username}@${sshHost}`, '-p', `${sshPort}`], {
        name: 'xterm-color',
        cols: 120,
        rows: 36,
        cwd: process.env.HOME,
        env: process.env,
      });

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
});
