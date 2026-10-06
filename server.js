const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

app.get("/", (_req, res) => {
  res.json({
    service: "Cybs Test Server",
    status: "ok",
    websocket: "/ws"
  });
});

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "cybs-test-server",
    time: new Date().toISOString()
  });
});

let nextClientId = 1;
const clients = new Map();

wss.on("connection", (ws) => {
  const clientId = String(nextClientId++);

  clients.set(ws, {
    clientId,
    userId: null
  });

  send(ws, {
    type: "connected",
    clientId
  });

  ws.on("message", (raw) => {
    try {
      const message = JSON.parse(raw.toString());

      if (message.type === "identify") {
        const userId = String(message.userId || "").trim();

        if (!userId) {
          send(ws, {
            type: "error",
            message: "userId is required"
          });
          return;
        }

        clients.get(ws).userId = userId;

        send(ws, {
          type: "identified",
          userId
        });

        return;
      }

      if (message.type === "chat") {
        const text = String(message.text || "").trim();

        if (!text) {
          send(ws, {
            type: "error",
            message: "text is required"
          });
          return;
        }

        const sender = clients.get(ws);

        broadcast({
          type: "chat",
          messageId: randomId(),
          senderId: sender?.userId || sender?.clientId,
          text,
          timestamp: new Date().toISOString()
        });

        return;
      }

      send(ws, {
        type: "error",
        message: "Unknown message type"
      });

    } catch (_error) {
      send(ws, {
        type: "error",
        message: "Invalid JSON"
      });
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
  });
});

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const encoded = JSON.stringify(payload);

  for (const ws of clients.keys()) {
    if (ws.readyState === ws.OPEN) {
      ws.send(encoded);
    }
  }
}

function randomId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Cybs test server listening on port ${PORT}`);
});
