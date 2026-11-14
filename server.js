
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const indexPage = fs.readFileSync(path.join(__dirname, "index.html"));
const spacePage = fs.readFileSync(path.join(__dirname, "space-background.html"));

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, "http://localhost").pathname;

  if (pathname === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("ok");
  }

  if (pathname === "/space-background.html") {
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=300"
    });
    return res.end(spacePage);
  }

  if (pathname === "/" || /^\/room\/[a-z0-9-]{1,32}\/?$/i.test(pathname)) {
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-cache"
    });
    return res.end(indexPage);
  }

  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

const wss = new WebSocketServer({ server });
const rooms = new Map();
const MAX_PER_ROOM = 20;
const HISTORY = 50;

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj) {
  for (const c of room.clients) send(c, obj);
}

function sendUsers(room) {
  broadcast(room, {
    type: "users",
    list: [...room.clients].map((c) => c.userName)
  });
}

wss.on("connection", (ws) => {
  ws.room = null;
  ws.isAlive = true;
  ws.lastMsg = 0;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }

    if (m.type === "join" && !ws.room) {
      const id = String(m.room || "");
      if (!/^[a-z0-9-]{1,32}$/i.test(id)) return;

      if (!rooms.has(id)) {
        rooms.set(id, { clients: new Set(), history: [] });
      }

      const room = rooms.get(id);
      if (room.clients.size >= MAX_PER_ROOM) {
        return send(ws, { type: "full" });
      }

      ws.userName = String(m.name || "Аноним").trim().slice(0, 24) || "Аноним";
      ws.room = room;
      ws.roomId = id;
      room.clients.add(ws);

      send(ws, { type: "history", list: room.history });
      sendUsers(room);
    }

    if (m.type === "msg" && ws.room) {
      const now = Date.now();
      if (now - ws.lastMsg < 400) return;
      ws.lastMsg = now;

      const text = String(m.text || "").trim().slice(0, 1000);
      if (!text) return;

      const msg = {
        type: "msg",
        name: ws.userName,
        text,
        t: now
      };

      ws.room.history.push(msg);
      if (ws.room.history.length > HISTORY) {
        ws.room.history.shift();
      }

      broadcast(ws.room, msg);
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;

    room.clients.delete(ws);

    if (room.clients.size === 0) {
      rooms.delete(ws.roomId);
    } else {
      sendUsers(room);
    }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log("Чат запущен на порту " + PORT);
});
