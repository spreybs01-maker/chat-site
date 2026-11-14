const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const page = fs.readFileSync(path.join(__dirname, "index.html"));

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    return res.end("ok");
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(page);
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> { clients: Set<ws>, history: [] }
const MAX_PER_ROOM = 20;
const HISTORY = 50;

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj) {
  for (const c of room.clients) send(c, obj);
}
function sendUsers(room) {
  broadcast(room, { type: "users", list: [...room.clients].map((c) => c.userName) });
}

wss.on("connection", (ws) => {
  ws.room = null;
  ws.isAlive = true;
  ws.lastMsg = 0;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === "join" && !ws.room) {
      const id = String(m.room || "");
      if (!/^[a-z0-9-]{1,32}$/i.test(id)) return;
      if (!rooms.has(id)) rooms.set(id, { clients: new Set(), history: [] });
      const room = rooms.get(id);
      if (room.clients.size >= MAX_PER_ROOM) return send(ws, { type: "full" });
      ws.userName = String(m.name || "Аноним").trim().slice(0, 24) || "Аноним";
      ws.room = room;
      ws.roomId = id;
      room.clients.add(ws);
      send(ws, { type: "history", list: room.history });
      sendUsers(room);
    }

    if (m.type === "msg" && ws.room) {
      const now = Date.now();
      if (now - ws.lastMsg < 400) return; // защита от спама
      ws.lastMsg = now;
      const text = String(m.text || "").trim().slice(0, 1000);
      if (!text) return;
      const msg = { type: "msg", name: ws.userName, text, t: now };
      ws.room.history.push(msg);
      if (ws.room.history.length > HISTORY) ws.room.history.shift();
      broadcast(ws.room, msg);
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;
    room.clients.delete(ws);
    if (room.clients.size === 0) rooms.delete(ws.roomId); // пустая комната удаляется вместе с историей
    else sendUsers(room);
  });
});

// пинг, чтобы соединения не обрывались
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log("Чат запущен на порту " + PORT));
