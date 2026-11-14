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

  // Главная страница и ссылки вида /room/abc123 обслуживаются одним приложением.
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

const MAX_IMAGE_DATA = 1900000; // лимит data URL на одно фото
const wss = new WebSocketServer({ server, maxPayload: 2 * 1024 * 1024 });
const rooms = new Map(); // roomId -> { clients: Set<ws>, history: [] }
const MAX_PER_ROOM = 20;
const HISTORY = 50;
const REACTION_SET = new Set(["❤️", "😂", "👍", "✨"]);

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
  ws.userName = "";
  ws.isTyping = false;
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
      const msg = {
        type: "msg", id: String(m.id || (now.toString(36) + Math.random().toString(36).slice(2, 8))),
        name: ws.userName, text, t: now,
        reply: m.reply && typeof m.reply === "object" ? {
          id: String(m.reply.id || "").slice(0, 80),
          name: String(m.reply.name || "").slice(0, 24),
          text: String(m.reply.text || "").slice(0, 180)
        } : null,
        reactions: {}
      };
      ws.room.history.push(msg);
      if (ws.room.history.length > HISTORY) ws.room.history.shift();
      broadcast(ws.room, msg);
    }

    // Фото: принимаем только JPEG data URL ограниченного размера.
    // Клиент предварительно уменьшает изображение и конвертирует его в JPEG.
    if (m.type === "img" && ws.room) {
      const now = Date.now();
      if (now - ws.lastMsg < 400) return; // общий лимит частоты сообщений
      ws.lastMsg = now;

      if (typeof m.data !== "string" || m.data.length > MAX_IMAGE_DATA) return;
      if (!/^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/.test(m.data)) return;

      const msg = {
        type: "msg", id: now.toString(36) + Math.random().toString(36).slice(2, 8),
        name: ws.userName, img: m.data, t: now,
        reply: m.reply && typeof m.reply === "object" ? {
          id: String(m.reply.id || "").slice(0, 80),
          name: String(m.reply.name || "").slice(0, 24),
          text: String(m.reply.text || "").slice(0, 180)
        } : null,
        reactions: {}
      };
      ws.room.history.push(msg);
      if (ws.room.history.length > HISTORY) ws.room.history.shift();
      broadcast(ws.room, msg);
    }

    if (m.type === "react" && ws.room) {
      const id = String(m.id || "").slice(0, 80);
      const emoji = String(m.emoji || "");
      if (!id || !REACTION_SET.has(emoji)) return;
      const target = ws.room.history.find((item) => item.id === id);
      if (!target) return;
      if (!target.reactions) target.reactions = {};
      if (!Array.isArray(target.reactions[emoji])) target.reactions[emoji] = [];
      const users = target.reactions[emoji];
      const index = users.indexOf(ws.userName);
      if (index === -1) users.push(ws.userName);
      else users.splice(index, 1);
      if (!users.length) delete target.reactions[emoji];
      broadcast(ws.room, { type: "react", id, message: target });
    }

    if (m.type === "typing" && ws.room) {
      const active = !!m.active;
      if (ws.isTyping === active) return;
      ws.isTyping = active;
      for (const client of ws.room.clients) {
        if (client !== ws) send(client, { type: "typing", name: ws.userName, active });
      }
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;
    if (ws.isTyping) {
      for (const client of room.clients) {
        if (client !== ws) send(client, { type: "typing", name: ws.userName, active: false });
      }
    }
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
