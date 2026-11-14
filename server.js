"use strict";

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 3000);
const publicPath = path.join(__dirname, "public");
const publicDir = fs.existsSync(path.join(publicPath, "index.html")) ? publicPath : __dirname;

const io = new Server(server, {
  cors: { origin: "*" },
  transports: ["websocket", "polling"],
  pingInterval: 25000,
  pingTimeout: 60000,
  maxHttpBufferSize: 1e6
});

app.use(express.json({ limit: "100kb" }));

const players = new Map();
const rooms = new Map();
const waitingQueue = [];
const pendingChallenges = new Map();

const MATCH_DURATION = 240;
const START_HP = 1000;
const START_MANA = 50;
const MAX_POSITION = 100000;
const MAX_NAME_LENGTH = 24;

function cleanName(value) {
  const name = String(value && typeof value === "object"
    ? value.name || value.username || ""
    : value || "").trim().replace(/[<>]/g, "").slice(0, MAX_NAME_LENGTH);
  return name || "Játékos";
}

function cleanRoomName(value) {
  return String(value || "Játékosszoba")
    .trim().replace(/[<>]/g, "").slice(0, 32) || "Játékosszoba";
}

function makeId() {
  return crypto.randomBytes(8).toString("hex");
}

function makeCode() {
  return crypto.randomBytes(3).toString("hex").toUpperCase();
}

function roomInfo(room) {
  return {
    id: room.id,
    name: room.name,
    code: room.code,
    private: room.private,
    members: room.members.map(id => ({
      id,
      name: players.get(id)?.name || "Játékos",
      ready: room.ready.has(id)
    })),
    hostId: room.hostId,
    status: room.status,
    createdAt: room.createdAt
  };
}

function roomList() {
  return [...rooms.values()]
    .filter(room => !room.private && room.status === "waiting")
    .map(room => roomInfo(room));
}

function broadcastRooms() {
  io.emit("rooms:update", roomList());
}

function broadcastPlayers() {
  io.emit("players:update", [...players.entries()].map(([id, p]) => ({
    id,
    name: p.name,
    inMatch: Boolean(p.roomId)
  })));
}

function removeFromQueue(id) {
  let index;
  while ((index = waitingQueue.indexOf(id)) !== -1) {
    waitingQueue.splice(index, 1);
  }
}

function createRoomObject(name, hostId, isPrivate = false, password = "") {
  const id = makeId();
  const room = {
    id,
    name: cleanRoomName(name),
    code: makeCode(),
    private: Boolean(isPrivate),
    password: isPrivate ? String(password || "") : "",
    hostId,
    members: [hostId],
    ready: new Set(),
    status: "waiting",
    createdAt: Date.now(),
    match: null
  };

  rooms.set(id, room);
  const player = players.get(hostId);
  if (player) player.roomId = id;

  io.sockets.sockets.get(hostId)?.join(id);
  return room;
}

function updateRoom(room) {
  if (!room) return;
  io.to(room.id).emit("room:update", roomInfo(room));
  broadcastRooms();
}

function removeRoom(room) {
  if (!room) return;

  for (const id of room.members) {
    const player = players.get(id);
    if (player) player.roomId = null;
    io.sockets.sockets.get(id)?.leave(room.id);
    io.to(id).emit("room:left", { message: "A szoba megszűnt." });
  }

  rooms.delete(room.id);
  broadcastRooms();
  broadcastPlayers();
}

function leaveRoom(id) {
  const player = players.get(id);
  if (!player || !player.roomId) return;

  const room = rooms.get(player.roomId);
  if (!room) {
    player.roomId = null;
    return;
  }

  if (room.status === "playing") {
    io.to(room.id).emit("match:ended", {
      message: "A másik játékos kilépett."
    });
  }

  room.members = room.members.filter(member => member !== id);
  room.ready.delete(id);
  player.roomId = null;
  io.sockets.sockets.get(id)?.leave(room.id);

  if (!room.members.length || room.hostId === id) {
    removeRoom(room);
    return;
  }

  room.hostId = room.members[0];
  room.status = "waiting";
  room.match = null;
  room.ready.clear();
  updateRoom(room);
  broadcastPlayers();
}

function startRoom(room) {
  if (!room || room.status !== "waiting" || room.members.length !== 2) return;
  if (!room.members.every(id => room.ready.has(id))) return;

  room.status = "playing";
  room.match = {
    startedAt: Date.now(),
    duration: MATCH_DURATION,
    hp: Object.fromEntries(room.members.map(id => [id, START_HP])),
    mana: Object.fromEntries(room.members.map(id => [id, START_MANA]))
  };

  const data = {
    roomId: room.id,
    players: room.members.map(id => ({
      id,
      name: players.get(id)?.name || "Játékos"
    })),
    duration: MATCH_DURATION,
    hp: START_HP,
    mana: START_MANA
  };

  io.to(room.id).emit("room:match-started", data);
  // Mindkét kliens megkapja a közös szoba játékoslistáját, így egymást is megjeleníthetik.
  io.to(room.id).emit("player:presence", {
    roomId: room.id,
    players: room.members.map(id => ({
      id,
      name: players.get(id)?.name || "Játékos",
      position: players.get(id)?.position || { x: 0, y: 0, angle: 0 }
    }))
  });
  // A gyors párbaj kliense is megkapja a már használt match:found eseményt.
  room.members.forEach(id => {
    const opponentId = room.members.find(other => other !== id);
    io.to(id).emit("match:found", {
      roomId: room.id,
      opponent: {
        id: opponentId,
        name: players.get(opponentId)?.name || "Játékos"
      }
    });
  });
  broadcastRooms();
  broadcastPlayers();
}

function makeMatch(idA, idB) {
  const a = players.get(idA);
  const b = players.get(idB);
  if (!a || !b || a.roomId || b.roomId || idA === idB) return false;

  removeFromQueue(idA);
  removeFromQueue(idB);

  const room = createRoomObject("Gyors párbaj", idA);
  room.members.push(idB);
  room.status = "waiting";
  b.roomId = room.id;
  io.sockets.sockets.get(idB)?.join(room.id);

  room.ready.add(idA);
  room.ready.add(idB);
  startRoom(room);

  broadcastRooms();
  broadcastPlayers();
  return true;
}

function validNumber(value, min = -MAX_POSITION, max = MAX_POSITION) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : null;
}

function getPlayingRoom(socket) {
  const p = players.get(socket.id);
  const room = p?.roomId ? rooms.get(p.roomId) : null;
  if (!room || room.status !== "playing" || !room.members.includes(socket.id)) return null;
  return room;
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "runa-online",
    players: io.engine.clientsCount,
    rooms: rooms.size,
    waiting: waitingQueue.length
  });
});

app.use(express.static(publicDir));
app.get("*", (_req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

io.on("connection", socket => {
  players.set(socket.id, {
    name: "Játékos",
    roomId: null,
    connectedAt: Date.now(),
    lastPositionAt: 0,
    position: { x: 0, y: 0, angle: 0 }
  });

  socket.emit("rooms:update", roomList());
  broadcastPlayers();

  socket.on("profile:set", name => {
    const p = players.get(socket.id);
    if (!p) return;
    p.name = cleanName(name);
    broadcastPlayers();
    const room = rooms.get(p.roomId);
    if (room) updateRoom(room);
  });

  socket.on("rooms:list", () => socket.emit("rooms:update", roomList()));

  socket.on("room:create", data => {
    const p = players.get(socket.id);
    if (!p || p.roomId) {
      return socket.emit("room:error", { message: "Már egy szobában vagy." });
    }

    const isPrivate = Boolean(data?.private);
    const password = String(data?.password || "");
    if (isPrivate && password.length < 4) {
      return socket.emit("room:error", { message: "A jelszó legalább 4 karakter legyen." });
    }

    const room = createRoomObject(cleanRoomName(data?.name), socket.id, isPrivate, password);
    socket.emit("room:created", roomInfo(room));
    updateRoom(room);
    broadcastPlayers();
  });

  socket.on("room:join", data => {
    const p = players.get(socket.id);
    if (!p || p.roomId) {
      return socket.emit("room:error", { message: "Már egy szobában vagy." });
    }

    const room = rooms.get(String(data?.roomId || ""));
    if (!room || room.status !== "waiting") {
      return socket.emit("room:error", { message: "A szoba már nem elérhető." });
    }
    if (room.members.length >= 2) {
      return socket.emit("room:error", { message: "A szoba megtelt." });
    }
    if (room.private && String(data?.password || "") !== room.password) {
      return socket.emit("room:error", { message: "Hibás szobajelszó." });
    }

    room.members.push(socket.id);
    room.ready.clear();
    p.roomId = room.id;
    socket.join(room.id);
    socket.emit("room:joined", roomInfo(room));
    updateRoom(room);
    broadcastPlayers();
  });

  socket.on("room:leave", () => leaveRoom(socket.id));

  socket.on("room:ready", value => {
    const p = players.get(socket.id);
    const room = rooms.get(p?.roomId);
    if (!room || room.status !== "waiting" || !room.members.includes(socket.id)) return;

    if (value === false) room.ready.delete(socket.id);
    else room.ready.add(socket.id);

    updateRoom(room);
    startRoom(room);
  });

  socket.on("room:join-code", data => {
    const code = String(data?.code || "").trim().toUpperCase();
    const room = [...rooms.values()].find(r => r.code === code);
    if (!room) {
      return socket.emit("room:error", { message: "Ismeretlen szobakód." });
    }
    socket.emit("room:code-found", {
      roomId: room.id,
      private: room.private
    });
  });

  socket.on("queue:join", () => {
    const p = players.get(socket.id);
    if (!p || p.roomId) {
      return socket.emit("queue:status", {
        active: false,
        message: "Már egy párbajszobában vagy."
      });
    }

    if (!waitingQueue.includes(socket.id)) waitingQueue.push(socket.id);
    socket.emit("queue:status", {
      active: true,
      message: "Ellenfelet keresünk..."
    });

    while (waitingQueue.length >= 2) {
      const first = waitingQueue.shift();
      const second = waitingQueue.shift();
      if (
        first !== second &&
        players.has(first) &&
        players.has(second) &&
        !players.get(first).roomId &&
        !players.get(second).roomId
      ) {
        if (!makeMatch(first, second)) {
          if (players.has(first) && !players.get(first).roomId) waitingQueue.unshift(first);
          if (players.has(second) && !players.get(second).roomId) waitingQueue.push(second);
          break;
        }
      }
    }
  });

  socket.on("queue:leave", () => {
    removeFromQueue(socket.id);
    socket.emit("queue:status", {
      active: false,
      message: "Ellenfélkeresés megszakítva."
    });
  });

  socket.on("challenge:send", targetId => {
    const sender = players.get(socket.id);
    const targetSocketId = String(targetId || "");
    const target = players.get(targetSocketId);

    if (!sender || !target || socket.id === targetSocketId) {
      return socket.emit("challenge:error", { message: "A játékos már nem elérhető." });
    }
    if (sender.roomId || target.roomId) {
      return socket.emit("challenge:error", { message: "Az egyik játékos már párbajban van." });
    }

    pendingChallenges.set(targetSocketId, socket.id);
    io.to(targetSocketId).emit("challenge:received", {
      from: socket.id,
      name: sender.name
    });
    socket.emit("challenge:sent", { name: target.name });
  });

  socket.on("challenge:answer", data => {
    if (!data || typeof data.from !== "string") return;
    const challenger = pendingChallenges.get(socket.id);
    if (challenger !== data.from) return;
    pendingChallenges.delete(socket.id);

    if (data.accept) {
      if (!makeMatch(challenger, socket.id)) {
        io.to(challenger).emit("challenge:error", {
          message: "A párbaj már nem indítható."
        });
      }
    } else {
      io.to(challenger).emit("challenge:error", {
        message: "A játékos nem fogadta el a kihívást."
      });
    }
  });

  // Valós idejű pozíció/frissítés. A kliens "player:move" vagy "player:position"
  // eseményt küldhet {x,y,angle} adattal.
  function relayPosition(data) {
    const room = getPlayingRoom(socket);
    if (!room) return;

    const now = Date.now();
    const p = players.get(socket.id);
    if (now - p.lastPositionAt < 25) return; // max. kb. 40 frissítés/mp
    p.lastPositionAt = now;

    const x = validNumber(data?.x);
    const y = validNumber(data?.y);
    const angle = validNumber(data?.angle, -100000, 100000);
    if (x === null || y === null || angle === null) return;

    p.position = { x, y, angle };
    socket.to(room.id).emit("player:position", {
      id: socket.id,
      name: p.name,
      x, y, angle,
      at: now
    });
  }
  socket.on("player:move", relayPosition);
  socket.on("player:position", relayPosition);

  // Lövések továbbítása. A kliens "player:shoot" vagy "shoot" eseményt küldhet.
  function relayShot(data) {
    const room = getPlayingRoom(socket);
    if (!room) return;

    const x = validNumber(data?.x);
    const y = validNumber(data?.y);
    const angle = validNumber(data?.angle, -100000, 100000);
    if (x === null || y === null || angle === null) return;

    socket.to(room.id).emit("player:shoot", {
      id: socket.id,
      x, y, angle,
      projectileId: String(data?.projectileId || makeId()).slice(0, 64),
      at: Date.now()
    });
  }
  socket.on("player:shoot", relayShot);
  socket.on("shoot", relayShot);

  // Sebzés: csak a szoba másik tagja támadható; a szerver számolja a HP-t.
  socket.on("player:hit", data => {
    const room = getPlayingRoom(socket);
    if (!room || !room.match) return;

    const targetId = String(data?.targetId || "");
    if (!room.members.includes(targetId) || targetId === socket.id) return;

    const damage = validNumber(data?.damage, 1, 100);
    if (damage === null) return;

    room.match.hp[targetId] = Math.max(0, (room.match.hp[targetId] ?? START_HP) - damage);
    io.to(room.id).emit("player:hp", {
      id: targetId,
      hp: room.match.hp[targetId],
      by: socket.id
    });

    if (room.match.hp[targetId] <= 0) {
      room.status = "finished";
      io.to(room.id).emit("match:ended", {
        message: (players.get(targetId)?.name || "Játékos") + " vereséget szenvedett.",
        winnerId: socket.id,
        loserId: targetId
      });
      broadcastRooms();
    }
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);

    for (const [target, challenger] of pendingChallenges) {
      if (target === socket.id || challenger === socket.id) {
        pendingChallenges.delete(target);
      }
    }

    leaveRoom(socket.id);
    players.delete(socket.id);
    broadcastPlayers();
    broadcastRooms();
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Runa online szerver fut: http://0.0.0.0:${PORT}`);
});
