/**
 * Chromatch Multiplayer Server
 * Node.js + ws WebSocket server
 *
 * Install:  npm install ws
 * Run:      node server.js
 * Default:  ws://localhost:8080
 */

const { WebSocketServer } = require("ws");
const { randomUUID } = require("crypto");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// ─── In-memory state ──────────────────────────────────────────────
const rooms   = new Map();   // roomId → Room
const clients = new Map();   // ws      → ClientMeta

// Room shape:
// {
//   id, hostId, phase: 'lobby'|'preview'|'guess'|'results',
//   players: Map<playerId, PlayerState>,
//   target: {r,g,b}, difficulty: Number,
//   previewTimer: TimeoutId, guessTimer: TimeoutId
// }

// PlayerState shape:
// { id, name, ws, score: null|Number, deltaE: null|Number, submitted: bool, ready: bool }

// ─── Helpers ──────────────────────────────────────────────────────
function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcast(room, msg, excludeId = null) {
  for (const [pid, p] of room.players) {
    if (pid !== excludeId) send(p.ws, msg);
  }
}

function broadcastAll(room, msg) {
  broadcast(room, msg);
}

function roomSummary(room) {
  return {
    id:         room.id,
    phase:      room.phase,
    difficulty: room.difficulty,
    hostId:     room.hostId,
    players: [...room.players.values()].map(p => ({
      id:        p.id,
      name:      p.name,
      score:     p.score,
      deltaE:    p.deltaE,
      submitted: p.submitted,
      ready:     p.ready,
    })),
  };
}

function genTarget() {
  return {
    r: Math.floor(Math.random() * 256),
    g: Math.floor(Math.random() * 256),
    b: Math.floor(Math.random() * 256),
  };
}

function checkAllSubmitted(room) {
  return [...room.players.values()].every(p => p.submitted);
}

function endRound(room) {
  clearTimeout(room.guessTimer);
  room.phase = "results";

  // Sort leaderboard
  const leaderboard = [...room.players.values()]
    .map(p => ({ id: p.id, name: p.name, score: p.score ?? 0, deltaE: p.deltaE }))
    .sort((a, b) => b.score - a.score);

  broadcastAll(room, {
    type:        "round_end",
    target:      room.target,
    leaderboard,
  });

  // Reset submitted flags; keep scores for display
  for (const p of room.players.values()) {
    p.submitted = false;
    p.ready     = false;
  }
}

// ─── Message handlers ─────────────────────────────────────────────
function handleCreate(ws, { name, difficulty }) {
  const playerId = randomUUID();
  const roomId   = Math.random().toString(36).slice(2, 7).toUpperCase();

  const player = {
    id: playerId, name: name || "Player",
    ws, score: null, deltaE: null, submitted: false, ready: false,
  };

  const room = {
    id:         roomId,
    hostId:     playerId,
    phase:      "lobby",
    difficulty: difficulty || 3,
    players:    new Map([[playerId, player]]),
    target:     null,
    previewTimer: null,
    guessTimer:   null,
  };

  rooms.set(roomId, room);
  clients.set(ws, { playerId, roomId });

  send(ws, { type: "joined", playerId, room: roomSummary(room) });
}

function handleJoin(ws, { roomId, name }) {
  const room = rooms.get(roomId?.toUpperCase());
  if (!room) { send(ws, { type: "error", msg: "Room not found" }); return; }
  if (room.phase !== "lobby") { send(ws, { type: "error", msg: "Round in progress" }); return; }
  if (room.players.size >= 8) { send(ws, { type: "error", msg: "Room is full" }); return; }

  const playerId = randomUUID();
  const player   = {
    id: playerId, name: name || "Player",
    ws, score: null, deltaE: null, submitted: false, ready: false,
  };

  room.players.set(playerId, player);
  clients.set(ws, { playerId, roomId: room.id });

  send(ws, { type: "joined", playerId, room: roomSummary(room) });
  broadcast(room, { type: "player_joined", player: { id: playerId, name: player.name } }, playerId);
}

function handleStartRound(ws, _, meta) {
  const room = rooms.get(meta.roomId);
  if (!room || room.hostId !== meta.playerId) return;
  if (room.phase !== "lobby" && room.phase !== "results") return;

  // Reset player state
  for (const p of room.players.values()) {
    p.score = null; p.deltaE = null; p.submitted = false; p.ready = false;
  }

  room.target = genTarget();
  room.phase  = "preview";
  const previewMs = room.difficulty * 1000;
  const guessMs   = room.difficulty * 2000;

  broadcastAll(room, {
    type:    "round_start",
    target:  room.target,          // shown during preview only
    preview: previewMs,
    guess:   guessMs,
  });

  // After preview → guess phase
  room.previewTimer = setTimeout(() => {
    room.phase = "guess";
    broadcastAll(room, { type: "guess_phase" });

    // Auto-end after guess window
    room.guessTimer = setTimeout(() => endRound(room), guessMs);
  }, previewMs);
}

function handleSubmit(ws, { r, g, b }, meta) {
  const room = rooms.get(meta.roomId);
  if (!room || room.phase !== "guess") return;

  const player = room.players.get(meta.playerId);
  if (!player || player.submitted) return;

  const t = room.target;
  const de = Math.sqrt(
    (r - t.r) ** 2 + (g - t.g) ** 2 + (b - t.b) ** 2
  );
  // Approximate ΔE from RGB distance (good enough for real-time scoring)
  const score = Math.max(0, Math.round(100 - de * 100 / 441.67));

  player.submitted = true;
  player.score     = score;
  player.deltaE    = +de.toFixed(1);

  // Notify everyone of partial update
  broadcastAll(room, {
    type:      "player_submitted",
    playerId:  player.id,
    name:      player.name,
    score,
  });

  if (checkAllSubmitted(room)) endRound(room);
}

function handleSetDifficulty(ws, { difficulty }, meta) {
  const room = rooms.get(meta.roomId);
  if (!room || room.hostId !== meta.playerId) return;
  if (room.phase !== "lobby") return;
  room.difficulty = difficulty;
  broadcastAll(room, { type: "difficulty_changed", difficulty });
}

function handleReady(ws, _, meta) {
  const room = rooms.get(meta.roomId);
  if (!room) return;
  const player = room.players.get(meta.playerId);
  if (!player) return;
  player.ready = true;
  broadcastAll(room, { type: "player_ready", playerId: player.id });
}

// ─── Connection lifecycle ─────────────────────────────────────────
wss.on("connection", ws => {
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const meta = clients.get(ws) || {};

    switch (msg.type) {
      case "create":         handleCreate(ws, msg); break;
      case "join":           handleJoin(ws, msg); break;
      case "start_round":    handleStartRound(ws, msg, meta); break;
      case "submit":         handleSubmit(ws, msg, meta); break;
      case "set_difficulty": handleSetDifficulty(ws, msg, meta); break;
      case "ready":          handleReady(ws, msg, meta); break;
    }
  });

  ws.on("close", () => {
    const meta = clients.get(ws);
    if (!meta) return;
    clients.delete(ws);

    const room = rooms.get(meta.roomId);
    if (!room) return;

    room.players.delete(meta.playerId);
    broadcast(room, { type: "player_left", playerId: meta.playerId });

    if (room.players.size === 0) {
      clearTimeout(room.previewTimer);
      clearTimeout(room.guessTimer);
      rooms.delete(room.id);
      return;
    }

    // Transfer host if host left
    if (room.hostId === meta.playerId) {
      room.hostId = [...room.players.keys()][0];
      broadcastAll(room, { type: "host_changed", hostId: room.hostId });
    }
  });
});

console.log(`🎨 Chromatch server running on ws://localhost:${PORT}`);
