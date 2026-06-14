const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Maps roomCode -> { hostSocket: ws, playerSockets: Map(playerId -> ws) }
const rooms = new Map();

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    let currentRoomCode = null;
    let isHost = false;
    let playerId = null;

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            const { action, roomCode, data } = parsed;

            if (action === 'register_host') {
                const code = roomCode.toUpperCase();
                if (rooms.has(code)) {
                    ws.send(JSON.stringify({ type: 'error', message: 'Room code already exists.' }));
                    return;
                }
                rooms.set(code, { host: ws, players: new Map() });
                currentRoomCode = code;
                isHost = true;
                ws.send(JSON.stringify({ type: 'host_registered', roomCode: code }));
                console.log(`Room [${code}] registered by Host.`);
            }

            else if (action === 'join_room') {
                const code = roomCode.toUpperCase();
                if (!rooms.has(code)) {
                    ws.send(JSON.stringify({ type: 'error', message: 'Room not found.' }));
                    return;
                }
                
                const room = rooms.get(code);
                playerId = '_' + Math.random().toString(36).substr(2, 9); // Create a temporary unique ID
                room.players.set(playerId, ws);
                currentRoomCode = code;

                // Tell the player's phone that they succeeded
                ws.send(JSON.stringify({ type: 'joined', playerId: playerId, roomCode: code }));

                // Tell the Godot Host that a player joined
                if (room.host && room.host.readyState === WebSocket.OPEN) {
                    room.host.send(JSON.stringify({
                        type: 'player_joined',
                        playerId: playerId,
                        playerName: data.playerName || 'Anonymous'
                    }));
                }
                console.log(`Player ${playerId} joined Room [${code}].`);
            }

            else if (action === 'send_to_host') {
                // Relays phone input to Godot
                if (!currentRoomCode || !rooms.has(currentRoomCode)) return;
                const room = rooms.get(currentRoomCode);
                if (room.host && room.host.readyState === WebSocket.OPEN) {
                    room.host.send(JSON.stringify({
                        type: 'player_input',
                        playerId: playerId,
                        payload: data
                    }));
                }
            }

            else if (action === 'send_to_player') {
                // Host sends something back to a specific phone (e.g., feedback/points)
                if (!isHost || !currentRoomCode) return;
                const room = rooms.get(currentRoomCode);
                const targetId = parsed.targetPlayerId;
                if (room && room.players.has(targetId)) {
                    const playerWs = room.players.get(targetId);
                    if (playerWs.readyState === WebSocket.OPEN) {
                        playerWs.send(JSON.stringify({
                            type: 'host_message',
                            payload: data
                        }));
                    }
                }
            }

        } catch (e) {
            console.error("Failed to parse message or route action:", e);
        }
    });

    ws.on('close', () => {
        if (currentRoomCode && rooms.has(currentRoomCode)) {
            const room = rooms.get(currentRoomCode);
            if (isHost) {
                // Host disconnected: Disconnect all players and clean up the room
                for (const [pId, playerWs] of room.players) {
                    if (playerWs.readyState === WebSocket.OPEN) {
                        playerWs.send(JSON.stringify({ type: 'error', message: 'Host disconnected.' }));
                        playerWs.close();
                    }
                }
                rooms.delete(currentRoomCode);
                console.log(`Room [${currentRoomCode}] cleared because host disconnected.`);
            } else if (playerId) {
                // Player disconnected: Inform host
                room.players.delete(playerId);
                if (room.host && room.host.readyState === WebSocket.OPEN) {
                    room.host.send(JSON.stringify({
                        type: 'player_left',
                        playerId: playerId
                    }));
                }
                console.log(`Player ${playerId} disconnected.`);
            }
        }
    });
});

// Periodic ping/pong to keep connections alive on free tiers
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

console.log(`Relay server actively listening on port ${PORT}`);
