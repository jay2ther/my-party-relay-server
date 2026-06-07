import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// Active sessions tracking layout: room_code -> { host: ws, players: Map(name -> ws) }
const rooms = new Map();

wss.on('connection', (ws) => {
    console.log("New raw socket handshake established.");
    
    ws.isHost = false;
    ws.roomCode = "";
    ws.playerName = "";

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (e) {
            console.log("Dropped packet: Incoming stream payload is not valid JSON.");
            return;
        }

        const { action, room_code, name, target_name, payload } = data;

        switch (action) {
            case 'register_host':
                if (!room_code) return;
                const cleanHostCode = room_code.toUpperCase();
                
                // Verify if the Godot-generated key is already in active use
                if (rooms.has(cleanHostCode)) {
                    ws.send(JSON.stringify({ action: 'error', message: 'room_collision' }));
                    console.log(`Registration Denied: Host code collision detected on -> ${cleanHostCode}`);
                    return;
                }

                ws.isHost = true;
                ws.roomCode = cleanHostCode;
                rooms.set(cleanHostCode, { host: ws, players: new Map() });
                
                ws.send(JSON.stringify({ action: 'room_created', room_code: cleanHostCode }));
                console.log(`Successfully opened room registry channel: ${cleanHostCode}`);
                break;

            case 'join_room':
                if (!room_code || !name) return;
                const cleanJoinCode = room_code.toUpperCase();

                if (!rooms.has(cleanJoinCode)) {
                    ws.send(JSON.stringify({ action: 'error', message: 'room_not_found' }));
                    return;
                }

                const activeRoom = rooms.get(cleanJoinCode);
                
                ws.isHost = false;
                ws.roomCode = cleanJoinCode;
                ws.playerName = name;
                activeRoom.players.set(name, ws);
                
                // Notify the Godot Host that a player has entered the lobby
                if (activeRoom.host && activeRoom.host.readyState === 1) { // 1 = OPEN
                    activeRoom.host.send(JSON.stringify({ action: 'player_joined', name: name }));
                }
                
                console.log(`User '${name}' routed successfully to room: ${cleanJoinCode}`);
                break;

            case 'host_command':
            case 'update_client':
                if (ws.roomCode && rooms.has(ws.roomCode)) {
                    const roomChannels = rooms.get(ws.roomCode);
                    
                    if (target_name) {
                        const clientSocket = roomChannels.players.get(target_name);
                        if (clientSocket && clientSocket.readyState === 1) {
                            clientSocket.send(JSON.stringify({ action: action, payload: payload }));
                        }
                    } else {
                        // Global broadcast to all phone controllers in this room
                        roomChannels.players.forEach((clientSocket) => {
                            if (clientSocket.readyState === 1) {
                                clientSocket.send(JSON.stringify({ action: action, payload: payload }));
                            }
                        });
                    }
                }
                break;

            case 'button_press':
            case 'player_input':
                if (ws.roomCode && rooms.has(ws.roomCode)) {
                    const roomChannels = rooms.get(ws.roomCode);
                    if (roomChannels.host && roomChannels.host.readyState === 1) {
                        roomChannels.host.send(JSON.stringify({
                            action: action,
                            name: ws.playerName,
                            payload: payload
                        }));
                    }
                }
                break;

            case 'ping':
                ws.send(JSON.stringify({ action: 'pong' }));
                break;
        }
    });

    ws.on('close', () => {
        if (ws.roomCode && rooms.has(ws.roomCode)) {
            const activeRoom = rooms.get(ws.roomCode);
            
            if (ws.isHost) {
                console.log(`Host closed room socket connection: ${ws.roomCode}. Terminating channel records.`);
                activeRoom.players.forEach((playerWs) => {
                    if (playerWs.readyState === 1) {
                        playerWs.send(JSON.stringify({ action: 'error', message: 'host_disconnected' }));
                    }
                });
                rooms.delete(ws.roomCode);
            } else if (ws.playerName) {
                console.log(`User '${ws.playerName}' dropped connection path from room: ${ws.roomCode}`);
                activeRoom.players.delete(ws.playerName);
                
                if (activeRoom.host && activeRoom.host.readyState === 1) {
                    activeRoom.host.send(JSON.stringify({ action: 'player_left', name: ws.playerName }));
                }
            }
        }
    });
});

console.log(`Stateless Party Game Relay Server online! Listening path open on port ${PORT}`);
