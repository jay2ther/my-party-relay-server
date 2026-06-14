const { WebSocketServer } = require('ws');
const http = require('http');

// Render.com uses the PORT environment variable. Defaults to 10000.
const PORT = process.env.PORT || 10000;

// Create an HTTP server so Render.com can perform its required health checks.
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Relay server is running');
});

const wss = new WebSocketServer({ server });

// Structure: Map( roomCode -> { hostSocket, clients: Map(clientId -> socket) } )
const rooms = new Map();

wss.on('connection', (socket) => {
    let myRoomCode = null;
    let isHost = false;
    let myClientId = null;

    socket.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            // 1. Godot registers as the Room Host
            if (data.action === 'host') {
                const roomCode = data.room.toUpperCase();
                
                if (rooms.has(roomCode)) {
                    socket.send(JSON.stringify({ type: 'error', message: 'Room code already exists.' }));
                    return;
                }

                myRoomCode = roomCode;
                isHost = true;
                
                rooms.set(roomCode, {
                    hostSocket: socket,
                    clients: new Map()
                });
                
                console.log(`Room created: ${roomCode}`);
                socket.send(JSON.stringify({ type: 'host_registered', room: roomCode }));
            } 
            
            // 2. Mobile client joins a specific room
            else if (data.action === 'join') {
                const roomCode = data.room.toUpperCase();
                const clientId = data.clientId;

                if (!rooms.has(roomCode)) {
                    socket.send(JSON.stringify({ type: 'error', message: 'Room not found.' }));
                    return;
                }

                const room = rooms.get(roomCode);
                
                myRoomCode = roomCode;
                myClientId = clientId;
                isHost = false;
                
                room.clients.set(clientId, socket);
                
                console.log(`Client ${clientId} joined room ${roomCode}`);
                socket.send(JSON.stringify({ type: 'join_success', room: roomCode }));
                
                // Notify the Godot host that a client joined
                if (room.hostSocket && room.hostSocket.readyState === 1) { // 1 = OPEN
                    room.hostSocket.send(JSON.stringify({
                        type: 'player_joined',
                        clientId: clientId
                    }));
                }
            } 
            
            // 3. Relay message from Phone -> Godot
            else if (data.action === 'to_host') {
                if (myRoomCode && rooms.has(myRoomCode)) {
                    const room = rooms.get(myRoomCode);
                    if (room.hostSocket && room.hostSocket.readyState === 1) {
                        room.hostSocket.send(JSON.stringify({
                            type: 'client_message',
                            clientId: myClientId,
                            payload: data.payload
                        }));
                    }
                }
            } 
            
            // 4. Relay message from Godot -> Specific Phone
            else if (data.action === 'to_client') {
                const targetClientId = data.clientId;
                if (myRoomCode && rooms.has(myRoomCode) && isHost) {
                    const room = rooms.get(myRoomCode);
                    const clientSocket = room.clients.get(targetClientId);
                    if (clientSocket && clientSocket.readyState === 1) {
                        clientSocket.send(JSON.stringify({
                            type: 'host_message',
                            payload: data.payload
                        }));
                    }
                }
            }
        } catch (err) {
            console.error('Error parsing JSON message:', err);
        }
    });

    // 5. Cleanup when someone disconnects
    socket.on('close', () => {
        if (!myRoomCode) return;

        if (isHost) {
            // Godot disconnected: close room, alert clients, and purge room
            console.log(`Host closed room: ${myRoomCode}`);
            const room = rooms.get(myRoomCode);
            if (room) {
                room.clients.forEach((clientSocket) => {
                    clientSocket.send(JSON.stringify({ type: 'room_closed' }));
                    clientSocket.close();
                });
                rooms.delete(myRoomCode);
            }
        } else {
            // Phone disconnected: notify Godot, remove player
            console.log(`Client ${myClientId} disconnected from room ${myRoomCode}`);
            const room = rooms.get(myRoomCode);
            if (room) {
                room.clients.delete(myClientId);
                if (room.hostSocket && room.hostSocket.readyState === 1) {
                    room.hostSocket.send(JSON.stringify({
                        type: 'player_left',
                        clientId: myClientId
                    }));
                }
            }
        }
    });
});

server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});
