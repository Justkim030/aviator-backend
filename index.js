require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const crypto = require('crypto');
const bcrypt = require('bcrypt');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: process.env.FRONTEND_URL || "*" },
    transports: ['websocket']
});

// ─── DATABASE SETUP ──────────────────────────────────────────────────────────
let db;
async function initDB() {
    // Render automatically provides DATABASE_URL for PostgreSQL
    const rawUrl = process.env.DATABASE_URL;

    if (!rawUrl) {
        console.error('[DATABASE] Fatal: DATABASE_URL is not defined in Environment Variables.');
        process.exit(1);
    }

    // Use the URL object for safe host extraction and robust credential handling
    const dbUrl = new URL(rawUrl);

    db = new Pool({
        user: decodeURIComponent(dbUrl.username),
        password: decodeURIComponent(dbUrl.password),
        host: dbUrl.hostname,
        port: dbUrl.port,
        database: dbUrl.pathname.split('/')[1] || 'defaultdb',
        ssl: { rejectUnauthorized: false },
        max: 20 // Connection pooling for high-frequency betting
    });

    // Initialize tables
    await db.query(`
        CREATE TABLE IF NOT EXISTS users (
            phone VARCHAR(20) PRIMARY KEY,
            password VARCHAR(255),
            balance DECIMAL(15, 2) DEFAULT 0.00
        )
    `);

    console.log(`[DATABASE] Success: Connected to ${dbUrl.hostname} (PostgreSQL)`);
    
    // Start the first game cycle ONLY after the database is ready
    startCycle();
}

initDB().catch(err => {
    const isAuthError = err.code === '28P01' || err.message.includes('password authentication');
    const hint = isAuthError ? ' (Check your password in Render Env Variables)' : '';
    console.error(`[DATABASE] Fatal Connection Error: ${err.code || err.message}${hint}`);
    process.exit(1);
});

// Map to track active connections: phone number -> socket ID
const activeUsers = new Map();

// Map to track current round bets: socketId -> { phone, amount, status }
let activeBets = new Map();

// ─── GAME STATE ──────────────────────────────────────────────────────────────
let gameState = {
    phase: 'waiting',      // waiting, countdown, flying, crashed
    roundId: 0,
    multiplier: 1.00,
    startTime: 0,
    countdown: 5,
    history: [1.22, 5.43, 1.08, 12.99, 3.21],
    serverSeedHash: '',
    clientSeed: 'aviator-community-seed',
    nonce: 0
};

let currentServerSeed = '';
let gameLoopInterval = null;

// ─── LOGIC ───────────────────────────────────────────────────────────────────

/**
 * Generates a crash point using HMAC-SHA256 (Provably Fair)
 */
function generateProvablyFairCrash(serverSeed, clientSeed, nonce) {
    const hmac = crypto.createHmac('sha256', serverSeed);
    hmac.update(`${clientSeed}:${nonce}`);
    const hash = hmac.digest('hex');
    
    // Use the first 8 characters of the hash for the random value
    const decimal = parseInt(hash.slice(0, 8), 16) / 0xFFFFFFFF;

    if (decimal < 0.01) return 1.00; // 1% Instant crash
    const crash = Math.max(1.00, Math.floor(100 * (0.99 / (1 - decimal))) / 100);
    return parseFloat(crash.toFixed(2));
}

function startCycle() {
    gameState.phase = 'countdown';
    gameState.countdown = 5;
    gameState.multiplier = 1.00;
    gameState.roundId++;
    gameState.nonce++;
    
    activeBets.clear(); // Clear bets from the previous round

    // Generate new seeds for the round
    currentServerSeed = crypto.randomBytes(32).toString('hex');
    gameState.serverSeedHash = crypto.createHash('sha256').update(currentServerSeed).digest('hex');

    const cd = setInterval(() => {
        gameState.countdown--;
        io.emit('countdown', { 
            countdown: gameState.countdown,
            serverSeedHash: gameState.serverSeedHash // Users can see the hash BEFORE the round starts
        });

        if (gameState.countdown <= 0) {
            clearInterval(cd);
            beginFlight();
        }
    }, 1000);
}

function beginFlight() {
    gameState.phase = 'flying';
    gameState.startTime = Date.now();
    
    const targetCrash = generateProvablyFairCrash(currentServerSeed, gameState.clientSeed, gameState.nonce);
    
    console.log(`[Round ${gameState.roundId}] Started. Target: ${targetCrash}x`);
    io.emit('flightStart', { startTime: gameState.startTime });

    gameLoopInterval = setInterval(() => {
        const elapsed = (Date.now() - gameState.startTime) / 1000;
        // Exponential growth: 1.00 * e^(0.1 * t)
        // This matches the curve logic mentioned in your visual breakdown
        gameState.multiplier = Math.pow(Math.E, 0.12 * elapsed);

        if (gameState.multiplier >= targetCrash) {
            endFlight();
        } else {
            io.emit('multiplierUpdate', { multiplier: gameState.multiplier });
        }
    }, 50); // 20 updates per second for smooth rendering
}

function endFlight() {
    clearInterval(gameLoopInterval);
    gameState.phase = 'crashed';
    const finalMult = parseFloat(gameState.multiplier.toFixed(2));
    
    gameState.history.unshift(finalMult);
    gameState.history = gameState.history.slice(0, 20);

    // Any bet still in 'active' status is now lost. 
    // In a professional setup, you'd log these losses to a 'bets' table here.
    activeBets.clear();

    io.emit('crash', { 
        crashMultiplier: finalMult,
        serverSeed: currentServerSeed, // Reveal the seed so players can verify the hash
        history: gameState.history
    });

    // Wait 3 seconds at the crashed screen before starting next round
    setTimeout(() => {
        startCycle();
    }, 3000);
}

// ─── SOCKET CONNECTION ───────────────────────────────────────────────────────
io.on('connection', (socket) => {
    console.log('User connected:', socket.id);
    // Send current game state to the new user so they sync immediately
    socket.emit('gameState', gameState);

    // Register user by phone so we can send them targeted balance updates
    socket.on('registerUser', (phone) => {
        if (phone) {
            socket.phone = phone; // Attach phone to socket for easier lookup
            activeUsers.set(phone, socket.id);
            console.log(`Socket ${socket.id} registered to phone ${phone}`);
        }
    });

    // ─── BETTING LOGIC ───────────────────────────────────────────────────────
    socket.on('placeBet', async ({ amount }) => {
        if (gameState.phase !== 'waiting' && gameState.phase !== 'countdown') {
            return socket.emit('betError', 'Bets only accepted before flight.');
        }
        if (!socket.phone) return socket.emit('betError', 'Please login to bet.');

        const betAmount = Number(Number(amount).toFixed(2)); // Support decimal bets for KES
        if (activeBets.has(socket.id)) return socket.emit('betError', 'Bet already placed for this round.');
        if (isNaN(betAmount) || betAmount <= 0) return socket.emit('betError', 'Invalid bet amount.');

        try {
            const result = await db.query('SELECT balance FROM users WHERE phone = $1', [socket.phone]);
            const user = result.rows[0];

            if (!user || Number(user.balance) < betAmount) {
                return socket.emit('betError', 'Insufficient balance.');
            }

            // 1. Deduct from Database immediately
            await db.query('UPDATE users SET balance = balance - $1 WHERE phone = $2', [betAmount, socket.phone]);
            
            // 2. Track bet in memory for the duration of the flight
            activeBets.set(socket.id, { phone: socket.phone, amount: betAmount, status: 'active' });
            
            // 3. Notify user and others
            const newBal = Number((Number(user.balance || 0) - betAmount).toFixed(2));
            socket.emit('balanceUpdate', { balance: Number(newBal) });
            io.emit('playerBet', { user: socket.phone.replace(/(\d{3})\d+(\d{3})/, '$1***$2'), amount: betAmount });
            
            console.log(`Bet placed: ${socket.phone} - KES ${betAmount}`);
        } catch (e) {
            console.error('Bet placement error:', e);
        }
    });

    socket.on('cashOut', async () => {
        if (gameState.phase !== 'flying') return socket.emit('betError', 'Not in flight.');
        
        const bet = activeBets.get(socket.id);
        if (!bet || bet.status !== 'active') return socket.emit('betError', 'No active bet.');

        try {
            const currentMult = gameState.multiplier;
            const winAmount = Number((bet.amount * currentMult).toFixed(2));

            // 1. Mark as cashed out so they can't double-click
            bet.status = 'cashed';
            activeBets.delete(socket.id);

            // 2. Update Database
            await db.query('UPDATE users SET balance = balance + $1 WHERE phone = $2', [winAmount, socket.phone]);
            
            // 3. Fetch final balance to sync UI
            const result = await db.query('SELECT balance FROM users WHERE phone = $1', [socket.phone]);
            
            socket.emit('balanceUpdate', { balance: Number(result.rows[0].balance || 0) });
            socket.emit('cashOutSuccess', { win: winAmount, multiplier: currentMult });
            
            io.emit('playerCashOut', { 
                user: socket.phone.replace(/(\d{3})\d+(\d{3})/, '$1***$2'), 
                multiplier: currentMult, 
                win: winAmount 
            });
        } catch (e) {
            console.error('Cashout error:', e);
        }
    });

    socket.on('disconnect', () => {
        console.log('User disconnected:', socket.id);
        // Optimized O(1) cleanup using the phone attached to the socket
        if (socket.phone && activeUsers.get(socket.phone) === socket.id) {
            activeUsers.delete(socket.phone);
        }
    });
});

// ─── AUTH API ────────────────────────────────────────────────────────────────
app.post('/api/register', async (req, res) => {
    const phone = req.body.phone?.trim();
    const password = req.body.password;
    if (!phone || !password) return res.status(400).json({ status: false, message: 'Missing phone or password' });

    try {
        const check = await db.query('SELECT phone FROM users WHERE phone = $1', [phone]);
        if (check.rows.length > 0) return res.status(400).json({ status: false, message: 'User already exists' });

        const hashedPassword = await bcrypt.hash(password, 10);
        await db.query('INSERT INTO users (phone, password, balance) VALUES ($1, $2, $3)', [phone, hashedPassword, 0.0]);
        
        res.json({ status: true, message: 'Registration successful' });
    } catch (e) {
        console.error('Registration Error:', e);
        res.status(500).json({ status: false, message: 'Server error' });
    }
});

app.post('/api/login', async (req, res) => {
    const phone = req.body.phone?.trim();
    const password = req.body.password;
    if (!phone || !password) return res.status(400).json({ status: false, message: 'Missing phone or password' });

    try {
        const result = await db.query('SELECT * FROM users WHERE phone = $1', [phone]);
        const user = result.rows[0];
        const isMatch = user ? await bcrypt.compare(password, user.password) : false;

        if (!isMatch) {
            return res.status(401).json({ status: false, message: 'Invalid phone or password' });
        }
        res.json({ 
            status: true, 
            user: { phone: user.phone, balance: Number(user.balance) } 
        });
    } catch (e) {
        console.error('Login Error:', e);
        res.status(500).json({ status: false, message: 'Server error' });
    }
});

// ─── DEPOSIT API (REAL STK PUSH) ─────────────────────────────────────────────
app.post('/api/deposit', async (req, res) => {
    const { amount, phone, email } = req.body;

    try {
        // Using Paystack Charge API for M-Pesa STK Push
        const response = await axios.post(
            'https://api.paystack.co/charge',
            {
                email: email || 'customer@example.com',
                amount: amount * 100, // Paystack expects cents/kobo
                currency: "KES",
                metadata: { phone },
                mobile_money: {
                    phone: phone,
                    provider: "mpesa"
                }
            },
            {
                headers: {
                    Authorization: `Bearer ${process.env.PAYSTACK_SECRET_KEY}`,
                    'Content-Type': 'application/json'
                }
            }
        );

        res.json({ status: true, data: response.data });
    } catch (error) {
        console.error('STK Push Error:', error.response?.data || error.message);
        res.status(500).json({ status: false, message: 'Failed to initiate STK push' });
    }
});

// ─── PAYSTACK WEBHOOK ────────────────────────────────────────────────────────
// This endpoint receives confirmation from Paystack when the user enters their PIN.
app.post('/api/webhook', async (req, res) => {
    const secret = process.env.PAYSTACK_SECRET_KEY;
    
    // Verify that the request actually came from Paystack
    const hash = crypto.createHmac('sha512', secret)
                       .update(JSON.stringify(req.body))
                       .digest('hex');

    if (hash !== req.headers['x-paystack-signature']) {
        return res.status(401).send('Unauthorized');
    }

    const { event, data } = req.body;

    if (event === 'charge.success') {
        const amount = data.amount / 100; // Convert back from cents to KES
        const phone = data.metadata?.phone;

        if (phone) {
            await db.query(`
                INSERT INTO users (phone, balance) VALUES ($1, $2) 
                ON CONFLICT (phone) DO UPDATE SET balance = users.balance + $3
            `, [phone, amount, amount]);
            
            // Fetch the updated balance to send back to the user
            const result = await db.query('SELECT balance FROM users WHERE phone = $1', [phone]);
            const updatedBalance = Number(result.rows[0]?.balance || 0);
            
            console.log(`[WEBHOOK] Successfully credited KES ${amount} to ${phone}. New balance: ${updatedBalance}`);

            const socketId = activeUsers.get(phone);
            if (socketId) {
                io.to(socketId).emit('balanceUpdate', { balance: updatedBalance });
            }
        }
    }

    res.status(200).send('OK');
});

// ─── GRACEFUL SHUTDOWN ───────────────────────────────────────────────────────
const shutdown = (signal) => {
    console.log(`[SERVER] Received ${signal}. Shutting down gracefully...`);
    clearInterval(gameLoopInterval);
    if (db) db.end(); // Close PostgreSQL pool
    process.exit(0);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM')); // Required for Render/Cloud environments

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`Aviator Server running on port ${PORT}`);
});