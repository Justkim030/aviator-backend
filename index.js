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
const jwt = require('jsonwebtoken');
const winston = require('winston');
const rateLimit = require('express-rate-limit');

// ─── LOGGING SETUP ──────────────────────────────────────────────────────────
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.Console(),
        // In production, these logs can be piped to services like Logtail or Datadog
    ],
});

const app = express();

// Required for express-rate-limit to work behind Render's proxy
app.set('trust proxy', 1);

const allowedOrigin = process.env.FRONTEND_URL || "http://localhost:5173";
app.use(cors({ 
    origin: allowedOrigin,
    credentials: true // Required if you decide to use HttpOnly cookies for JWTs
}));
app.use(express.json());

// ─── RATE LIMITING ──────────────────────────────────────────────────────────
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100,
    message: { status: false, message: 'Too many requests, please try again later.' }
});

const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 5,
    message: { status: false, message: 'Too many attempts. Please try again in 15 mins.' }
});

// Apply to sensitive routes
app.use('/api/login', authLimiter);
app.use('/api/register', authLimiter);
app.use('/api/deposit', apiLimiter);

const ADMIN_PHONE = process.env.ADMIN_PHONE || '254700000000';
const JWT_SECRET = process.env.JWT_SECRET; // Removed fallback to force env variable usage
if (!JWT_SECRET) logger.error("JWT_SECRET is missing from environment variables!");

const server = http.createServer(app);
const PORT = process.env.PORT || 3001;
const io = new Server(server, {
    cors: { origin: allowedOrigin },
    transports: ['websocket']
});

// ─── DATABASE SETUP ──────────────────────────────────────────────────────────
let db;
async function initDB() {
    // Render automatically provides DATABASE_URL for PostgreSQL
    const rawUrl = process.env.DATABASE_URL;

    if (!rawUrl) {
        logger.error('[DATABASE] Fatal: DATABASE_URL is not defined in Environment Variables.');
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
    
    await db.query(`
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            phone VARCHAR(20),
            type VARCHAR(20), -- 'deposit', 'bet', 'win', 'withdrawal'
            amount DECIMAL(15, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    logger.info(`[DATABASE] Success: Connected to ${dbUrl.hostname} (PostgreSQL)`);
    
    // Start the first game cycle ONLY after the database is ready
    await runPhoneMigration();
    startCycle();
}

initDB().catch(err => {
    const isAuthError = err.code === '28P01' || err.message.includes('password authentication');
    const hint = isAuthError ? ' (Check your password in Render Env Variables)' : '';
    logger.error(`[DATABASE] Fatal Connection Error: ${err.code || err.message}${hint}`);
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
let seedGeneratedAt = 0;
let gameLoopInterval = null;

// ─── LOGIC ───────────────────────────────────────────────────────────────────

/**
 * Standardizes phone numbers to 2547XXXXXXXX format (no plus)
 */
function normalizePhone(phone) {
    if (!phone) return '';
    let p = phone.trim().replace(/\D/g, '');
    if (p.startsWith('0')) p = '254' + p.slice(1);
    else if ((p.startsWith('7') || p.startsWith('1')) && p.length === 9) p = '254' + p;
    return p;
}

/**
 * One-time migration to ensure all phone numbers follow the 2547XXXXXXXX format.
 * This handles merging balances if a user registered twice with different formats.
 */
async function runPhoneMigration() {
    const client = await db.connect();
    try {
        await client.query('BEGIN');
        // Find users that need normalization (start with 0, or are 9 digits, or have a +)
        const { rows: users } = await client.query("SELECT phone, balance FROM users WHERE phone LIKE '0%' OR LENGTH(phone) = 9 OR phone LIKE '+%'");
        
        for (const u of users) {
            const norm = normalizePhone(u.phone);
            if (norm === u.phone) continue;

            const { rows: collisions } = await client.query('SELECT phone FROM users WHERE phone = $1', [norm]);
            if (collisions.length > 0) {
                // Collision found: Merge balance into the normalized account and delete the old one
                await client.query('UPDATE users SET balance = balance + $1 WHERE phone = $2', [u.balance, norm]);
                await client.query('DELETE FROM users WHERE phone = $1', [u.phone]);
                logger.info(`[MIGRATION] Merged balance for ${u.phone} into ${norm}`);
            } else {
                // No collision: Just update the primary key to the new format
                await client.query('UPDATE users SET phone = $1 WHERE phone = $2', [norm, u.phone]);
                logger.info(`[MIGRATION] Updated ${u.phone} to ${norm}`);
            }
        }

        // Normalize all historical transaction records in one batch SQL command
        await client.query(`
            UPDATE transactions 
            SET phone = CASE 
                WHEN regexp_replace(phone, '\\D', '', 'g') ~ '^0' THEN '254' || SUBSTRING(regexp_replace(phone, '\\D', '', 'g') FROM 2)
                WHEN length(regexp_replace(phone, '\\D', '', 'g')) = 9 AND regexp_replace(phone, '\\D', '', 'g') ~ '^[71]' THEN '254' || regexp_replace(phone, '\\D', '', 'g')
                ELSE regexp_replace(phone, '\\D', '', 'g')
            END
            WHERE phone LIKE '0%' OR LENGTH(phone) = 9 OR phone LIKE '+%'
        `);

        await client.query('COMMIT');
        if (users.length > 0) logger.info(`[MIGRATION] Database normalization complete.`);
    } catch (e) {
        await client.query('ROLLBACK');
        logger.error('[MIGRATION] Error during phone normalization:', e);
    } finally {
        client.release();
    }
}

/**
 * Sends an SMS notification via TalkSasa API
 * @param {string} phone - Formatted as 2547XXXXXXXX
 * @param {string} message - Customizable message content
 */
async function sendTalkSasaSMS(phone, message) {
    const apiKey = process.env.TALKSASA_API_KEY;
    const senderId = process.env.TALKSASA_SENDER_ID || 'SASA_SMS';

    if (!apiKey) {
        logger.error('[SMS] TalkSasa API Key is missing');
        return;
    }

    try {
        const response = await axios.post('https://api.talksasa.com/v1/sms', {
            apiKey,
            senderId,
            message,
            phone
        });
        logger.info(`[SMS] Attempted for ${phone}. Status: success. Ref: ${JSON.stringify(response.data)}`);
    } catch (error) {
        logger.error(`[SMS] Attempted for ${phone}. Status: failure. Error: ${error.message}. Message: "${message}"`);
    }
}

/**
 * Rotates the server seed if 24 hours have passed.
 */
function refreshDailySeed() {
    const now = Date.now();
    const oneDay = 24 * 60 * 60 * 1000;

    if (!currentServerSeed || (now - seedGeneratedAt) > oneDay) {
        currentServerSeed = crypto.randomBytes(32).toString('hex');
        gameState.serverSeedHash = crypto.createHash('sha256').update(currentServerSeed).digest('hex');
        seedGeneratedAt = now;
        logger.info('[FAIRNESS] New Daily Server Seed Generated.');
    }
}

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
    refreshDailySeed(); // Ensure the seed is rotated daily

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
    
    // Look-ahead: Pre-calculate the next 5 crash points for monitoring
    let upcoming = [];
    for (let i = 1; i <= 5; i++) {
        const nextCrash = generateProvablyFairCrash(currentServerSeed, gameState.clientSeed, gameState.nonce + i);
        upcoming.push(`R${gameState.roundId + i}: ${nextCrash}x`);
    }

    logger.info(`[Round ${gameState.roundId}] Started. Target: ${targetCrash}x | Next 5: ${upcoming.join(' | ')}`);
    io.emit('flightStart', { startTime: gameState.startTime });

    gameLoopInterval = setInterval(() => {
        try {
            const elapsed = (Date.now() - gameState.startTime) / 1000;
            // Exponential growth: 1.00 * e^(0.1 * t)
            // This matches the curve logic mentioned in your visual breakdown
            gameState.multiplier = Math.pow(Math.E, 0.12 * elapsed);

            if (gameState.multiplier >= targetCrash) {
                endFlight();
            } else {
                io.emit('multiplierUpdate', { multiplier: gameState.multiplier });
            }
        } catch (err) {
            logger.error(`[GAMELOOP] Error in Round ${gameState.roundId}:`, err);
            endFlight(); // Safely end the flight if an error occurs
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
    logger.info(`User connected: ${socket.id}`);
    // Send current game state to the new user so they sync immediately
    socket.emit('gameState', gameState);

    // SECURITY: Authenticate socket connection via JWT
    socket.on('authenticate', ({ phone, token }) => {
        jwt.verify(token, JWT_SECRET, (err, decoded) => {
            if (err || decoded.phone !== phone) {
                return socket.emit('betError', 'Authentication failed. Please login again.');
            }
            socket.phone = decoded.phone;
            activeUsers.set(decoded.phone, socket.id);
            logger.info(`Socket ${socket.id} authenticated for ${decoded.phone}`);
        });
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

        const client = await db.connect();
        try {
            await client.query('BEGIN');
            
            // Lock the user row for the duration of the transaction to prevent race conditions
            const result = await client.query('SELECT balance FROM users WHERE phone = $1 FOR UPDATE', [socket.phone]);
            const user = result.rows[0];

            if (!user || Number(user.balance) < betAmount) {
                await client.query('ROLLBACK');
                return socket.emit('betError', 'Insufficient balance.');
            }

            await client.query('UPDATE users SET balance = balance - $1 WHERE phone = $2', [betAmount, socket.phone]);
            await client.query('INSERT INTO transactions (phone, type, amount) VALUES ($1, $2, $3)', [socket.phone, 'bet', betAmount]);
            await client.query('COMMIT');
            
            activeBets.set(socket.id, { phone: socket.phone, amount: betAmount, status: 'active' });
            
            const newBal = Number((Number(user.balance) - betAmount).toFixed(2));
            socket.emit('balanceUpdate', { balance: Number(newBal) });
            io.emit('playerBet', { user: socket.phone.replace(/(\d{3})\d+(\d{3})/, '$1***$2'), amount: betAmount });
            
            logger.info(`Bet placed: ${socket.phone} - KES ${betAmount}`);
        } catch (e) {
            await client.query('ROLLBACK');
            logger.error('Bet placement error:', e);
        } finally {
            client.release();
        }
    });

    socket.on('cashOut', async () => {
        if (gameState.phase !== 'flying') return socket.emit('betError', 'Not in flight.');
        
        const bet = activeBets.get(socket.id);
        if (!bet || bet.status !== 'active') return socket.emit('betError', 'No active bet.');

        const client = await db.connect();
        try {
            await client.query('BEGIN');
            const currentMult = gameState.multiplier;
            const winAmount = Number((bet.amount * currentMult).toFixed(2));

            bet.status = 'cashed';
            activeBets.delete(socket.id);

            await client.query('UPDATE users SET balance = balance + $1 WHERE phone = $2', [winAmount, socket.phone]);
            await client.query('INSERT INTO transactions (phone, type, amount) VALUES ($1, $2, $3)', [socket.phone, 'win', winAmount]);
            
            const result = await client.query('SELECT balance FROM users WHERE phone = $1 FOR UPDATE', [socket.phone]);
            await client.query('COMMIT');
            
            socket.emit('balanceUpdate', { balance: Number(result.rows[0].balance || 0) });
            socket.emit('cashOutSuccess', { win: winAmount, multiplier: currentMult });
            
            io.emit('playerCashOut', { 
                user: socket.phone.replace(/(\d{3})\d+(\d{3})/, '$1***$2'), 
                multiplier: currentMult, 
                win: winAmount 
            });
        } catch (e) {
            await client.query('ROLLBACK');
            logger.error('Cashout error:', e);
        } finally {
            client.release();
        }
    });

    socket.on('disconnect', () => {
        logger.info(`User disconnected: ${socket.id}`);
        // Optimized O(1) cleanup using the phone attached to the socket
        if (socket.phone && activeUsers.get(socket.phone) === socket.id) {
            activeUsers.delete(socket.phone);
        }
    });
});

// ─── AUTH API ────────────────────────────────────────────────────────────────
app.post('/api/register', authLimiter, async (req, res) => {
    const phone = normalizePhone(req.body.phone);
    const password = req.body.password;
    if (!phone || !password) return res.status(400).json({ status: false, message: 'Missing phone or password' });

    try {
        const check = await db.query('SELECT phone FROM users WHERE phone = $1', [phone]);
        if (check.rows.length > 0) return res.status(400).json({ status: false, message: 'User already exists' });

        const hashedPassword = await bcrypt.hash(password, 10);
        await db.query('INSERT INTO users (phone, password, balance) VALUES ($1, $2, $3)', [phone, hashedPassword, 0.0]);
        
        res.json({ status: true, message: 'Registration successful' });
    } catch (e) {
        logger.error('Registration Error:', e);
        res.status(500).json({ status: false, message: 'Server error' });
    }
});

app.post('/api/login', authLimiter, async (req, res) => {
    const phone = normalizePhone(req.body.phone);
    const password = req.body.password;
    if (!phone || !password) return res.status(400).json({ status: false, message: 'Missing phone or password' });

    try {
        const result = await db.query('SELECT * FROM users WHERE phone = $1', [phone]);
        const user = result.rows[0];
        const isMatch = user ? await bcrypt.compare(password, user.password) : false;

        if (!isMatch) {
            return res.status(401).json({ status: false, message: 'Invalid phone or password' });
        }

        // Generate JWT Token
        const token = jwt.sign({ phone: user.phone }, JWT_SECRET, { expiresIn: '7d' });

        res.json({ 
            status: true, 
            user: { phone: user.phone, balance: Number(user.balance), token } 
        });
    } catch (e) {
        logger.error('Login Error:', e);
        res.status(500).json({ status: false, message: 'Server error' });
    }
});

// ─── DEPOSIT API (REAL STK PUSH) ─────────────────────────────────────────────
app.post('/api/deposit', async (req, res) => {
    const { amount, phone } = req.body; // Removed email from destructuring

    if (!amount || isNaN(amount) || amount < 49) {
        return res.status(400).json({ status: false, message: 'Minimum deposit is KES 49' });
    }

    if (!process.env.PAYSTACK_SECRET_KEY) {
        logger.error('[DEPOSIT] PAYSTACK_SECRET_KEY is missing from environment variables');
        return res.status(500).json({ status: false, message: 'Payment provider not configured' });
    }

    const normalized = normalizePhone(phone);
    // Paystack needs the + prefix for the actual STK push
    const paystackPhone = '+' + normalized;

    // Strict final validation for Paystack M-Pesa format (+254 subscriber number)
    if (!/^\+254(7|1)\d{8}$/.test(paystackPhone)) {
        logger.error('[DEPOSIT] Final phone number format validation failed for Paystack', { originalPhone: phone, paystackPhone });
        return res.status(400).json({ status: false, message: 'Invalid phone number. Use format +2547XXXXXXXX or 07XXXXXXXX.' });
    }

    // Generate a plausible email for Paystack receipt if not provided by client
    const receiptEmail = `${normalized}@aviator.game`;

    try {
        // Using Paystack Charge API for M-Pesa STK Push
        const response = await axios.post(
            'https://api.paystack.co/charge',
            {
                email: receiptEmail,
                amount: amount * 100, // Paystack expects cents/kobo
                currency: "KES",
                metadata: { phone: normalized }, // Use normalized for internal matching
                mobile_money: {
                    phone: paystackPhone,
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
        const paystackError = error.response?.data?.message || error.message;
        logger.error('STK Push Error:', { 
            error: paystackError, 
            details: error.response?.data,
            phone: paystackPhone 
        });
        res.status(500).json({ status: false, message: paystackError || 'Failed to initiate STK push' });
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
        const phone = normalizePhone(data.metadata?.phone);

        if (phone) {
            await db.query(`
                INSERT INTO users (phone, balance) VALUES ($1, $2) 
                ON CONFLICT (phone) DO UPDATE SET balance = users.balance + $3
            `, [phone, amount, amount]);
            
            await db.query('INSERT INTO transactions (phone, type, amount) VALUES ($1, $2, $3)', [phone, 'deposit', amount]);

            // Fetch the updated balance to send back to the user
            const result = await db.query('SELECT balance FROM users WHERE phone = $1', [phone]);
            const updatedBalance = Number(result.rows[0]?.balance || 0);
            
            logger.info(`[WEBHOOK] Successfully credited KES ${amount} to ${phone}. New balance: ${updatedBalance}`);

            const socketId = activeUsers.get(phone);
            if (socketId) {
                io.to(socketId).emit('balanceUpdate', { balance: updatedBalance });
            }
        }
    }

    res.status(200).send('OK');
});

// ─── WITHDRAWAL API ──────────────────────────────────────────────────────────
app.post('/api/withdraw', authLimiter, async (req, res) => {
    const { amount } = req.body;
    const phone = normalizePhone(req.body.phone);

    if (!amount || amount < 100) return res.status(400).json({ status: false, message: 'Minimum withdrawal is KES 100' });

    const client = await db.connect();
    try {
        await client.query('BEGIN');
        const result = await client.query('SELECT balance FROM users WHERE phone = $1 FOR UPDATE', [phone]);
        const user = result.rows[0];

        const withdrawalAmount = Number(amount);

        if (!user || Number(user.balance) < withdrawalAmount) {
            await client.query('ROLLBACK');
            return res.status(400).json({ status: false, message: 'Insufficient balance' });
        }

        await client.query('UPDATE users SET balance = balance - $1 WHERE phone = $2', [withdrawalAmount, phone]);
        const txResult = await client.query(
            'INSERT INTO transactions (phone, type, amount) VALUES ($1, $2, $3) RETURNING id', 
            [phone, 'withdrawal', withdrawalAmount]
        );
        const txRef = txResult.rows[0].id;
        await client.query('COMMIT');

        // After successful commit, send SMS notification
        const now = new Date();
        const formattedDate = `${now.getDate()}/${now.getMonth() + 1}/${now.getFullYear().toString().slice(-2)}`;
        const formattedTime = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true });
        const formattedAmount = withdrawalAmount.toLocaleString('en-KE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const transactionId = crypto.randomBytes(4).toString('hex').toUpperCase() + txRef;

        const smsContent = `Confirmed. Ksh${formattedAmount} has been sent to you from AVIATOR GAME (Acc: ${phone}) on ${formattedDate} at ${formattedTime}. Transaction ID: ${transactionId}.`;
        // --- CUSTOMIZE THESE LABELS ---
        const brandName = "AVIATOR GAME"; 
        const accountLabel = phone; // You can change this to a username if you add a name column to your DB

        const smsContent = `Confirmed. Ksh${formattedAmount} has been sent to you from ${brandName} (Acc: ${accountLabel}) on ${formattedDate} at ${formattedTime}. Transaction ID: ${transactionId}.`;
        sendTalkSasaSMS(phone, smsContent);

        res.json({ status: true, message: 'Withdrawal request received and is being processed.' });
    } catch (e) {
        await client.query('ROLLBACK');
        logger.error('Withdrawal error:', e);
        res.status(500).json({ status: false, message: 'Server error' });
    } finally {
        client.release();
    }
});

// ─── ADMIN DASHBOARD ROUTE ──────────────────────────────────────────────────
// Provides visibility into the next 50 crash points for monitoring and debugging.
app.get('/api/admin/upcoming', async (req, res) => {
    const authHeader = req.headers.authorization;
    if (!authHeader) return res.status(401).json({ status: false, message: 'Unauthorized' });

    const token = authHeader.split(' ')[1];
    jwt.verify(token, JWT_SECRET, (err, decoded) => {
        if (err || decoded.phone !== ADMIN_PHONE) {
            return res.status(403).json({ status: false, message: 'Forbidden' });
        }

        const upcoming = [];
        const count = 50;

        for (let i = 1; i <= count; i++) {
            const nextNonce = gameState.nonce + i;
            const nextRoundId = gameState.roundId + i;
            const crashPoint = generateProvablyFairCrash(currentServerSeed, gameState.clientSeed, nextNonce);
            upcoming.push({
                roundId: nextRoundId,
                nonce: nextNonce,
                crashMultiplier: crashPoint
            });
        }

        res.json({
            status: true,
            serverSeedHash: gameState.serverSeedHash,
            clientSeed: gameState.clientSeed,
            upcoming
        });
    });
});

// ─── GRACEFUL SHUTDOWN ───────────────────────────────────────────────────────
const shutdown = (signal) => {
    logger.info(`[SERVER] Received ${signal}. Shutting down gracefully...`);
    clearInterval(gameLoopInterval);
    if (db) db.end(); // Close PostgreSQL pool
    process.exit(0);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM')); // Required for Render/Cloud environments

server.listen(PORT, () => {
    logger.info(`Aviator Server running on port ${PORT}`);
});