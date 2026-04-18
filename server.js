'use strict'

const express    = require('express')
const http       = require('http')
const { Server } = require('socket.io')
const crypto     = require('crypto')
const cors       = require('cors')

const app    = express()
const server = http.createServer(app)
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] }
})

app.use(cors())
app.use(express.json())

// ─────────────────────────────────────────────
//  CONSTANTS
// ─────────────────────────────────────────────
const SPEED          = 0.00006   // multiplier growth rate per ms  (e^(SPEED*t))
const WAITING_MS     = 5000      // between-round gap
const TICK_MS        = 100       // multiplier broadcast interval

// ─────────────────────────────────────────────
//  GAME STATE
// ─────────────────────────────────────────────
const state = {
  phase          : 'waiting',   // waiting | flying | crashed
  roundId        : 0,
  serverSeed     : '',
  serverSeedHash : '',
  clientSeed     : 'betika-client-2024',
  nonce          : 0,
  crashMultiplier: 1.00,
  startTime      : 0,
  bets           : {},          // socketId → bet object
  history        : [],          // last 30 crash values
  countdown      : 5,
}

// ─────────────────────────────────────────────
//  PROVABLY FAIR CRASH GENERATION
//  hash = HMAC_SHA256(serverSeed, clientSeed:nonce)
//  decimal in [0,1)
//  if decimal < 0.01  → crash = 1.00
//  else               → crash = max(1.00, floor(100 / (1 - decimal)) / 100)
// ─────────────────────────────────────────────
function generateCrash (serverSeed, clientSeed, nonce) {
  const hmac    = crypto.createHmac('sha256', serverSeed)
  hmac.update(`${clientSeed}:${nonce}`)
  const hash    = hmac.digest('hex')
  const decimal = parseInt(hash.slice(0, 8), 16) / 0xFFFFFFFF

  if (decimal < 0.01) return 1.00
  return Math.max(1.00, Math.floor(100 * (1 / (1 - decimal))) / 100)
}

function newServerSeed () {
  return crypto.randomBytes(32).toString('hex')
}

function hashServerSeed (seed) {
  return crypto.createHash('sha256').update(seed).digest('hex')
}

// ─────────────────────────────────────────────
//  MULTIPLIER AT TIME t (ms since flight start)
// ─────────────────────────────────────────────
function multiplierAt (elapsedMs) {
  return Math.exp(SPEED * elapsedMs)
}

// ─────────────────────────────────────────────
//  ROUND LIFECYCLE
// ─────────────────────────────────────────────
let ticker    = null
let waiter    = null

function startWaiting () {
  state.phase   = 'waiting'
  state.bets    = {}
  state.countdown = 5
  state.roundId++
  state.nonce++

  // Pre-generate next round's crash so hash can be published before flight
  state.serverSeed      = newServerSeed()
  state.serverSeedHash  = hashServerSeed(state.serverSeed)
  state.crashMultiplier = generateCrash(state.serverSeed, state.clientSeed, state.nonce)

  console.log(`[Round ${state.roundId}] Waiting — crash will be ${state.crashMultiplier}x (hash: ${state.serverSeedHash.slice(0,16)}…)`)

  io.emit('roundStart', {
    roundId       : state.roundId,
    serverSeedHash: state.serverSeedHash,
    nonce         : state.nonce,
    countdown     : 5,
  })

  // Tick the countdown every second
  let cd = 5
  const cdTick = setInterval(() => {
    cd--
    state.countdown = cd
    if (cd > 0) {
      io.emit('countdown', { countdown: cd })
    } else {
      clearInterval(cdTick)
      startFlight()
    }
  }, 1000)
}

function startFlight () {
  state.phase     = 'flying'
  state.startTime = Date.now()

  console.log(`[Round ${state.roundId}] Flying — crash at ${state.crashMultiplier}x`)

  io.emit('flightStart', { roundId: state.roundId, startTime: state.startTime })

  ticker = setInterval(() => {
    const elapsedMs = Date.now() - state.startTime
    const mult      = multiplierAt(elapsedMs)

    if (mult >= state.crashMultiplier) {
      clearInterval(ticker)
      docrash()
    } else {
      io.emit('multiplierUpdate', {
        multiplier: parseFloat(mult.toFixed(2)),
        elapsedMs,
      })
    }
  }, TICK_MS)
}

function docrash () {
  state.phase = 'crashed'

  // Mark uncashed bets as lost
  for (const bet of Object.values(state.bets)) {
    if (bet.status === 'active') bet.status = 'lost'
  }

  const crashAt = parseFloat(state.crashMultiplier.toFixed(2))
  state.history.unshift(crashAt)
  if (state.history.length > 30) state.history.pop()

  console.log(`[Round ${state.roundId}] CRASHED at ${crashAt}x`)

  io.emit('crash', {
    roundId        : state.roundId,
    crashMultiplier: crashAt,
    serverSeed     : state.serverSeed,
    clientSeed     : state.clientSeed,
    nonce          : state.nonce,
    history        : state.history,
  })

  waiter = setTimeout(startWaiting, WAITING_MS)
}

// ─────────────────────────────────────────────
//  SOCKET.IO
// ─────────────────────────────────────────────
io.on('connection', socket => {
  console.log(`Socket connected: ${socket.id}`)

  // Sync new client to current state
  socket.emit('gameState', {
    phase          : state.phase,
    roundId        : state.roundId,
    serverSeedHash : state.serverSeedHash,
    nonce          : state.nonce,
    history        : state.history,
    countdown      : state.countdown,
    startTime      : state.startTime,
    crashMultiplier: state.phase === 'crashed' ? state.crashMultiplier : undefined,
    multiplier     : state.phase === 'flying'
      ? parseFloat(multiplierAt(Date.now() - state.startTime).toFixed(2))
      : 1.00,
    bets           : Object.values(state.bets),
  })

  // ── PLACE BET ──
  socket.on('placeBet', ({ playerName, amount }) => {
    if (state.phase !== 'waiting') {
      return socket.emit('betError', { message: 'Bets only accepted before flight starts.' })
    }
    if (!amount || amount < 10) {
      return socket.emit('betError', { message: 'Minimum bet is KES 10.' })
    }
    const bet = {
      socketId  : socket.id,
      playerName: playerName || `P${socket.id.slice(0,4)}`,
      amount    : parseFloat(amount),
      status    : 'active',   // active | cashed | lost
      cashedAt  : null,
      win       : null,
      avatar    : randomAvatar(),
      color     : randomColor(),
    }
    state.bets[socket.id] = bet
    socket.emit('betPlaced', bet)
    io.emit('playerBet', { playerName: bet.playerName, amount: bet.amount })
  })

  // ── CASH OUT ──
  socket.on('cashOut', () => {
    if (state.phase !== 'flying') {
      return socket.emit('cashOutError', { message: 'Not in flight.' })
    }
    const bet = state.bets[socket.id]
    if (!bet || bet.status !== 'active') {
      return socket.emit('cashOutError', { message: 'No active bet to cash out.' })
    }
    const elapsedMs  = Date.now() - state.startTime
    const currentMult = parseFloat(multiplierAt(elapsedMs).toFixed(2))

    bet.status   = 'cashed'
    bet.cashedAt = currentMult
    bet.win      = parseFloat((bet.amount * currentMult).toFixed(2))

    socket.emit('cashOutSuccess', { cashedAt: bet.cashedAt, win: bet.win })
    io.emit('playerCashOut', {
      playerName: bet.playerName,
      cashedAt  : bet.cashedAt,
      win       : bet.win,
    })
    console.log(`${bet.playerName} cashed out at ${bet.cashedAt}x — win KES ${bet.win}`)
  })

  socket.on('disconnect', () => {
    console.log(`Socket disconnected: ${socket.id}`)
  })
})

// ─────────────────────────────────────────────
//  REST ENDPOINTS
// ─────────────────────────────────────────────

// Current live multiplier
app.get('/api/current', (req, res) => {
  const elapsedMs  = state.phase === 'flying' ? Date.now() - state.startTime : 0
  res.json({
    phase     : state.phase,
    roundId   : state.roundId,
    multiplier: state.phase === 'flying'
      ? parseFloat(multiplierAt(elapsedMs).toFixed(2))
      : 1.00,
    countdown : state.countdown,
  })
})

// History of last 30 crashes
app.get('/api/history', (req, res) => {
  res.json({ history: state.history })
})

// Provably fair verification for LAST completed round
app.get('/api/verify', (req, res) => {
  const decimal = parseInt(
    crypto.createHmac('sha256', state.serverSeed)
      .update(`${state.clientSeed}:${state.nonce}`)
      .digest('hex').slice(0, 8),
    16
  ) / 0xFFFFFFFF

  res.json({
    roundId        : state.roundId,
    serverSeed     : state.serverSeed,
    serverSeedHash : state.serverSeedHash,
    clientSeed     : state.clientSeed,
    nonce          : state.nonce,
    decimal        : decimal.toFixed(8),
    crashMultiplier: state.crashMultiplier,
    verified       : hashServerSeed(state.serverSeed) === state.serverSeedHash,
  })
})

// Admin: start round manually
app.post('/api/startRound', (req, res) => {
  if (state.phase === 'flying') return res.status(400).json({ error: 'Round already in progress.' })
  clearTimeout(waiter)
  startWaiting()
  res.json({ started: true, roundId: state.roundId })
})

// ─────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────
const AVATAR_COLORS = ['#ef4444','#f97316','#eab308','#22c55e','#3b82f6','#8b5cf6','#ec4899','#14b8a6']
const AVATAR_LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ'
function randomColor  () { return AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)] }
function randomAvatar () { return AVATAR_LETTERS[Math.floor(Math.random() * AVATAR_LETTERS.length)] }

// ─────────────────────────────────────────────
//  START
// ─────────────────────────────────────────────
const PORT = process.env.PORT || 3001
server.listen(PORT, () => {
  console.log(`\n🚀 Aviator server on http://localhost:${PORT}`)
  console.log(`   Socket.io ready`)
  console.log(`   REST: GET /api/current | /api/history | /api/verify\n`)
  startWaiting()
})
