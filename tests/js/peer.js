// npm protomux peer for tests/interop.rs, over Hyperswarm on a local HyperDHT testnet (UDX).
// usage: node peer.js connect   (topic client, Noise initiator)
//        node peer.js listen    (topic server, Noise responder)
// Both print "bootstrap <port>" first, for the Rust peer to join the same testnet.
const Hyperswarm = require('hyperswarm')
const createTestnet = require('hyperdht/testnet')
const Protomux = require('protomux')
const c = require('compact-encoding')

const log = (s) => process.stdout.write(s + '\n')
const [role] = process.argv.slice(2)
const TOPIC = Buffer.alloc(32, 0x3d)

// The server side announces before the client looks up: `listen` prints "ready" once announced,
// `connect` waits for a line on stdin saying the Rust server has announced.
;(async () => {
  const testnet = await createTestnet(3, { host: '127.0.0.1' })
  log('bootstrap ' + testnet.bootstrap[0].port)
  const swarm = new Hyperswarm({ bootstrap: testnet.bootstrap })
  let started = false
  swarm.on('connection', (conn) => {
    if (started) return conn.destroy()
    started = true
    run(conn)
  })
  if (role === 'listen') {
    await swarm.join(TOPIC, { server: true, client: false }).flushed()
    log('ready')
  } else {
    process.stdin.once('data', () => swarm.join(TOPIC, { server: false, client: true }))
  }
})()

function run(ss) {
  const mux = new Protomux(ss)
  ss.on('close', () => {
    log('stream-closed')
    process.exit(0)
  })
  ss.on('error', (err) => log('stream-error ' + err.message))

  // Rust opens first; we open on demand and echo.
  mux.pair({ protocol: 'rust/first' }, (id) => {
    const ch = mux.createChannel({
      protocol: 'rust/first',
      id,
      handshake: c.string,
      messages: [{ encoding: c.raw, onmessage: (m) => ch.messages[0].send(Buffer.concat([Buffer.from('echo:'), m])) }],
      onopen: (hs) => log('rust-first-open ' + id.toString() + ' ' + hs)
    })
    ch.open('js-ack')
  })

  // We open first; Rust pairs on demand. We send a corked batch once open.
  const first = mux.createChannel({
    protocol: 'js/first',
    id: Buffer.from('abc'),
    handshake: c.string,
    messages: [{ encoding: c.raw, onmessage: (m) => m.toString() === 'bye' && first.close() }],
    onopen(hs) {
      log('js-first-open ' + hs)
      mux.cork()
      for (const s of ['a', 'b', 'c']) first.messages[0].send(Buffer.from(s))
      mux.uncork()
    },
    onclose: () => log('js-first-closed')
  })
  first.open('hello-from-js')

  // Both sides open at once.
  const both = mux.createChannel({
    protocol: 'both',
    handshake: c.string,
    messages: [{ encoding: c.raw, onmessage: (m) => log('both-msg ' + m) }],
    onopen(hs) {
      log('both-open ' + hs)
      both.messages[0].send(Buffer.from('from-js'))
    }
  })
  both.open('js')

  // Rust streams into us; we report the byte count when asked.
  let received = 0
  const bulk = mux.createChannel({
    protocol: 'bulk',
    messages: [
      { encoding: c.raw, onmessage: (m) => (received += m.byteLength) },
      { encoding: c.raw, onmessage: () => bulk.messages[1].send(Buffer.from(String(received))) }
    ]
  })
  bulk.open()

  // We stream 4 MiB into Rust, honouring back-pressure.
  const chunk = Buffer.alloc(64 * 1024, 1)
  let sent = 0
  const pump = () => {
    while (sent < 4 * 1024 * 1024) {
      sent += chunk.byteLength
      if (!bulkJs.messages[0].send(chunk)) return
    }
    bulkJs.messages[1].send(Buffer.from(String(sent)))
    sent = Infinity
  }
  const bulkJs = mux.createChannel({
    protocol: 'bulk-js',
    messages: [{ encoding: c.raw }, { encoding: c.raw }],
    onopen: pump,
    ondrain: () => sent < Infinity && sent > 0 && pump()
  })
  bulkJs.open()

  // Rust declines this one.
  const rej = mux.createChannel({ protocol: 'rust/rejects', onclose: (remote) => log('js-rejected ' + remote) })
  rej.open()
}
