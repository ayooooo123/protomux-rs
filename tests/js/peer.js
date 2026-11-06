// npm protomux peer for tests/interop.rs.
// usage: node peer.js connect <port>   (TCP client, Noise initiator)
//        node peer.js listen           (prints "port <n>", Noise responder)
const net = require('net')
const SecretStream = require('@hyperswarm/secret-stream')
const Protomux = require('protomux')
const c = require('compact-encoding')

const log = (s) => process.stdout.write(s + '\n')
const [role, port] = process.argv.slice(2)

if (role === 'connect') run(net.connect(Number(port), '127.0.0.1'), true)
else {
  const server = net.createServer((socket) => {
    server.close()
    run(socket, false)
  })
  server.listen(0, '127.0.0.1', () => log('port ' + server.address().port))
}

function run(socket, isInitiator) {
  const ss = new SecretStream(isInitiator, socket)
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
