import amqplib from 'amqplib'
import { spawn, execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import process from 'node:process'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(here, '..', '..')
const workDir = await mkdtemp(path.join(os.tmpdir(), 'erionn-mq-compat-'))
const binary = path.join(workDir, process.platform === 'win32' ? 'erionn-mq.exe' : 'erionn-mq')
const dataDir = path.join(workDir, 'data')
const amqpPort = await freePort()
const managementPort = await freePort()
const amqpURL = `amqp://127.0.0.1:${amqpPort}`
const managementURL = `http://127.0.0.1:${managementPort}`
const authHeader = `Basic ${Buffer.from('guest:guest').toString('base64')}`

let broker
let brokerLogs = []

try {
  await buildBroker()
  broker = await startBroker()

  await run('publish/consume/ack', testPublishConsumeAck)
  await run('nack + requeue', testNackAndRequeue)
  await run('prefetch', testPrefetch)
  await run('management API + auth', testManagementAPIAuth)
  await run('durable restart', testDurableRestart)
  await run('manual reconnect after restart', testReconnectAfterRestart)

  console.log('amqplib compatibility suite passed')
} catch (err) {
  console.error(err?.stack ?? err)
  if (brokerLogs.length > 0) {
    console.error('\nBroker logs:\n' + brokerLogs.join(''))
  }
  process.exitCode = 1
} finally {
  await stopBroker(broker)
  await rm(workDir, { recursive: true, force: true })
}

async function run(name, fn) {
  process.stdout.write(`- ${name}... `)
  await fn()
  console.log('ok')
}

async function buildBroker() {
  await execFileAsync('go', ['build', '-o', binary, './cmd'], { cwd: root })
}

async function startBroker() {
  brokerLogs = []
  const child = spawn(binary, [], {
    cwd: root,
    env: {
      ...process.env,
      ERIONN_AMQP_ADDR: `127.0.0.1:${amqpPort}`,
      ERIONN_MGMT_ADDR: `127.0.0.1:${managementPort}`,
      ERIONN_DATA_DIR: dataDir,
      ERIONN_MGMT_USERS: 'guest:guest:admin',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  child.stdout?.on('data', (chunk) => brokerLogs.push(chunk.toString('utf8')))
  child.stderr?.on('data', (chunk) => brokerLogs.push(chunk.toString('utf8')))
  await Promise.all([waitForPort(amqpPort, child), waitForPort(managementPort, child)])
  return child
}

async function restartBroker() {
  await stopBroker(broker)
  broker = await startBroker()
}

async function stopBroker(child) {
  if (!child || child.exitCode !== null) {
    return
  }
  child.kill()
  if (await waitForExit(child, 5000)) {
    return
  }
  if (process.platform === 'win32') {
    await execFileAsync('taskkill', ['/pid', String(child.pid), '/t', '/f']).catch(() => {})
  } else {
    child.kill('SIGKILL')
  }
  await waitForExit(child, 5000)
}

async function testPublishConsumeAck() {
  await withChannel(async (ch) => {
    const exchange = unique('compat.ex')
    const queue = unique('compat.q')
    const routingKey = unique('compat.key')
    await ch.assertExchange(exchange, 'direct', { durable: false })
    await ch.assertQueue(queue, { durable: false })
    await ch.bindQueue(queue, exchange, routingKey)

    const received = waitForOne(ch, queue, async (msg) => {
      const body = msg.content.toString('utf8')
      ch.ack(msg)
      return body
    })

    ch.publish(exchange, routingKey, Buffer.from('hello'))
    expect(await received === 'hello', 'unexpected body from publish/consume flow')
  })
}

async function testNackAndRequeue() {
  await withChannel(async (ch) => {
    const queue = unique('compat.nack')
    await ch.assertQueue(queue, { durable: false })

    ch.sendToQueue(queue, Buffer.from('drop'))
    const dropped = await waitForOne(ch, queue, async (msg) => {
      ch.nack(msg, false, false)
      return msg.content.toString('utf8')
    })
    expect(dropped === 'drop', 'expected first nack test message')
    await sleep(200)
    const afterDrop = await ch.checkQueue(queue)
    expect(afterDrop.messageCount === 0, 'nack without requeue should discard the message')

    ch.sendToQueue(queue, Buffer.from('retry'))
    const redelivered = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out waiting for requeued message')), 3000)
      let seen = 0
      ch.consume(queue, (msg) => {
        if (!msg) {
          clearTimeout(timer)
          reject(new Error('consumer cancelled unexpectedly'))
          return
        }
        seen++
        if (seen === 1) {
          ch.nack(msg, false, true)
          return
        }
        clearTimeout(timer)
        try {
          expect(msg.fields.redelivered, 'expected requeued message to be marked redelivered')
          ch.ack(msg)
          resolve(msg.content.toString('utf8'))
        } catch (err) {
          reject(err)
        }
      }, { noAck: false }).catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
    })
    expect(redelivered === 'retry', 'unexpected requeued body')
  })
}

async function testPrefetch() {
  await withChannel(async (ch) => {
    const queue = unique('compat.prefetch')
    await ch.assertQueue(queue, { durable: false })
    await ch.prefetch(1)
    ch.sendToQueue(queue, Buffer.from('first'))
    ch.sendToQueue(queue, Buffer.from('second'))

    let firstSeen
    let secondSeen
    let resolveFirst
    let resolveSecond
    const first = new Promise((resolve) => { resolveFirst = resolve })
    const second = new Promise((resolve) => { resolveSecond = resolve })
    const { consumerTag } = await ch.consume(queue, (msg) => {
      if (!msg) {
        return
      }
      if (!firstSeen) {
        firstSeen = msg
        resolveFirst(msg)
        return
      }
      secondSeen = msg
      resolveSecond(msg)
    }, { noAck: false })

    await withTimeout(first, 3000, 'first prefetch delivery')
    await sleep(250)
    expect(!secondSeen, 'second message should wait until the first is acked when prefetch=1')
    ch.ack(firstSeen)

    const secondMsg = await withTimeout(second, 3000, 'second prefetch delivery')
    expect(secondMsg.content.toString('utf8') === 'second', 'unexpected second prefetch body')
    ch.ack(secondMsg)
    await ch.cancel(consumerTag)
  })
}

async function testManagementAPIAuth() {
  const queue = unique('compat.mgmt')
  await withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
  })

  const unauthorized = await request('/api/overview')
  expect(unauthorized.status === 401, 'management API should require auth')

  const overview = await request('/api/overview', { auth: true })
  expect(overview.status === 200, 'authorized overview request should succeed')
  expect(typeof overview.body?.message_stats?.publish === 'number', 'overview should include message stats')

  const queues = await request('/api/queues', { auth: true })
  expect(queues.status === 200, 'authorized queues request should succeed')
  expect(Array.isArray(queues.body) && queues.body.some((item) => item.name === queue), 'management queue list should include declared queue')
}

async function testDurableRestart() {
  const queue = unique('compat.durable')
  await withConnection(async (conn) => {
    const ch = await conn.createConfirmChannel()
    try {
      await ch.assertQueue(queue, { durable: true })
      await publishConfirmed(ch, '', queue, Buffer.from('persisted'), { persistent: true })
    } finally {
      await ch.close().catch(() => {})
    }
  })

  await restartBroker()

  await withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: true })
    const received = await waitForOne(ch, queue, async (msg) => {
      ch.ack(msg)
      return msg.content.toString('utf8')
    })
    expect(received === 'persisted', 'durable message should survive restart')
  })
}

async function testReconnectAfterRestart() {
  const queue = unique('compat.reconnect')
  await withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
  })

  await restartBroker()

  await withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
    const received = waitForOne(ch, queue, async (msg) => {
      ch.ack(msg)
      return msg.content.toString('utf8')
    })
    ch.sendToQueue(queue, Buffer.from('after-reconnect'))
    expect(await received === 'after-reconnect', 'new connection should work after restart')
  })
}

async function withConnection(run) {
  const conn = await amqplib.connect(amqpURL)
  try {
    return await run(conn)
  } finally {
    await conn.close().catch(() => {})
  }
}

async function withChannel(run) {
  return withConnection(async (conn) => {
    const ch = await conn.createChannel()
    try {
      return await run(ch)
    } finally {
      await ch.close().catch(() => {})
    }
  })
}

async function waitForOne(ch, queue, handler) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for message on ${queue}`)), 3000)
    let consumerTag = ''
    ch.consume(queue, (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error('consumer cancelled unexpectedly'))
        return
      }
      Promise.resolve(handler(msg))
        .then(async (value) => {
          clearTimeout(timer)
          if (consumerTag) {
            await ch.cancel(consumerTag).catch(() => {})
          }
          resolve(value)
        })
        .catch((err) => {
          clearTimeout(timer)
          reject(err)
        })
    }, { noAck: false })
      .then((result) => {
        consumerTag = result.consumerTag
      })
      .catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
  })
}

async function publishConfirmed(ch, exchange, routingKey, body, options = {}) {
  await new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, body, options, (err) => {
      if (err) {
        reject(err)
        return
      }
      resolve()
    })
  })
}

async function request(resource, { auth = false } = {}) {
  const headers = auth ? { Authorization: authHeader } : {}
  const res = await fetch(`${managementURL}${resource}`, { headers })
  const text = await res.text()
  let body = text
  if (text) {
    try {
      body = JSON.parse(text)
    } catch {
      body = text
    }
  }
  return { status: res.status, body }
}

function unique(prefix) {
  return `${prefix}.${randomUUID().slice(0, 8)}`
}

function expect(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    sleep(ms).then(() => {
      throw new Error(`timed out: ${label}`)
    }),
  ])
}

async function waitForPort(port, child) {
  const started = Date.now()
  while (Date.now() - started < 10000) {
    if (child.exitCode !== null) {
      throw new Error(`broker exited before port ${port} was ready`)
    }
    try {
      await new Promise((resolve, reject) => {
        const socket = net.createConnection({ host: '127.0.0.1', port }, () => {
          socket.destroy()
          resolve()
        })
        socket.on('error', reject)
      })
      return
    } catch {
      await sleep(100)
    }
  }
  throw new Error(`timed out waiting for port ${port}`)
}

async function waitForExit(child, ms) {
  return Promise.race([
    once(child, 'exit').then(() => true),
    sleep(ms).then(() => false),
  ])
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('failed to allocate free port')))
        return
      }
      server.close((err) => {
        if (err) {
          reject(err)
          return
        }
        resolve(address.port)
      })
    })
    server.on('error', reject)
  })
}

function execFileAsync(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    execFile(command, args, options, (err, stdout, stderr) => {
      if (err) {
        err.stdout = stdout
        err.stderr = stderr
        reject(err)
        return
      }
      resolve({ stdout, stderr })
    })
  })
}
