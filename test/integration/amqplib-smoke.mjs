import amqplib from 'amqplib'

const url = process.env.AMQP_URL ?? 'amqp://127.0.0.1:5672'
const exchange = 'test-ex'
const queue = 'test-q'
const routingKey = 'test-key'
const body = 'hello'

const conn = await amqplib.connect(url)

try {
  const ch = await conn.createChannel()
  await ch.assertExchange(exchange, 'direct', { durable: false })
  await ch.assertQueue(queue, { durable: false })
  await ch.bindQueue(queue, exchange, routingKey)

  const received = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out waiting for message')), 3000)

    ch.consume(queue, (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error('consumer cancelled unexpectedly'))
        return
      }

      ch.ack(msg)
      clearTimeout(timer)
      resolve(msg.content.toString('utf8'))
    })
      .catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
  })

  ch.publish(exchange, routingKey, Buffer.from(body))

  const message = await received
  if (message !== body) {
    throw new Error(`unexpected message ${message}`)
  }

  await ch.close()
} finally {
  await conn.close()
}

console.log('amqplib smoke test passed')
