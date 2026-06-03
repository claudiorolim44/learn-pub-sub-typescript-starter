import amqp, { type Channel } from 'amqplib'

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel()

  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    exclusive: queueType !== SimpleQueueType.Durable,
    autoDelete: queueType !== SimpleQueueType.Durable,
  })

  await ch.bindQueue(queue.queue, exchange, key)
  return [ch, queue]
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
  const [channel, assertQueue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
  )

  const queue = assertQueue.queue

  await channel.consume(queue, (msg) => {
    if (msg === null) return

    let data: T
    try {
      data = JSON.parse(msg.content.toString()) as T
    } catch (err) {
      console.error('Could not unmarshal message:', err)
      // JSON inválido normalmente não adianta reenfileirar,
      // porque vai falhar de novo do mesmo jeito.
      channel.nack(msg, false, false)

      return
    }

    handler(data)
    channel.ack(msg)
  })
}
