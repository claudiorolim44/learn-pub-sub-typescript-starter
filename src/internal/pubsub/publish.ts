import { type ConfirmChannel } from 'amqplib'

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(value))

  return new Promise<void>((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: 'application/json' },
      (err) => {
        if (err) {
          reject(
            new Error("Message wasn't confirmed by the broker", { cause: err }),
          )
        } else {
          resolve()
        }
      },
    )
  })
}
