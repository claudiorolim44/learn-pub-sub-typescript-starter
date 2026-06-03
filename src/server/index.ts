import amqp from 'amqplib'
import { publishJSON } from '../internal/pubsub/publish.js'
import { ExchangePerilDirect, PauseKey } from '../internal/routing/routing.js'
import type { PlayingState } from '../internal/gamelogic/gamestate.js'
import { getInput, printServerHelp } from '../internal/gamelogic/gamelogic.js'
import { declareAndBind, SimpleQueueType } from '../internal/pubsub/consume.js'

async function main() {
  const rabbitConnString = 'amqp://guest:guest@localhost:5672/'
  const conn = await amqp.connect(rabbitConnString)
  const channel = await conn.createConfirmChannel()
  console.log('Peril game server connected to RabbitMQ!')

  async function publishPause(publishValue: PlayingState): Promise<void> {
    try {
      await publishJSON<PlayingState>(
        channel,
        ExchangePerilDirect,
        PauseKey,
        publishValue,
      )
    } catch (err) {
      throw new Error('Error publishing pause/resume message', { cause: err })
    }
  }

  async function closeConnection(): Promise<void> {
    try {
      await conn.close()
      console.log('RabbitMQ connection closed.')
    } catch (err) {
      console.error('Error closing RabbitMQ connection:', err)
    }
  }

  ;['SIGINT', 'SIGTERM'].forEach((signal) =>
    process.on(signal, () => {
      void closeConnection().finally(() => process.exit(0))
    }),
  )

  await declareAndBind(
    conn,
    ExchangePerilDirect,
    'game_logs',
    'game_logs.*',
    SimpleQueueType.Durable,
  )

  printServerHelp()

  let running = true
  try {
    while (running) {
      const words: string[] = await getInput()

      if (words.length === 0) continue

      switch (words[0]) {
        case 'pause':
          console.log('Sending a pause message')
          await publishPause({ isPaused: true })
          break

        case 'resume':
          console.log('Sending a resume message')
          await publishPause({ isPaused: false })
          break

        case 'help':
          printServerHelp()
          break

        case 'quit':
          console.log("You're exiting")
          running = false
          break

        default:
          console.log("I don't understand the command.")
      }
    }
  } finally {
    await closeConnection()
    process.exit(0)
  }
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
