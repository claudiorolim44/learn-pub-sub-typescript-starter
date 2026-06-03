import amqp from 'amqplib'
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from '../internal/gamelogic/gamelogic.js'
import { commandSpawn } from '../internal/gamelogic/spawn.js'
import { GameState } from '../internal/gamelogic/gamestate.js'
import { subscribeJSON, SimpleQueueType } from '../internal/pubsub/consume.js'
import { ExchangePerilDirect, PauseKey } from '../internal/routing/routing.js'
import { commandMove } from '../internal/gamelogic/move.js'
import { handlerPause } from './handlers.js'

async function main() {
  const rabbitConnString = 'amqp://guest:guest@localhost:5672/'
  const conn = await amqp.connect(rabbitConnString)
  console.log('Starting Peril client...')
  ;['SIGINT', 'SIGTERM'].forEach((signal) =>
    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    process.on(signal, async () => {
      try {
        await conn.close()
        console.log('RabbitMQ connection closed.')
      } catch (err) {
        console.error('Error closing RabbitMQ connection:', err)
      } finally {
        process.exit(0)
      }
    }),
  )

  const userName = await clientWelcome()

  const gameState = new GameState(userName)

  const finalHandlerPause = handlerPause(gameState)

  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    PauseKey + '.' + userName,
    PauseKey,
    SimpleQueueType.Transient,
    finalHandlerPause,
  )

  function printError(err: unknown) {
    console.log(err instanceof Error ? err.message : String(err))
  }

  let running = true
  while (running) {
    const words: string[] = await getInput()
    if (words.length === 0) {
      continue
    }
    const command = words[0]

    switch (command) {
      case 'move':
        try {
          commandMove(gameState, words)
        } catch (err) {
          printError(err)
        }
        break
      case 'status':
        await commandStatus(gameState)
        break
      case 'spawn':
        try {
          commandSpawn(gameState, words)
        } catch (err) {
          printError(err)
        }
        break
      case 'help':
        printClientHelp()
        break
      case 'quit':
        printQuit()
        running = false
        break
      case 'spam':
        console.log('Spamming not allowed yet!')
        break
      default:
        console.log('Error! Please, try another command.')
    }
  }
  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
