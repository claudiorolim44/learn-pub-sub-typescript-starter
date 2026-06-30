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
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  PauseKey,
  WarRecognitionsPrefix,
} from '../internal/routing/routing.js'
import { commandMove } from '../internal/gamelogic/move.js'
import { handlerMove, handlerPause, handlerWar } from './handlers.js'
import type { ArmyMove } from '../internal/gamelogic/gamedata.js'
import { publishJSON } from '../internal/pubsub/publish.js'

async function main() {
  const rabbitConnString = 'amqp://guest:guest@localhost:5672/'
  const conn = await amqp.connect(rabbitConnString)
  const channel = await conn.createConfirmChannel()
  console.log('Starting Peril client...')

  async function publishInTopic(key: string, value: unknown): Promise<void> {
    try {
      await publishJSON(channel, ExchangePerilTopic, key, value)
    } catch (err) {
      throw new Error('Error publishing message', { cause: err })
    }
  }

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

  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    PauseKey + '.' + userName,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gameState),
  )

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${userName}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gameState, channel),
  )

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gameState),
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
      case 'spawn':
        try {
          commandSpawn(gameState, words)
        } catch (err) {
          printError(err)
        }
        break
      case 'move': {
        let armyMove: ArmyMove
        try {
          armyMove = commandMove(gameState, words)
        } catch (err) {
          printError(err)
          break
        }
        await publishInTopic(`${ArmyMovesPrefix}.${userName}`, armyMove)
        break
      }
      case 'status':
        await commandStatus(gameState)
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
