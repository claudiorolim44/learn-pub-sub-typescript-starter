import type {
  ArmyMove,
  RecognitionOfWar,
} from '../internal/gamelogic/gamedata.js'
import type {
  GameState,
  PlayingState,
} from '../internal/gamelogic/gamestate.js'
import { handleMove, MoveOutcome } from '../internal/gamelogic/move.js'
import { handlePause } from '../internal/gamelogic/pause.js'
import { AckType } from '../internal/pubsub/consume.js'
import { publishJSON } from '../internal/pubsub/publish.js'
import { type ConfirmChannel } from 'amqplib'
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from '../internal/routing/routing.js'
import { handleWar } from '../internal/gamelogic/war.js'
import { WarOutcome } from '../internal/gamelogic/war.js'

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps)
    process.stdout.write('> ')
    return AckType.Ack
  }
}

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    try {
      const outcome = handleMove(gs, move)
      switch (outcome) {
        case MoveOutcome.Safe:
        case MoveOutcome.SamePlayer:
          return AckType.Ack
        case MoveOutcome.MakeWar: {
          const recognition: RecognitionOfWar = {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          }
          await publishJSON(
            ch,
            ExchangePerilTopic,
            `${WarRecognitionsPrefix}.${gs.getUsername()}`,
            recognition,
          )

          return AckType.NackRequeue
        }
        default:
          return AckType.NackDiscard
      }
    } finally {
      process.stdout.write('> ')
    }
  }
}

export function handlerWar(gs: GameState) {
  return (recognition: RecognitionOfWar): AckType => {
    try {
      const outcome = handleWar(gs, recognition)
      switch (outcome.result) {
        case WarOutcome.NotInvolved:
          return AckType.NackRequeue
        case WarOutcome.NoUnits:
          return AckType.NackDiscard
        case WarOutcome.OpponentWon:
        case WarOutcome.YouWon:
        case WarOutcome.Draw:
          return AckType.Ack
        default:
          return AckType.NackDiscard
      }
    } finally {
      process.stdout.write('> ')
    }
  }
}
