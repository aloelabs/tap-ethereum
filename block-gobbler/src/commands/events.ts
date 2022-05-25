import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, intervalFlags, rpcFlags} from '../utils/flags'
import {EventData} from 'web3-eth-contract/types'
import * as fs from 'node:fs'
import pLimit from 'p-limit'
import {range} from 'lodash'
import Heap from 'heap-js'

// TODO: make events work
// Start naive with a single call

type EventsInRange = {
  rangeStart: number
  rangeEnd: number
  events: EventData[]
}

export default class Events extends Command {
  static description = 'describe the command here'

  static flags = {
    ...intervalFlags,
    ...rpcFlags,
    ...contractFlags,
    maxBlockRange: Flags.integer({
      default: 10_000,
      description: "Max block range supported by RPC node's eth_getLogs",
    }),
    concurrency: Flags.integer({default: 100}),
    event: Flags.string({required: true}),
  }

  static args = [{name: 'file'}]

  public async run(): Promise<void> {
    const {flags} = await this.parse(Events)

    const web3 = new Web3(flags.rpc)

    const abi = JSON.parse(
      flags.abiFile ? fs.readFileSync(flags.abiFile, 'utf-8') : flags.abi!,
    )

    const contract = new web3.eth.Contract(abi, flags.address)

    const endBlock =
      flags.endBlock ?? (await web3.eth.getBlockNumber()) - flags.confirmations

    // TODO: figure out code reuse for concurrecny stuff
    // can I move it up into higher lvel functions or class?

    let lastRangeEnd = flags.startBlock - 1

    const blockRanges = range(
      flags.startBlock,
      endBlock + 1,
      flags.maxBlockRange,
    ).map(batchStartBlock => [
      batchStartBlock,
      Math.min(batchStartBlock + flags.maxBlockRange - 1, endBlock),
    ])

    const concurrencyLimit = pLimit(flags.concurrency)

    const minHeap = new Heap<EventsInRange>(
      (a, b) => a.rangeStart - b.rangeStart,
    )

    const rangePromises: Promise<unknown>[] = []

    rangePromises.push(
      ...blockRanges.map(([rangeStart, rangeEnd]) =>
        concurrencyLimit(async () => {
          const events = await contract.getPastEvents(flags.event, {
            fromBlock: rangeStart,
            toBlock: rangeEnd,
          })

          minHeap.push({
            rangeStart,
            rangeEnd,
            events,
          })

          while (minHeap.peek()?.rangeStart == lastRangeEnd + 1) {
            const eventsInRange = minHeap.pop()!
            for (const event of eventsInRange.events) {
              console.log(JSON.stringify(event))
            }

            lastRangeEnd = eventsInRange.rangeEnd
          }
        }),
      ),
    )

    await Promise.all(rangePromises)
  }
}
