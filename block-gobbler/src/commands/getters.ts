import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, intervalFlags, rpcFlags} from '../utils/flags'
import * as fs from 'node:fs'
import {Example} from '@oclif/core/lib/interfaces'
import {AsyncBatchRequest} from '../utils/batch'
import {flatten, range} from 'lodash'
import pLimit from 'p-limit'

export default class Getters extends Command {
  static description = 'describe the command here'

  static flags = {
    ...intervalFlags,
    ...rpcFlags,
    ...contractFlags,
    getter: Flags.string({char: 'g', required: true, multiple: true}), // multicall
  }

  public async run(): Promise<void> {
    const {flags} = await this.parse(Getters)

    const web3 = new Web3(flags.rpc)

    const abiJson = JSON.parse(fs.readFileSync(flags.abi, 'utf-8'))

    const contract = new web3.eth.Contract(abiJson, flags.address)

    const endBlock = flags.endBlock ?? (await web3.eth.getBlockNumber())

    const blockNumberBatches = range(
      flags.startBlock,
      endBlock + 1,
      flags.batchSize,
    ).map(batchStartBlock =>
      range(
        batchStartBlock,
        Math.min(endBlock + 1, batchStartBlock + flags.batchSize),
      ),
    )

    const concurrencyLimit = pLimit(flags.concurrency)

    const batchPromises: Promise<unknown>[] = []

    // TODO: figure out multicall

    for (const getterName of flags.getter) {
      batchPromises.push(
        ...blockNumberBatches.map(blockNumbers =>
          concurrencyLimit(async () => {
            const batch = new AsyncBatchRequest(web3)
            for (const block of blockNumbers) {
              batch.add(contract.methods[getterName]().call.request, block)
            }

            const batchData = (await batch.execute()) as [number, any][]
            // TODO: put on priority queue and print out earlier
            return batchData.map(([blockNumber, result]) => {
              if (typeof result === 'object') {
                const resultArray = []
                for (let i = 0; i.toString() in result; i++) {
                  resultArray[i] = result[i.toString()]
                }

                result = resultArray
              }

              return [blockNumber, result]
            })
          }),
        ),
      )
    }

    const batchesData: any[] = await Promise.all(batchPromises)
    const data = flatten(batchesData)
    console.log(data)
  }
}
