import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, rpcFlags} from '../utils/flags'
import * as fs from 'node:fs'
import {Example} from '@oclif/core/lib/interfaces'
import {AsyncBatchRequest} from '../utils/batch'
import {flatten, range} from 'lodash'
import pLimit from 'p-limit'

export default class Getters extends Command {
  static description = 'describe the command here'

  static flags = {
    ...rpcFlags,
    ...contractFlags,
    getter: Flags.string({char: 'g', required: true, multiple: true}), // multicall
  }

  static examples: Example[] = [
    './bin/dev getters --rpc https://misty-wandering-fog.quiknode.pro/5c2f9b4a29a2901ec9a3873bd105246553faf69d/ --abi ../examples/aloe-blend/abi.json --address 0x33cB657E7fd57F1f2d5f392FB78D5FA80806d1B4 -g getInventory -s 14170520 -b 1000 -c 2',
  ]

  public async run(): Promise<void> {
    const {flags} = await this.parse(Getters)

    const web3 = new Web3(flags.rpc)

    const abiJson = JSON.parse(fs.readFileSync(flags.abi, 'utf-8'))

    const contract = new web3.eth.Contract(abiJson, flags.address)

    // const endBlock = await web3.eth.getBlockNumber()
    const endBlock = flags.startBlock + 1000

    const batches = range(flags.startBlock, endBlock + 1, flags.batchSize).map(
      batchStartBlock =>
        range(
          batchStartBlock,
          Math.min(endBlock + 1, batchStartBlock + flags.batchSize),
        ),
    )

    const concurrencyLimit = pLimit(flags.concurrency)

    const batchPromises: Promise<unknown>[] = []

    for (const getterName of flags.getter) {
      batchPromises.push(
        ...batches.map(batchBlocks =>
          concurrencyLimit(() => {
            const batch = new AsyncBatchRequest(web3)
            for (const block of batchBlocks) {
              batch.add(contract.methods[getterName]().call.request, block)
            }

            return batch.execute()
          }),
        ),
      )
    }

    const batchesData = await Promise.all(batchPromises)
    const data = flatten(batchesData)
    console.log(data)

    // console.log(batches, null, 4)

    // const batch = new AsyncBatchRequest(web3)
    // batch.add(contract.methods.getInventory().call.request, flags.startBlock)
    // batch.add(
    //   contract.methods.getInventory().call.request,
    //   flags.startBlock + 10_000,
    // )
    // const data = await batch.execute()
    // console.log(data)
  }
}
