import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, intervalFlags, rpcFlags} from '../utils/flags'
import * as fs from 'node:fs'
import {AsyncBatchRequest, getBlockNumberBatches} from '../utils/batch'
import Heap from 'heap-js'
import pLimit from 'p-limit'

const callResultToArray = (result: any) => {
  let resultArray
  if (typeof result === 'object') {
    resultArray = []
    for (let i = 0; i.toString() in result; i++) {
      resultArray.push(result[i.toString()])
    }
  } else {
    resultArray = [result]
  }

  return resultArray
}

type GetterResult = [number, any[]]

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

    const abi = JSON.parse(
      flags.abiFile ? fs.readFileSync(flags.abiFile, 'utf-8') : flags.abi!,
    )

    const contract = new web3.eth.Contract(abi, flags.address)

    const endBlock =
      flags.endBlock ?? (await web3.eth.getBlockNumber()) - flags.confirmations

    const blockNumberBatches = getBlockNumberBatches(
      flags.startBlock,
      endBlock,
      flags.batchSize,
    )

    const concurrencyLimit = pLimit(flags.concurrency)

    const batchPromises: Promise<unknown>[] = []

    // TODO: figure out multicall

    const minHeap = new Heap<GetterResult>()

    let lastEmittedBlock = flags.startBlock - 1

    for (const getterName of flags.getter) {
      batchPromises.push(
        ...blockNumberBatches.map(blockNumbers =>
          concurrencyLimit(async () => {
            const batch = new AsyncBatchRequest(web3)
            for (const block of blockNumbers) {
              batch.add(contract.methods[getterName]().call.request, block)
            }

            const results = (await batch.execute()) as [number, any][]
            const data = results.map(
              ([blockNumber, result]) =>
                [blockNumber, callResultToArray(result)] as GetterResult,
            )
            minHeap.push(...data)
            while (minHeap.peek()?.[0] == lastEmittedBlock + 1) {
              // TODO: should it emit the full record or not?
              console.log(JSON.stringify(minHeap.pop()))
              lastEmittedBlock++
            }
          }),
        ),
      )
    }

    await Promise.all(batchPromises)
  }
}
