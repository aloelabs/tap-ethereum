import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, intervalFlags, rpcFlags} from '../utils/flags'
import * as fs from 'node:fs'

// TODO: make events work
// Start naive with a single call

export default class Events extends Command {
  static description = 'describe the command here'

  static flags = {
    ...intervalFlags,
    ...rpcFlags,
    ...contractFlags,
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

    const pastEventsData = await contract.getPastEvents(flags.event, {
      fromBlock: flags.startBlock,
      toBlock: endBlock,
    })

    for (const eventData of pastEventsData) {
      console.log(JSON.stringify(eventData))
    }
  }
}
