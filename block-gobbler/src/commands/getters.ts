import {Command, Flags} from '@oclif/core'
import Web3 from 'web3'
import {contractFlags, rpcFlags} from '../utils/flags'
import * as fs from 'node:fs'
import {Example} from '@oclif/core/lib/interfaces'

export default class Getters extends Command {
  static description = 'describe the command here'

  static flags = {
    ...rpcFlags,
    ...contractFlags,
    getter: Flags.string({char: 'g', required: true, multiple: true}), // multicall
  }

  static examples: Example[] = [
    './bin/dev getters --rpc https://eth-mainnet.alchemyapi.io/v2/K-4XLGXimXhs-VxcLLUoxhpbJIFROYbz --abi ../examples/aloe-blend/abi.json --address 0x33cB657E7fd57F1f2d5f392FB78D5FA80806d1B4 -g getInventory',
  ]

  public async run(): Promise<void> {
    const {flags} = await this.parse(Getters)

    const web3 = new Web3(flags.rpc)

    const abiJson = JSON.parse(fs.readFileSync(flags.abi, 'utf-8'))

    const contract = new web3.eth.Contract(abiJson, flags.address)

    console.log(contract)
  }
}
