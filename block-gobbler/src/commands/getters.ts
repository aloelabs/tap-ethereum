import {Command, Flags} from '@oclif/core'
import { contractFlags, rpcFlags } from '../utils/flags'

export default class Getters extends Command {
  static description = 'describe the command here'

  static flags = {
    ...rpcFlags,
    ...contractFlags,
    getter: Flags.string({ char: 'g', required: true, multiple: true }), // multicall
  }

  public async run(): Promise<void> {
    const {flags} = await this.parse(Getters)
  }
}
