import {Command, Flags} from '@oclif/core'
import { rpcFlags } from '../utils/flags'

export default class Blocks extends Command {
  static description = 'describe the command here'
  
  static flags = {
    ...rpcFlags,
  }

  public async run(): Promise<void> {
    const {flags} = await this.parse(Blocks)
  }
}
