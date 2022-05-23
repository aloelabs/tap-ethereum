import {Command, Flags} from '@oclif/core'
import { contractFlags, rpcFlags } from '../utils/flags'

export default class Events extends Command {
  static description = 'describe the command here'

  static flags = {
    ...rpcFlags,
    ...contractFlags,
    event: Flags.string({ char: 'e', multiple: true, required: true })
  }

  static args = [{name: 'file'}]

  public async run(): Promise<void> {
    const {flags} = await this.parse(Events)
  }
}
