import {Flags} from '@oclif/core'

export const contractFlags = {
  abi: Flags.string({required: true}), // TODO: make optional and use Etherscan as backup
  address: Flags.string({char: 'a', required: true}),
}

export const rpcFlags = {
  batchSize: Flags.integer({char: 'b', default: 100}),
  concurrency: Flags.integer({char: 'c', default: 100}),
  startBlock: Flags.integer({char: 's', default: 0}),
  confirmations: Flags.integer({char: 's', default: 12}),
  rpc: Flags.string({char: 'r', required: true}),
}

export const etherscanFlags = {
  etherscanApiKey: Flags.string({}),
}
