import {Flags} from '@oclif/core'

export const contractFlags = {
  abiFile: Flags.string({exactlyOne: ['abiFile', 'abi']}), // TODO: make optional and use Etherscan as backup
  abi: Flags.string({}), // TODO: make optional and use Etherscan as backup
  address: Flags.string({char: 'a', required: true}),
}

export const intervalFlags = {
  startBlock: Flags.integer({char: 's', default: 0}),
  endBlock: Flags.integer(),
  confirmations: Flags.integer({default: 12}),
}

export const rpcFlags = {
  rpc: Flags.string({char: 'r', required: true}),
}

export const etherscanFlags = {
  etherscanApiKey: Flags.string({}),
}
