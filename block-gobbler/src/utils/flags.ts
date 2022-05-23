import { Flags } from "@oclif/core";

export const contractFlags = {
    abi: Flags.string({required: true}),
    address: Flags.string({ char: 'a', required: true }),
}

export const rpcFlags = {
    batchSize: Flags.integer({char: 'b', required: true}),
    concurrency: Flags.integer({ char: 'c', required: true }),
    startBlock: Flags.integer({ char: 's', default: 0 }),
    confirmations: Flags.integer({ char: 's', default: 12 }),
    ethereumRpc: Flags.string({ char: 'r', required: true })
}