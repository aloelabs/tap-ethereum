import {BatchRequest} from 'web3-core/types'
import Web3 from 'web3'

export class AsyncBatchRequest {
  batch: BatchRequest
  promises: Promise<unknown>[]

  constructor(web3: Web3) {
    this.batch = new web3.BatchRequest()
    this.promises = []
  }

  add(request: any, ...params: any) {
    const promise = new Promise((resolve, reject) => {
      this.batch.add(
        request.call(null, ...params, (err: any, data: any) => {
          if (err) return reject(err)
          resolve([...params, data])
        }),
      )
    })
    this.promises.push(promise)
  }

  async execute() {
    this.batch.execute()
    return await Promise.all(this.promises)
  }
}
