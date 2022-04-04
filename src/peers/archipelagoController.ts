import { Codec, JSONCodec, NatsConnection, Subscription } from 'nats'
import {
  ArchipelagoController,
  Island,
  IslandUpdates,
  PeerData,
  PeerPositionChange,
  UpdatableArchipelagoParameters,
  UpdateSubscriber
} from '@dcl/archipelago'

export class ArchipelagoNats implements ArchipelagoController {
  private nc: NatsConnection
  private jc: Codec<unknown>
  private updates: Subscription
  private updateSubscribers: Set<UpdateSubscriber> = new Set()

  constructor(nc: NatsConnection) {
    this.nc = nc
    this.jc = JSONCodec()
    this.updates = this.nc.subscribe('archipelago.subscribeToUpdates')
    ;(async () => {
      for await (const message of this.updates) {
        try {
          for await (const subscriber of this.updateSubscribers) {
            const data = this.jc.decode(message.data)
            console.log(`[${this.updates.getProcessed()}]: ${message.subject}: ${JSON.stringify(data, null, 2)}`)
            await subscriber(data as IslandUpdates)
          }
        } catch (err) {
          console.log(err)
        }
      }
    })()
  }

  flush(): void {
    this.nc.publish('archipelago.flush')
  }

  setPeersPositions(...requests: PeerPositionChange[]): void {
    this.nc.publish('archipelago.setPeersPositions', this.jc.encode(requests))
  }

  async getIslands(): Promise<Island[]> {
    const message = await this.nc.request('archipelago.getIslands')
    return this.jc.decode(message.data) as Island[]
  }

  async getIsland(id: string): Promise<Island | undefined> {
    const message = await this.nc.request('archipelago.getIsland', this.jc.encode(id))
    return this.jc.decode(message.data) as Promise<Island | undefined>
  }

  clearPeers(...ids: string[]): void {
    this.nc.publish('archipelago.clearPeers', this.jc.encode(ids))
  }

  modifyOptions(options: UpdatableArchipelagoParameters): void {
    this.nc.publish('archipelago.modifyOptions', this.jc.encode(options))
  }

  async getPeersCount(): Promise<number> {
    const message = await this.nc.request('archipelago.getPeersCount')
    return this.jc.decode(message.data) as number
  }

  async getIslandsCount(): Promise<number> {
    const message = await this.nc.request('archipelago.getIslandsCount')
    return this.jc.decode(message.data) as number
  }

  subscribeToUpdates(subscriber: UpdateSubscriber): void {
    this.updateSubscribers.add(subscriber)
  }

  unsubscribeFromUpdates(subscriber: UpdateSubscriber): void {
    this.updateSubscribers.delete(subscriber)
  }

  handleWorkerMessage(): void {}

  async dispose(): Promise<void> {
    this.nc.publish('archipelago.dispose')
  }

  async getPeerData(id: string): Promise<PeerData | undefined> {
    const message = await this.nc.request('archipelago.getPeerData', this.jc.encode(id))
    return this.jc.decode(message.data) as Promise<PeerData | undefined>
  }

  async getPeersData(ids: string[]): Promise<Record<string, PeerData>> {
    const message = await this.nc.request('archipelago.getPeersData', this.jc.encode(ids))
    return this.jc.decode(message.data) as Promise<Record<string, PeerData>>
  }

  async getPeerIds(): Promise<string[]> {
    const message = await this.nc.request('archipelago.getPeerIds')
    return this.jc.decode(message.data) as string[]
  }
}
