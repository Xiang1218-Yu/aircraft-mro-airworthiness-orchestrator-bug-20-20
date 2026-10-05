import type { AirworthinessRelease } from '../domain/types.js';
import { Store } from '../store/store.js';

export class AirworthinessReleaseService {
  constructor(private readonly store: Store) {}

  issue(workOrderId: string, issuedAt: string): AirworthinessRelease {
    const current = [...this.store.airworthinessReleases.values()].filter((item) => item.workOrderId === workOrderId).sort((a, b) => b.version - a.version)[0];
    const release: AirworthinessRelease = { id: this.store.nextId('release'), workOrderId, version: (current?.version ?? 0) + 1, status: 'RELEASED', issuedAt };
    this.store.airworthinessReleases.set(release.id, release);
    return release;
  }

  revoke(releaseId: string): void {
    const release = this.store.airworthinessReleases.get(releaseId);
    if (!release) throw new Error('release not found');
    release.status = 'REVOKED';
    this.store.addAudit('system', 'airworthiness.revoked', 'release', release.id, { version: release.version });
  }

  applyReleasedEvent(releaseId: string): void {
    const release = this.store.airworthinessReleases.get(releaseId);
    if (!release) throw new Error('release not found');
    const order = this.store.workOrders.get(release.workOrderId);
    if (!order) throw new Error('work order not found');
    // BUG: event handling trusts the event payload and ignores revoked status/version.
    order.status = 'CLOSED';
    order.version += 1;
    this.store.addAudit(order.tenantId, 'airworthiness.event-applied', 'work-order', order.id, { releaseId, releaseVersion: release.version });
    this.store.addOutbox(order.tenantId, 'mro.airworthiness-released', order.id, { workOrderId: order.id, releaseId });
  }
}
