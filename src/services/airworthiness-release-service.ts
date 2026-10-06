import type { AirworthinessRelease, AirworthinessReleasedEvent, WorkOrder } from '../domain/types.js';
import { Store } from '../store/store.js';

export type AirworthinessEventResult = 'APPLIED' | 'DUPLICATE' | 'STALE';

export class AirworthinessReleaseService {
  constructor(private readonly store: Store) {}

  async issue(workOrderId: string, issuedAt: string): Promise<AirworthinessRelease> {
    return this.store.lock.runExclusive(() => {
      const current = this.latestForWorkOrder(workOrderId);
      const release: AirworthinessRelease = {
        id: this.store.nextId('release'),
        workOrderId,
        version: (current?.version ?? 0) + 1,
        status: 'RELEASED',
        issuedAt,
        processedEventIds: [],
      };
      this.store.airworthinessReleases.set(release.id, release);
      return release;
    });
  }

  async revoke(releaseId: string): Promise<void> {
    await this.store.lock.runExclusive(() => {
      const release = this.store.airworthinessReleases.get(releaseId);
      if (!release) throw new Error('release not found');
      // Concurrent or repeated revocation must be a no-op.
      if (release.status === 'REVOKED') return;
      release.status = 'REVOKED';
      const order = this.store.workOrders.get(release.workOrderId);
      this.store.addAudit('system', 'airworthiness.revoked', 'release', release.id, { version: release.version });
      this.store.addOutbox(order?.tenantId ?? 'system', 'mro.airworthiness-revoked', release.workOrderId, { workOrderId: release.workOrderId, releaseId: release.id, releaseVersion: release.version });
    });
  }

  async applyReleasedEvent(event: AirworthinessReleasedEvent): Promise<AirworthinessEventResult> {
    return this.store.lock.runExclusive(() => {
      const release = this.store.airworthinessReleases.get(event.releaseId);
      if (!release) throw new Error('release not found');
      const order = this.store.workOrders.get(release.workOrderId);
      if (!order) throw new Error('work order not found');

      // Idempotent delivery: the bus may redeliver the same event.
      if (release.processedEventIds.includes(event.id)) return 'DUPLICATE';

      // The event must describe the release record it points at.
      if (event.tenantId !== order.tenantId || event.workOrderId !== release.workOrderId || event.releaseVersion !== release.version) {
        return this.rejectStale(event, release, order, 'payload-mismatch');
      }
      // A newer release supersedes the one this event was emitted for.
      const latest = this.latestForWorkOrder(release.workOrderId);
      if (!latest || latest.id !== release.id) {
        return this.rejectStale(event, release, order, 'superseded');
      }
      // Revoked after the event was emitted: audit only, never close the order.
      if (release.status === 'REVOKED') {
        return this.rejectStale(event, release, order, 'revoked');
      }
      // The work order changed after the event was emitted.
      if (order.version !== event.workOrderVersion) {
        return this.rejectStale(event, release, order, 'work-order-version-mismatch');
      }
      // Republished event for an already-closed order: no duplicate side effects.
      if (order.status === 'CLOSED') {
        release.processedEventIds.push(event.id);
        return 'DUPLICATE';
      }
      // Only COMPLETED -> CLOSED is a legal transition.
      if (order.status !== 'COMPLETED') {
        return this.rejectStale(event, release, order, 'illegal-state');
      }

      // Apply atomically: release record, work order, outbox and audit stay consistent.
      release.processedEventIds.push(event.id);
      order.status = 'CLOSED';
      order.version += 1;
      this.store.addAudit(order.tenantId, 'airworthiness.event-applied', 'work-order', order.id, { eventId: event.id, releaseId: release.id, releaseVersion: release.version, workOrderVersion: event.workOrderVersion });
      this.store.addOutbox(order.tenantId, 'mro.airworthiness-released', order.id, { workOrderId: order.id, releaseId: release.id, releaseVersion: release.version, eventId: event.id });
      return 'APPLIED';
    });
  }

  private rejectStale(event: AirworthinessReleasedEvent, release: AirworthinessRelease, order: WorkOrder, reason: string): AirworthinessEventResult {
    release.processedEventIds.push(event.id);
    this.store.addAudit(order.tenantId, 'airworthiness.event-rejected', 'release', release.id, {
      eventId: event.id,
      reason,
      releaseVersion: release.version,
      releaseStatus: release.status,
      workOrderId: order.id,
      workOrderVersion: order.version,
    });
    return 'STALE';
  }

  private latestForWorkOrder(workOrderId: string): AirworthinessRelease | undefined {
    return [...this.store.airworthinessReleases.values()]
      .filter((item) => item.workOrderId === workOrderId)
      .sort((a, b) => b.version - a.version)[0];
  }
}
