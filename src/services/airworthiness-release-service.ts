import type { AirworthinessRelease, AirworthinessReleasedEvent, ExternalEvent } from '../domain/types.js';
import { Store } from '../store/store.js';
import { ExternalEventService } from './external-event-service.js';

export type AirworthinessEventResult =
  | 'APPLIED' // 放行事件生效，工单已关闭
  | 'IGNORED' // 旧/冲突事件，仅留下审计记录
  | 'DUPLICATE'; // 重复投递（含重试），完全无副作用

const EVENT_SOURCE = 'airworthiness';
const EVENT_TYPE = 'airworthiness.released';

export class AirworthinessReleaseService {
  constructor(
    private readonly store: Store,
    private readonly externalEvents: ExternalEventService,
  ) {}

  async issue(workOrderId: string, issuedAt: string): Promise<AirworthinessRelease> {
    const workOrder = this.store.workOrders.get(workOrderId);
    if (!workOrder) throw new Error('work order not found');
    return this.store.lock.runExclusive(() => {
      const current = this.latestRelease(workOrderId);
      const release: AirworthinessRelease = {
        id: this.store.nextId('release'),
        tenantId: workOrder.tenantId,
        workOrderId,
        // 快照事件被签发时的工单版本：事件到达后若工单已被推进，则视为陈旧
        workOrderVersion: workOrder.version,
        version: (current?.version ?? 0) + 1,
        status: 'RELEASED',
        issuedAt,
      };
      this.store.airworthinessReleases.set(release.id, release);
      this.store.addAudit(workOrder.tenantId, 'airworthiness.issued', 'release', release.id, {
        workOrderId,
        version: release.version,
        workOrderVersion: release.workOrderVersion,
      });
      return release;
    });
  }

  /**
   * 撤回放行。幂等且与事件处理互斥：
   * - 已撤回的放行再次撤回直接返回，不重复写审计/outbox；
   * - 若该撤回的放行当前正关闭着工单，则把工单回退到 COMPLETED、释放预留物料
   *   （旧放行事件已不可能再把它关闭——见 handleReleasedEvent 的版本/状态校验），
   *   并为撤回和客户通知各写一条 outbox；
   * - 若工单已不再由该放行关闭（事件未生效/已被更新版本取代），撤回仅更新放行状态。
   */
  revoke(releaseId: string, revokedAt: string): Promise<void> {
    return this.store.lock.runExclusive(() => {
      const release = this.store.airworthinessReleases.get(releaseId);
      if (!release) throw new Error('release not found');
      if (release.status === 'REVOKED') return;

      release.status = 'REVOKED';
      release.revokedAt = revokedAt;
      this.store.addAudit(release.tenantId, 'airworthiness.revoked', 'release', release.id, {
        workOrderId: release.workOrderId,
        version: release.version,
      });

      const order = this.store.workOrders.get(release.workOrderId);
      if (order && order.status === 'CLOSED' && order.closedByReleaseId === release.id) {
        order.status = 'COMPLETED';
        delete order.closedByReleaseId;
        order.version += 1;
        // 内联释放预留而非调用 InventoryService.releaseForWorkOrder：互斥锁不可重入，
        // 当前已持锁，再取锁会死锁
        this.releaseReservations(order, 'airworthiness revoked');
        this.store.addAudit(order.tenantId, 'airworthiness.revoke-reopened', 'work-order', order.id, {
          releaseId: release.id,
          releaseVersion: release.version,
        });
        this.store.addOutbox(order.tenantId, 'mro.airworthiness-revoked', order.id, {
          workOrderId: order.id,
          releaseId: release.id,
          releaseVersion: release.version,
        });
        // 客户通知单独成条；outbox 投递本身至少一次，消费者按 releaseId 幂等
        this.store.addOutbox(order.tenantId, 'mro.customer-release-revoked', order.id, {
          workOrderId: order.id,
          releaseId: release.id,
          releaseVersion: release.version,
        });
      }
    });
  }

  /**
   * 处理延迟/乱序/重试到达的 airworthiness.released 事件。
   * 去重检查与全部校验在同一把互斥锁内完成，保证与并发撤回的原子性。
   */
  handleReleasedEvent(event: AirworthinessReleasedEvent): Promise<AirworthinessEventResult> {
    const external = this.toExternalEvent(event);
    return this.store.lock.runExclusive(() =>
      this.externalEvents.process(external, () => this.evaluateAndApply(event)),
    );
  }

  /**
   * 在锁内重新校验事件携带的放行版本、工单版本与撤回状态（调用方持锁）。
   * 任何校验失败都只写审计（返回 false），绝不关闭工单、写业务 outbox 或触发通知。
   */
  private evaluateAndApply(event: AirworthinessReleasedEvent): boolean {
    const release = this.store.airworthinessReleases.get(event.releaseId);
    if (!release) {
      this.auditIgnored(event, 'RELEASE_NOT_FOUND', { workOrderId: '' });
      return false;
    }
    const order = this.store.workOrders.get(release.workOrderId);
    if (!order || order.id !== release.workOrderId) {
      this.auditIgnored(event, 'WORK_ORDER_NOT_FOUND', { workOrderId: release.workOrderId });
      return false;
    }
    const meta = { workOrderId: order.id, releaseVersion: release.version, workOrderVersion: order.version };

    if (release.tenantId !== order.tenantId || event.tenantId !== release.tenantId) {
      this.auditIgnored(event, 'TENANT_MISMATCH', meta);
      return false;
    }
    // 事件版本必须仍指向这条放行的当前版本
    if (event.releaseVersion !== release.version) {
      this.auditIgnored(event, 'RELEASE_VERSION_CONFLICT', meta);
      return false;
    }
    // 乐观并发：事件签发后工单已被推进，放行基于的是过期工单状态
    if (event.workOrderVersion !== order.version) {
      this.auditIgnored(event, 'WORK_ORDER_VERSION_CONFLICT', meta);
      return false;
    }
    // 已存在更新版本的放行，旧版本事件即便晚到也不得再生效
    const latest = this.latestRelease(order.id);
    if (latest && latest.id !== release.id) {
      this.auditIgnored(event, 'SUPERSEDED', { ...meta, latestReleaseId: latest.id, latestVersion: latest.version });
      return false;
    }
    // 撤回优先：撤回后到达（或并发撤回中）的旧事件只能留审计
    if (release.status === 'REVOKED') {
      this.auditIgnored(event, 'RELEASE_REVOKED', meta);
      return false;
    }
    // 幂等：工单已由同一条放行关闭（重复到达且去重标记缺失的极端情况），不重复产生副作用
    if (order.status === 'CLOSED') {
      if (order.closedByReleaseId === release.id) {
        this.auditIgnored(event, 'ALREADY_CLOSED', meta);
        return false;
      }
      this.auditIgnored(event, 'INVALID_STATE', { ...meta, currentStatus: order.status });
      return false;
    }
    if (order.status === 'CANCELLED' || order.status !== 'COMPLETED') {
      this.auditIgnored(event, 'INVALID_STATE', { ...meta, currentStatus: order.status });
      return false;
    }

    // 全部校验通过：关闭工单，审计 + 单条 outbox（出库/客户通知由下游按 releaseId 幂等消费）
    order.status = 'CLOSED';
    order.closedByReleaseId = release.id;
    order.version += 1;
    this.store.addAudit(order.tenantId, 'airworthiness.event-applied', 'work-order', order.id, {
      releaseId: release.id,
      eventId: event.eventId,
      releaseVersion: release.version,
      workOrderVersion: event.workOrderVersion,
    });
    this.store.addOutbox(order.tenantId, 'mro.airworthiness-released', order.id, {
      workOrderId: order.id,
      releaseId: release.id,
      releaseVersion: release.version,
      eventId: event.eventId,
    });
    return true;
  }

  private auditIgnored(
    event: AirworthinessReleasedEvent,
    reason:
      | 'RELEASE_NOT_FOUND'
      | 'WORK_ORDER_NOT_FOUND'
      | 'TENANT_MISMATCH'
      | 'RELEASE_VERSION_CONFLICT'
      | 'WORK_ORDER_VERSION_CONFLICT'
      | 'SUPERSEDED'
      | 'RELEASE_REVOKED'
      | 'ALREADY_CLOSED'
      | 'INVALID_STATE',
    metadata: Record<string, string | number | boolean>,
  ): void {
    this.store.addAudit(event.tenantId, 'airworthiness.event-ignored', 'release', event.releaseId, {
      eventId: event.eventId,
      reason,
      eventReleaseVersion: event.releaseVersion,
      eventWorkOrderVersion: event.workOrderVersion,
      ...metadata,
    });
  }

  private releaseReservations(order: { tenantId: string; id: string; reservations: string[] }, reason: string): void {
    for (const reservationId of order.reservations) {
      const reservation = this.store.reservations.get(reservationId);
      if (!reservation || reservation.status !== 'ACTIVE') continue;
      const stock = this.store.stocks.get(reservation.stockId);
      if (stock) {
        stock.reserved = Math.max(0, stock.reserved - reservation.quantity);
        stock.version += 1;
      }
      reservation.status = 'RELEASED';
      reservation.version += 1;
      this.store.addAudit(order.tenantId, 'inventory.released', 'reservation', reservation.id, {
        reason,
        quantity: reservation.quantity,
      });
    }
  }

  private latestRelease(workOrderId: string): AirworthinessRelease | undefined {
    return [...this.store.airworthinessReleases.values()]
      .filter((item) => item.workOrderId === workOrderId)
      .sort((a, b) => b.version - a.version)[0];
  }

  private toExternalEvent(event: AirworthinessReleasedEvent): ExternalEvent {
    const payloadHash = [event.releaseId, event.releaseVersion, event.workOrderVersion].join('#');
    return {
      tenantId: event.tenantId,
      source: EVENT_SOURCE,
      type: EVENT_TYPE,
      entityId: event.releaseId,
      externalId: event.eventId,
      payloadHash,
    };
  }
}
