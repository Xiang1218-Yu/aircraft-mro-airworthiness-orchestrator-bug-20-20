import type { ExternalEvent } from '../domain/types.js';
import { Store } from '../store/store.js';

export type ExternalEventResult = 'APPLIED' | 'IGNORED' | 'DUPLICATE';

export class ExternalEventService {
  constructor(private readonly store: Store) {}

  static key(event: ExternalEvent): string {
    return [event.tenantId, event.source, event.type, event.entityId, event.externalId].join('|');
  }

  /**
   * 以“先执行业务/审计，成功后再登记去重标记”的顺序处理外部事件：
   * - apply() 必须在调用方的互斥锁内执行，并保证在同一把锁内完成全部状态写入；
   * - apply() 返回 true（已生效）或 false（陈旧/冲突，仅留审计）后才登记事件，
   *   这样 apply 抛错时不会留下“已处理”标记，可安全重试；
   * - 同 key 事件再次到达且 payloadHash 一致即 DUPLICATE，不再产生任何副作用；
   * - payloadHash 不一致属于同一事件 ID 携带了不同内容（投毒/串号），直接拒绝。
   */
  process(event: ExternalEvent, apply: () => boolean): ExternalEventResult {
    const key = ExternalEventService.key(event);
    const existing = this.store.externalEvents.get(key);
    if (existing) {
      if (existing.payloadHash !== event.payloadHash) throw new Error('external event fingerprint conflict');
      return 'DUPLICATE';
    }
    const changed = apply();
    this.store.externalEvents.set(key, event);
    return changed ? 'APPLIED' : 'IGNORED';
  }
}
