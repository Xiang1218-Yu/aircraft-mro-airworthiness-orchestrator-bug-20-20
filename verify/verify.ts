import assert from 'node:assert';
import type { AirworthinessReleasedEvent, PartStock, WorkOrder } from '../src/domain/types.js';
import { MroApplication } from '../src/app.js';

let failures = 0;
function check(name: string, fn: () => void): void {
  try {
    fn();
    console.log(`PASS  ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL  ${name}`);
    console.error(`      ${(error as Error).message.split('\n').join('\n      ')}`);
  }
}

async function checkAsync(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
    console.log(`PASS  ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL  ${name}`);
    console.error(`      ${(error as Error).message.split('\n').join('\n      ')}`);
  }
}

function freshApp(orderStatus: WorkOrder['status'] = 'COMPLETED'): MroApplication {
  const app = new MroApplication();
  app.seed();
  const order = app.store.workOrders.get('wo_demo_001')!;
  order.status = orderStatus;
  return app;
}

function releasedEventFor(app: MroApplication, releaseId: string, eventId: string, overrides: Partial<AirworthinessReleasedEvent> = {}): AirworthinessReleasedEvent {
  const release = app.store.airworthinessReleases.get(releaseId)!;
  return {
    eventId,
    releaseId: release.id,
    tenantId: release.tenantId,
    releaseVersion: release.version,
    workOrderVersion: release.workOrderVersion,
    ...overrides,
  };
}

// 1. 正常路径：事件校验通过才关闭工单，且只写一条业务 outbox
await checkAsync('1. valid event closes COMPLETED order, single outbox entry', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  const versionBefore = app.store.workOrders.get('wo_demo_001')!.version;
  const outboxBefore = app.store.outbox.length;
  const result = await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-1'));
  const order = app.store.workOrders.get('wo_demo_001')!;
  assert.equal(result, 'APPLIED');
  assert.equal(order.status, 'CLOSED');
  assert.equal(order.closedByReleaseId, release.id);
  assert.equal(order.version, versionBefore + 1);
  const business = app.store.outbox.slice(outboxBefore).filter((m) => m.topic === 'mro.airworthiness-released');
  assert.equal(business.length, 1);
});

// 2. 重复投递（总线重试）：完全幂等，无重复关闭/出库/通知
await checkAsync('2. duplicate redelivery is a no-op', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  const event = releasedEventFor(app, release.id, 'evt-dup');
  await app.airworthiness.handleReleasedEvent(event);
  const versionAfterFirst = app.store.workOrders.get('wo_demo_001')!.version;
  const outboxAfterFirst = app.store.outbox.length;
  const auditAfterFirst = app.store.audit.length;
  const retried = await app.airworthiness.handleReleasedEvent({ ...event });
  const order = app.store.workOrders.get('wo_demo_001')!;
  assert.equal(retried, 'DUPLICATE');
  assert.equal(order.status, 'CLOSED');
  assert.equal(order.version, versionAfterFirst);
  assert.equal(app.store.outbox.length, outboxAfterFirst);
  assert.equal(app.store.audit.length, auditAfterFirst);
});

// 3. 撤回后旧事件晚到：不能重新关闭工单，只留审计，无业务 outbox
await checkAsync('3. stale event arriving after revoke is audit-only', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  await app.airworthiness.revoke(release.id, '2026-10-06T09:00:00Z');
  const outboxBefore = app.store.outbox.length;
  const result = await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-late'));
  const order = app.store.workOrders.get('wo_demo_001')!;
  assert.equal(result, 'IGNORED');
  assert.equal(order.status, 'COMPLETED'); // 未重新关闭
  assert.ok(!order.closedByReleaseId);
  const after = app.store.outbox.slice(outboxBefore);
  assert.equal(after.length, 0); // 不触发出库 / 不发客户通知
  assert.ok(app.store.audit.some((a) => a.action === 'airworthiness.event-ignored' && a.metadata.reason === 'RELEASE_REVOKED'));
});

// 4. 被忽略的陈旧事件重试仍然幂等（第二次起 DUPLICATE，审计只有一条）
await checkAsync('4. retry of ignored stale event is idempotent (single audit)', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  await app.airworthiness.revoke(release.id, '2026-10-06T09:00:00Z');
  const event = releasedEventFor(app, release.id, 'evt-late-retry');
  const first = await app.airworthiness.handleReleasedEvent(event);
  const audits = app.store.audit.filter((a) => a.action === 'airworthiness.event-ignored').length;
  const second = await app.airworthiness.handleReleasedEvent({ ...event });
  assert.equal(first, 'IGNORED');
  assert.equal(second, 'DUPLICATE');
  assert.equal(app.store.audit.filter((a) => a.action === 'airworthiness.event-ignored').length, audits);
});

// 5. 事件先生效后撤回：工单回退 COMPLETED、释放预留、发撤回+客户通知 outbox
await checkAsync('5. revoke after applied event reopens order and emits outbox once', async () => {
  const app = freshApp();
  await app.inventory.reserveForWorkOrder(app.store.workOrders.get('wo_demo_001')!);
  // reserveForWorkOrder 把工单推到 MATERIALS_RESERVED，模拟工单推进到 COMPLETED
  const order0 = app.store.workOrders.get('wo_demo_001')!;
  order0.status = 'COMPLETED';
  const release = await app.airworthiness.issue(order0.id, '2026-10-06T08:00:00Z');
  await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-ok'));
  assert.equal(app.store.workOrders.get(order0.id)!.status, 'CLOSED');
  const outboxBeforeRevoke = app.store.outbox.length;
  await app.airworthiness.revoke(release.id, '2026-10-06T10:00:00Z');
  const order = app.store.workOrders.get(order0.id)!;
  assert.equal(order.status, 'COMPLETED');
  assert.ok(!order.closedByReleaseId);
  const newMessages = app.store.outbox.slice(outboxBeforeRevoke);
  assert.deepEqual(newMessages.map((m) => m.topic).sort(), ['mro.airworthiness-revoked', 'mro.customer-release-revoked']);
  // 预留物料被释放
  for (const rid of order.reservations) {
    assert.equal(app.store.reservations.get(rid)!.status, 'RELEASED');
  }
  const stock: PartStock = app.store.stocks.get('stock_demo_001')!;
  assert.equal(stock.reserved, 0);
  // 撤回后旧事件再来也不能重新关闭
  const late = await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-late-2'));
  assert.equal(late, 'IGNORED');
  assert.equal(app.store.workOrders.get(order0.id)!.status, 'COMPLETED');
});

// 6. 重复撤回幂等：审计/outbox 不重复
await checkAsync('6. duplicate revoke is idempotent', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-x'));
  const outboxBefore = app.store.outbox.length;
  const revokedAuditsBefore = app.store.audit.filter((a) => a.action === 'airworthiness.revoked').length;
  await app.airworthiness.revoke(release.id, '2026-10-06T10:00:00Z');
  await app.airworthiness.revoke(release.id, '2026-10-06T11:00:00Z');
  assert.equal(app.store.outbox.length, outboxBefore + 2);
  assert.equal(app.store.audit.filter((a) => a.action === 'airworthiness.revoked').length, revokedAuditsBefore + 1);
});

// 7. 并发：事件与撤回同时提交，两种顺序都必须保持一致
await checkAsync('7a. concurrent event+revoke: event first then revoke', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  const event = releasedEventFor(app, release.id, 'evt-race-a');
  const [r1, r2] = await Promise.all([
    app.airworthiness.handleReleasedEvent(event),
    app.airworthiness.revoke(release.id, '2026-10-06T09:00:00Z').then(() => 'REVOKED' as const),
  ]);
  const order = app.store.workOrders.get('wo_demo_001')!;
  assert.equal(r1, 'APPLIED');
  assert.equal(r2, 'REVOKED');
  assert.equal(order.status, 'COMPLETED');
  assert.ok(!order.closedByReleaseId);
  assert.equal(app.store.airworthinessReleases.get(release.id)!.status, 'REVOKED');
});

await checkAsync('7b. concurrent event+revoke: revoke first then stale event', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  const event = releasedEventFor(app, release.id, 'evt-race-b');
  const [r1, r2] = await Promise.all([
    app.airworthiness.revoke(release.id, '2026-10-06T09:00:00Z').then(() => 'REVOKED' as const),
    app.airworthiness.handleReleasedEvent(event),
  ]);
  const order = app.store.workOrders.get('wo_demo_001')!;
  assert.equal(r1, 'REVOKED');
  assert.equal(r2, 'IGNORED');
  assert.equal(order.status, 'COMPLETED');
  assert.ok(!order.closedByReleaseId);
  assert.equal(app.store.outbox.filter((m) => m.topic === 'mro.airworthiness-released').length, 0);
});

// 8. 同一事件 ID 携带不同内容（指纹冲突）必须拒绝
await checkAsync('8. payload hash conflict rejects redelivery', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-conflict'));
  await assert.rejects(
    () => app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-conflict', { workOrderVersion: 999 })),
    /fingerprint conflict/,
  );
});

// 9. 工单版本乐观锁：事件签发后工单被推进，事件失效
await checkAsync('9. event with stale work-order version is ignored', async () => {
  const app = freshApp();
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  app.store.workOrders.get('wo_demo_001')!.version += 1; // 事件签发后工单又被推进
  const result = await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-occ'));
  assert.equal(result, 'IGNORED');
  assert.notEqual(app.store.workOrders.get('wo_demo_001')!.status, 'CLOSED');
  assert.ok(app.store.audit.some((a) => a.metadata.reason === 'WORK_ORDER_VERSION_CONFLICT'));
  assert.equal(app.store.outbox.filter((m) => m.topic === 'mro.airworthiness-released').length, 0);
});

// 10. 被更新版本放行取代的旧事件不得生效
await checkAsync('10. superseded release event is ignored', async () => {
  const app = freshApp();
  const r1 = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  // 工单未推进，但同一工单签发了更新版本的放行（如证书信息修订后重新签发）
  const r2 = await app.airworthiness.issue('wo_demo_001', '2026-10-06T11:00:00Z');
  const oldEvent = releasedEventFor(app, r1.id, 'evt-old');
  const result = await app.airworthiness.handleReleasedEvent(oldEvent);
  assert.equal(result, 'IGNORED');
  assert.notEqual(app.store.workOrders.get('wo_demo_001')!.status, 'CLOSED');
  assert.ok(app.store.audit.some((a) => a.metadata.reason === 'SUPERSEDED'));
  assert.equal(r2.version, r1.version + 1);
});

// 11. 非 COMPLETED（如 CANCELLED）工单的事件不得关闭
await checkAsync('11. event for CANCELLED order is ignored', async () => {
  const app = freshApp('CANCELLED');
  const release = await app.airworthiness.issue('wo_demo_001', '2026-10-06T08:00:00Z');
  const result = await app.airworthiness.handleReleasedEvent(releasedEventFor(app, release.id, 'evt-cancel'));
  assert.equal(result, 'IGNORED');
  assert.equal(app.store.workOrders.get('wo_demo_001')!.status, 'CANCELLED');
  assert.ok(app.store.audit.some((a) => a.metadata.reason === 'INVALID_STATE'));
});

console.log(failures === 0 ? '\nALL SCENARIOS PASSED' : `\n${failures} SCENARIO(S) FAILED`);
process.exit(failures === 0 ? 0 : 1);
