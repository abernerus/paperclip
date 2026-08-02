// CAS-10256 regression: a stale executionRunId lock (owner died without
// releasing or renewing it) must never allow two concurrent executor runs on
// one issue/worktree. Reproduces the CAS-10193 pattern: lock owner dies →
// two later runs dispatched within minutes of each other. Exactly one may
// proceed (via explicit, recorded takeover); the other is parked and
// re-delivered when the survivor finalizes.
import { randomUUID } from "node:crypto";
import type { ChildProcess } from "node:child_process";
import { and, eq, ne, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  activityLog,
  agents,
  agentWakeupRequests,
  companies,
  createDb,
  heartbeatRuns,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import {
  EXECUTION_LOCK_STALE_MS,
  heartbeatService,
} from "../services/heartbeat.ts";
import { runningProcesses } from "../adapters/index.ts";

const mockAdapterExecute = vi.hoisted(() =>
  vi.fn(async () => ({
    exitCode: 0,
    signal: null,
    timedOut: false,
    errorMessage: null,
    summary: "Execution-lock double-dispatch test run.",
    provider: "test",
    model: "test-model",
  })),
);

vi.mock("../adapters/index.ts", async () => {
  const actual = await vi.importActual<typeof import("../adapters/index.ts")>("../adapters/index.ts");
  return {
    ...actual,
    getServerAdapter: vi.fn(() => ({
      supportsLocalAgentJwt: false,
      execute: mockAdapterExecute,
    })),
  };
});

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres execution-lock double-dispatch tests on this host: ${
      embeddedPostgresSupport.reason ?? "unsupported environment"
    }`,
  );
}

async function waitForCondition(fn: () => Promise<boolean>, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return fn();
}

function fakeRunningProcess() {
  return { child: {} as unknown as ChildProcess, graceSec: 0, processGroupId: null };
}

describeEmbeddedPostgres("issue execution-lock double-dispatch prevention (CAS-10256)", () => {
  let db!: ReturnType<typeof createDb>;
  let heartbeat!: ReturnType<typeof heartbeatService>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-exec-lock-double-dispatch-");
    db = createDb(tempDb.connectionString);
    heartbeat = heartbeatService(db);
  }, 20_000);

  afterEach(async () => {
    mockAdapterExecute.mockReset();
    mockAdapterExecute.mockImplementation(async () => ({
      exitCode: 0,
      signal: null,
      timedOut: false,
      errorMessage: null,
      summary: "Execution-lock double-dispatch test run.",
      provider: "test",
      model: "test-model",
    }));
    runningProcesses.clear();
    let idlePolls = 0;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const runs = await db
        .select({ status: heartbeatRuns.status })
        .from(heartbeatRuns);
      const hasActiveRun = runs.some((run) => run.status === "queued" || run.status === "running");
      if (!hasActiveRun) {
        idlePolls += 1;
        if (idlePolls >= 3) break;
      } else {
        idlePolls = 0;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
    await db.execute(sql.raw(`
      TRUNCATE TABLE
        "issue_comments",
        "issue_documents",
        "document_revisions",
        "documents",
        "issues",
        "heartbeat_run_events",
        "activity_log",
        "heartbeat_runs",
        "agent_wakeup_requests",
        "agent_runtime_state",
        "agents",
        "companies"
      RESTART IDENTITY CASCADE
    `));
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedCompanyAndAgent(opts: { maxConcurrentRuns?: number } = {}) {
    const companyId = randomUUID();
    const agentId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "LockCoder",
      role: "engineer",
      status: "active",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {
        heartbeat: {
          wakeOnDemand: true,
          maxConcurrentRuns: opts.maxConcurrentRuns ?? 1,
        },
      },
      permissions: {},
    });
    return { companyId, agentId };
  }

  async function seedQueuedRun(input: {
    companyId: string;
    agentId: string;
    issueId: string;
    wakeReason?: string;
  }) {
    const wakeupRequestId = randomUUID();
    const runId = randomUUID();
    await db.insert(agentWakeupRequests).values({
      id: wakeupRequestId,
      companyId: input.companyId,
      agentId: input.agentId,
      source: "assignment",
      triggerDetail: "system",
      reason: input.wakeReason ?? "issue_assigned",
      payload: { issueId: input.issueId },
      status: "queued",
    });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId: input.companyId,
      agentId: input.agentId,
      invocationSource: "assignment",
      triggerDetail: "system",
      status: "queued",
      wakeupRequestId,
      contextSnapshot: {
        issueId: input.issueId,
        wakeReason: input.wakeReason ?? "issue_assigned",
      },
    });
    await db
      .update(agentWakeupRequests)
      .set({ runId })
      .where(eq(agentWakeupRequests.id, wakeupRequestId));
    return { runId, wakeupRequestId };
  }

  it("renews execution locks for runs the server is actively supervising", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const liveRunId = randomUUID();
    const zombieRunId = randomUUID();
    const staleLockedAt = new Date(Date.now() - EXECUTION_LOCK_STALE_MS * 2);
    await db.insert(heartbeatRuns).values([
      {
        id: liveRunId,
        companyId,
        agentId,
        status: "running",
        invocationSource: "assignment",
        startedAt: staleLockedAt,
      },
      {
        id: zombieRunId,
        companyId,
        agentId,
        status: "running",
        invocationSource: "assignment",
        startedAt: staleLockedAt,
      },
    ]);
    const liveIssueId = randomUUID();
    const zombieIssueId = randomUUID();
    await db.insert(issues).values([
      {
        id: liveIssueId,
        companyId,
        title: "Locked by live run",
        status: "in_progress",
        priority: "high",
        assigneeAgentId: agentId,
        executionRunId: liveRunId,
        executionLockedAt: staleLockedAt,
      },
      {
        id: zombieIssueId,
        companyId,
        title: "Locked by zombie run",
        status: "in_progress",
        priority: "high",
        assigneeAgentId: agentId,
        executionRunId: zombieRunId,
        executionLockedAt: staleLockedAt,
      },
    ]);

    runningProcesses.set(liveRunId, fakeRunningProcess());
    const result = await heartbeat.renewExecutionLocksForLiveRuns();
    expect(result.renewed).toBe(1);

    const [liveIssue, zombieIssue] = await Promise.all([
      db.select({ executionLockedAt: issues.executionLockedAt }).from(issues)
        .where(eq(issues.id, liveIssueId)).then((rows) => rows[0]),
      db.select({ executionLockedAt: issues.executionLockedAt }).from(issues)
        .where(eq(issues.id, zombieIssueId)).then((rows) => rows[0]),
    ]);
    // The supervised run's lock is renewed (fresh); the dead run's lock stays stale.
    expect(liveIssue.executionLockedAt!.getTime()).toBeGreaterThan(staleLockedAt.getTime());
    expect(zombieIssue.executionLockedAt!.getTime()).toBe(staleLockedAt.getTime());
  });

  it("CAS-10193 pattern: dead lock owner → takeover is explicit and a sibling dispatch is parked, never concurrent", async () => {
    // Slots: the zombie still counts as "running" and occupies one, so allow
    // three — both later dispatches must reach the claim gate in one pass.
    const { companyId, agentId } = await seedCompanyAndAgent({ maxConcurrentRuns: 3 });

    // Run Z: the dead owner — status "running", not tracked in memory, lock
    // never renewed (executionLockedAt far past the staleness horizon).
    const zombieRunId = randomUUID();
    const staleLockedAt = new Date(Date.now() - EXECUTION_LOCK_STALE_MS * 2);
    await db.insert(heartbeatRuns).values({
      id: zombieRunId,
      companyId,
      agentId,
      status: "running",
      invocationSource: "assignment",
      startedAt: staleLockedAt,
    });

    const issueId = randomUUID();
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "CAS-10193 shape — stale lock, double dispatch",
      status: "in_progress",
      priority: "high",
      assigneeAgentId: agentId,
      executionRunId: zombieRunId,
      executionLockedAt: staleLockedAt,
    });

    // Two later runs dispatched minutes apart (here: back to back).
    const first = await seedQueuedRun({ companyId, agentId, issueId });
    const second = await seedQueuedRun({ companyId, agentId, issueId });

    await heartbeat.resumeQueuedRuns();

    // The first run takes over the stale lock; the takeover is recorded.
    const takeover = await waitForCondition(async () => {
      const rows = await db
        .select({ details: activityLog.details })
        .from(activityLog)
        .where(eq(activityLog.action, "issue.execution_lock_takeover"));
      return rows.length === 1;
    });
    expect(takeover).toBe(true);
    const takeoverRow = await db
      .select({ details: activityLog.details })
      .from(activityLog)
      .where(eq(activityLog.action, "issue.execution_lock_takeover"))
      .then((rows) => rows[0]);
    const details = takeoverRow.details as {
      previousExecutionRunId?: string;
      newExecutionRunId?: string;
      takenOverAt?: string;
    };
    expect(details.previousExecutionRunId).toBe(zombieRunId);
    expect(details.newExecutionRunId).toBe(first.runId);
    expect(details.takenOverAt).toBeTruthy();

    // The second run must not have executed concurrently: it is cancelled with
    // the held-lock code and its wake is parked as a deferred issue execution.
    const secondRun = await db
      .select({ status: heartbeatRuns.status, errorCode: heartbeatRuns.errorCode })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, second.runId))
      .then((rows) => rows[0]);
    expect(secondRun.status).toBe("cancelled");
    expect(secondRun.errorCode).toBe("issue_execution_lock_held");

    const secondWakeup = await db
      .select({ status: agentWakeupRequests.status })
      .from(agentWakeupRequests)
      .where(eq(agentWakeupRequests.id, second.wakeupRequestId))
      .then((rows) => rows[0]);
    expect(secondWakeup.status).toBe("deferred_issue_execution");

    // The zombie's lock is gone — the issue is owned by the survivor (until it
    // finalizes and releases the lock normally).
    const lock = await db
      .select({ executionRunId: issues.executionRunId })
      .from(issues)
      .where(eq(issues.id, issueId))
      .then((rows) => rows[0]);
    expect(lock.executionRunId).not.toBe(zombieRunId);

    // When the survivor finalizes, the parked wake is promoted: a fresh run for
    // the issue appears instead of the trigger being lost.
    const redelivered = await waitForCondition(async () => {
      const runs = await db
        .select({ id: heartbeatRuns.id })
        .from(heartbeatRuns)
        .where(
          and(
            eq(heartbeatRuns.companyId, companyId),
            ne(heartbeatRuns.id, zombieRunId),
            ne(heartbeatRuns.id, first.runId),
            ne(heartbeatRuns.id, second.runId),
            sql`${heartbeatRuns.contextSnapshot} ->> 'issueId' = ${issueId}`,
          ),
        );
      return runs.length >= 1;
    }, 10_000);
    expect(redelivered).toBe(true);
  });

  it("refuses to dispatch a second executor while a live run holds a fresh lock", async () => {
    // The live holder occupies one slot; allow two so the second dispatch
    // reaches the claim gate instead of being screened by the concurrency cap.
    const { companyId, agentId } = await seedCompanyAndAgent({ maxConcurrentRuns: 2 });

    // Run L: a live holder on another instance — running, lock freshly renewed.
    const liveRunId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: liveRunId,
      companyId,
      agentId,
      status: "running",
      invocationSource: "assignment",
      startedAt: new Date(),
    });

    const issueId = randomUUID();
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Live fresh lock — second dispatch must park",
      status: "in_progress",
      priority: "high",
      assigneeAgentId: agentId,
      executionRunId: liveRunId,
      executionLockedAt: new Date(),
    });

    const { runId, wakeupRequestId } = await seedQueuedRun({ companyId, agentId, issueId });

    await heartbeat.resumeQueuedRuns();

    await waitForCondition(async () => {
      const run = await db
        .select({ status: heartbeatRuns.status })
        .from(heartbeatRuns)
        .where(eq(heartbeatRuns.id, runId))
        .then((rows) => rows[0] ?? null);
      return run?.status === "cancelled";
    });

    const run = await db
      .select({ status: heartbeatRuns.status, errorCode: heartbeatRuns.errorCode })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId))
      .then((rows) => rows[0]);
    expect(run.status).toBe("cancelled");
    expect(run.errorCode).toBe("issue_execution_lock_held");
    const executeCallsForRun = (
      mockAdapterExecute.mock.calls as unknown as Array<[{ runId?: string } | undefined]>
    ).filter(([context]) => context?.runId === runId);
    expect(executeCallsForRun).toHaveLength(0);

    const wakeup = await db
      .select({ status: agentWakeupRequests.status, payload: agentWakeupRequests.payload })
      .from(agentWakeupRequests)
      .where(eq(agentWakeupRequests.id, wakeupRequestId))
      .then((rows) => rows[0]);
    expect(wakeup.status).toBe("deferred_issue_execution");
    expect((wakeup.payload as { issueId?: string }).issueId).toBe(issueId);

    // The live holder's lock is untouched.
    const lock = await db
      .select({ executionRunId: issues.executionRunId })
      .from(issues)
      .where(eq(issues.id, issueId))
      .then((rows) => rows[0]);
    expect(lock.executionRunId).toBe(liveRunId);
  });
});
