import { describe, expect, it } from "vitest";
import { shouldAutoCheckoutIssueForWake } from "../services/heartbeat.ts";

// CAS-9603: A child-completed wake must not auto-promote a blocked parent to
// in_progress. The external blocker is still live; only the assignee can clear it.
// Reproduces: CAS-9358 churning blocked → in_progress → blocked on every
// close-out heartbeat after CAS-9602 (its child mitigation) completed.

const BASE_INPUT = {
  issueAssigneeAgentId: "agent-seneschal",
  agentId: "agent-seneschal",
  isDependencyReady: true,
};

describe("shouldAutoCheckoutIssueForWake — blocked parent with issue_children_completed", () => {
  it("does NOT auto-checkout a blocked parent for an issue_children_completed wake", () => {
    expect(
      shouldAutoCheckoutIssueForWake({
        ...BASE_INPUT,
        issueStatus: "blocked",
        contextSnapshot: { wakeReason: "issue_children_completed" },
      }),
    ).toBe(false);
  });

  it("still auto-checkouts a todo parent for an issue_children_completed wake", () => {
    expect(
      shouldAutoCheckoutIssueForWake({
        ...BASE_INPUT,
        issueStatus: "todo",
        contextSnapshot: { wakeReason: "issue_children_completed" },
      }),
    ).toBe(true);
  });

  it("still auto-checkouts a blocked issue for non-child-completed wakes (e.g. issue_assigned)", () => {
    expect(
      shouldAutoCheckoutIssueForWake({
        ...BASE_INPUT,
        issueStatus: "blocked",
        contextSnapshot: { wakeReason: "issue_assigned" },
      }),
    ).toBe(true);
  });

  it("still auto-checkouts a blocked issue when blockers resolve", () => {
    expect(
      shouldAutoCheckoutIssueForWake({
        ...BASE_INPUT,
        issueStatus: "blocked",
        contextSnapshot: { wakeReason: "issue_blockers_resolved" },
      }),
    ).toBe(true);
  });

  it("does NOT auto-checkout when dependency is not ready, regardless of wake reason", () => {
    expect(
      shouldAutoCheckoutIssueForWake({
        ...BASE_INPUT,
        issueStatus: "blocked",
        isDependencyReady: false,
        contextSnapshot: { wakeReason: "issue_blockers_resolved" },
      }),
    ).toBe(false);
  });
});
