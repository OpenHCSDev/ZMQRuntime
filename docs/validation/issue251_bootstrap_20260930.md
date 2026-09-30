# Canonical exclusive startup owner (OpenHCS issue251)

Extends the existing ZMQClient startup owner with explicit local empty-pair
startup, exact native ProcessIdentity capture, and both-address pre-bind reservation in the
existing transport startup lock. No new launcher, registry, future, catalogue
warmup, takeover/kill/retry, or timeout increase. Post-spawn reservation failure
preserves the exact native handle as uncertainty. Existing fake nominal process
implementations migrated with their identity contract; no NotImplemented stub.

Paired [OpenHCS256](https://github.com/OpenHCSDev/openhcs/pull/256) references issue251;
[metaclass-registry1](https://github.com/OpenHCSDev/metaclass-registry/pull/1) owns
non-creating cache projection. Combined source shard66 passed including9 native-owner
focused cases, existing startup cases and offline CLI projection; no native
runtime launched. Full native/installed acceptance and cross-process endpoint-pair
race evidence remain explicit pending released serial slot.
Parent integrates paired revisions; worker does not merge or install.
# Identity-proven lifecycle continuation

Parent-reported reproducer: FORCE shutdown at5993, exact PID2052959 /
creation time1790731598.48, returned succeeded/endpoint_terminated while
listeners5993/6993 disappeared but the same process remained live at981192KiB
RSS. The parent owns that original attempt and its final disposition in
H002B-CLEANUP-20260930.json; this worker did not contact, replay or signal it.
Interpreter/JVM teardown is a hypothesis, not a measured cause.

The existing EndpointShutdownMode completion leaves now distinguish transport
cessation from exact process exit. EndpointShutdownResult records the observed
incarnation, process_exited (unknown stays unknown), one request_attempted and
acknowledgement. Local identified FORCE cannot succeed on lost listeners alone.
The existing ProcessIdentity owner performs bounded TERM/KILL/wait under one
OperationDeadline, including PID-reuse checks; no new supervisor/process store.
GRACEFUL still clears workers and intentionally retains the server.

ZMQClient.close_owned_process requires both existing startup reservations to
match the supplied incarnation under the same native pair locks. It never
attaches, starts, kills port owners, or retries an RPC. An absent listener with
a proven child uses exact process cleanup without a shutdown replay. A changed
reservation or heartbeat rejects before control dispatch. The shutdown request
is a refinement of the existing ControlRequestHeader and carries the observed
ProcessIdentity; ExecutionServer validates it before cancellation or worker
mutation, closing the ping-to-send successor race. No action roster, compatibility
alias, second registry or application-level process signalling was added.
IPC cleanup uses the existing stale-address declaration after process exit;
unproved/live foreign socket sentinels remain intact.

Paired source shard:123 passed,2 actual-MCP cases deselected,12.16s; process12.86s,
peak285060KiB,exit0. Tests intercept socket sends/process signals/spawn; no native
endpoint/JVM/GUI starts. Covers listener-gone/process-live failure, exact exit,
GRACEFUL retention, absent listener/no RPC replay, both reservation identities,
changed heartbeat before/after dispatch, unknown liveness, missing ack/one-send,
expired deadline/no mutation, postdispatch expiry/no late signal, PID reuse,
bounded TERM/KILL waits, native pre-mutation admission and IPC stale/live cases.
Root tests also cover nested actual DTO decoding, default context/service
invocation, declaration exposure, path denial before native locks, and unchanged
QA/context bounds. Captures are preserved in paired OpenHCS256 under
docs/validation/runtime_bootstrap_20260930/source-close-final{,-resources}.txt.
Earlier fixture failures and coherent114/122-pass checkpoints are retained.

Focused review: IDEN-1 separates listener and process facts; IDEN-8 keeps exact
incarnation at observation and signalling; IMPL-2/12/13 retain mode completion,
process termination and stale socket cleanup at their existing native owners;
BOUND-1/2 decode the incarnation once into its original declaration before
effects; TIME-7 admits native lifecycle paths before acquiring locks. No full
NRA/global equivalence claim. Native cross-process and installed acceptance
remain gated on the parent's released serial slot and paired integration.
