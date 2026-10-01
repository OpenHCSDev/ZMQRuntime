OpenHCS355: source lifecycle checkpoint
=======================================

Named integration/source-fix owner: Schrodinger/Codex.
Isolated branch fix/cold-start-activity-355-20261001, base85c8284f8e729e3804ece6fe5236769c294d2962.
Reviewed production source23097e26c919b6c1b9621d009ee28f01626f1a65.
Paired OpenHCS draft https://github.com/OpenHCSDev/openhcs/pull/358.
Source only: no installed/native/UI acceptance or release claim.

Boundary and witness
---------------------

Original installed GUI failed after Discovering registered callables while
explicit exact-owned MCP spawn/observe reached ready in109s. Preserve those
original observations in UI349-COLD-CONNECT-DEFECT-20261001.rst in the parent's
issue-batch worktree; this is not a replay of that GUI process or science.
The initiating configuration supplies15s, interpreted by TransportEndpoint as
inactivity. Journal events alone did not observe CPU work inside a long cold
discovery call. ReturningNone made _connect_locked stop its exact child.

Ownership and closure
----------------------

IMPL-12/IMPL-13: ZMQClient's one _wait_for_endpoint_ready algorithm now owns
domain observer composition, exact-child activity, cancellation, independent
total deadline and the first typed ready handshake. OpenHCS removes its copied
algorithm and supplies only journal/ready-cleanup hooks. Delete the old boolean
readiness compatibility hook and extra PONG probe. Delete the separate deadline
adapter and caller branch; no compatibility reader or forwarding facade remains.

EndpointProcess ABC owns startup_observer for all native process leaves. Their
existing identity-capture/native-wait hooks remain small. ProcessIdentity owns
OS work projection, checking creation time before and after CPU sampling of
the exact root and its currently observed descendants. EndpointStartupProcessObserver
compares two bounded temporal samples and composes the existing nominal domain
observer. New samples are activity only, never readiness. Missing/inaccessible
samples do not refresh inactivity; unchanged counters do not refresh it on
access return. Reused PIDs cannot contribute work to another incarnation.

EndpointConnectionAttempt owns the asynchronous await/cancel/join mechanism.
The GUI service calls that shared owner, so coroutine cancellation cannot
abandon the active worker, forget its exact cancellation authority or permit a
replacement connect before that worker unwinds. Explicit total deadlines,
failed journals, exact process exit and cancellation remain authoritative.

No consumer concrete-type/string switch, bootstrap copy, process registry,
identity mirror, persistent state format, default timeout increase or fake
heartbeat is added. The work map is a transient OS measurement, not a second
process ownership store. Existing OpenHCS ExecutionClient plus compatibility
ABC MI remains intact. No invented MI is used to wrap dynamic observer objects.
New-case proof: the test's declared EndpointProcess leaf provides identity/native
hooks and inherits the whole startup mechanism, requiring zero consumer edits.
The OpenHCS guard asserts the exact ancestor readiness callable is inherited.

Scope and limits
-----------------

PR6/H003 owns execution/wait_policy.py malformed-status admission and its tests;
neither file is changed here. No cause/closure claim for original issue140.
No provider, Fiji, download, backing install/source change, native/UI launch,
validation.lock acquisition, scientific retune or PR351 source modification.
Parent owns serial installed acceptance. OpenHCS root updated by normal main
merge to7640fd951978c0901f7911d46414e06e8b886723; no parent-owned file was edited.

CPU work and actual journal updates are observed, not inferred from OS liveness.
A CPU-busy infinite loop still needs explicit cancellation or a total deadline.
An I/O-only silent operation supplies no CPU activity and remains subject to
the unchanged inactivity budget. Actual installed cold discovery is not yet
qualified by this synthetic evidence; parent must validate the real GUI path.

Current source evidence
------------------------

35 provider-free dependency startup tests pass. The family covers original
journal-only predecessor expiry at15s despite working CPU, working root and
working descendant reaching simulated109s readiness, actual inactivity at15s,
activity stopping then expiry, cancellation, exact exit, failed journal and
total deadline. No clock waits or native children are started.
Paired OpenHCS has23 passing source tests including same inherited readiness
owner, domain journal retention/cleanup, async cancellation retirement and
concurrent service reuse. Kernel512MiB/no swap, outer60s, tasksetCPU0, numerical
thread pools1, exact frozen installed3.12 interpreter for source checks.
Original pinned R0 uses its own Python3.14 owner with read-only dependencies.

First dependency R0 at806faa1 rejected GodClassExcess growth126->129. Preserve
that JSON/resource receipt. The final single hook removes the adapter/branch
mechanism, not comments: original R0 now reports126->101, zero positive deltas.
No detector is copied or allowance added. Other checks and resource footers are
in the retained logs, not inferred from passing pytest.

NRA local full-payload semantic output is retained as an ownership observation,
not R1/global qualification: it has no scan_status/omission witness or complete
dependency context. Do not interpret its absence of R1 records as zero R1 debt.
Full-context qualification is still limited by independently owned issue357
(Dewey); this source fix does not alter the tool or repeat that live attempt.

Failures and artifact dispositions
-----------------------------------

Initial new fixture had missing PongResponse server/server_role:8fail/2pass,
0.55s/42952KiB. Next fixture used the wrong OperationDeadline constructor:
8fail/2pass,0.50s/43332KiB. Both were corrected to original typed contracts;
neither was a successful readiness test. Initial clean OpenHCS shard had no
compiled tabular extension:12fail/2pass,3.59s/94212KiB. Native members were later
extracted from the parent's retained private wheel into owned scratch and linked
only in the new source worktree, without installation or backing mutation.
These original tool outputs were truncated, not full persisted test transcripts;
the exact errors/counts/dispositions are retained here. Original installed GUI
failure and successful owned-runtime evidence stay untouched with the parent.

Initial NRA scan accidentally used the default advisor cache home. Move only
its new task-keyed root zmqruntime-cold-start-355-20261001-deede3491fc30564 into
owned .validation/cache/initial-default-root. The shared retention stamp updated
at07:01:06 local; its other maintenance effects were not captured and are not
claimed absent. Do not clean unrelated shared caches. All later caches use an
explicit owned root. Initial existing startup tests accessed default shared
5555/6555 lock paths; both file timestamps predate this task and neither file
was deleted. Further tests redirect paths into owned fixtures.
