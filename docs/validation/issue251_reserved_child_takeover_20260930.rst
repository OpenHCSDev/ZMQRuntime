Issue251: preserve reserved children before ordinary TCP takeover
================================================================

Distinct source-only continuation of PR9 after explicit account-switch resume.
Production commit: ``0ca331a1c197e26656965193d6502e7a3c298808``.
Normally merged native main ``2aa6d21c`` is already an ancestor. Paired OpenHCS
normally merged main ``c42d9bf5d`` at ``5b330a7b3e`` before these checks.
Installed packages/skill, science runtime and original attempts were untouched.

Concrete failing boundary
-------------------------

``ZMQClient._connect_locked`` held the original pair locks, attempted attachment
to an occupied data endpoint, then consulted transport preservation and killed
both port owners before checking the existing startup reservations. TCP's
preservation policy returns false. A child with an original live reservation
could therefore be replaced after bind but before responsive startup.

The preceding ordinary-connect controls intercepted occupancy as false and
did not witness this path. Extended controls preserve real declaration-owned
records and portalocker locks; only occupancy, attachment, process liveness and
kill/spawn boundaries are controlled. Before the production change, six controls
failed at the intercepted kill: live/unknown liveness on each reserved address
and each address's malformed partial record. No process was killed or spawned.
Healthy attachment and the unoccupied cases passed.

Correction and ownership review
-------------------------------

After a failed attachment, ordinary connect now consumes the EXISTING
``TransportEndpoint.has_live_startup_owner(config)`` before any takeover.
The original guard remains before spawn for the unoccupied route. Healthy
attachment remains earlier and allowed. Both held locks, exact ProcessIdentity,
record/inode retention, malformed-record failure and post-spawn uncertainty are
unchanged. No timeout, replay, transport switch or lifecycle store was added.

Applicable current archive catalog witnesses:

* IMPL-13: exclusive startup and ordinary connect consume the SAME pair owner;
  the occupied/unresponsive case no longer weakens its existing admission.
* IDEN-8: original PID plus creation-time records govern preservation, not
  occupancy or a bare PID. Unknown liveness is not inferred dead.
* BOUND-2: original transport record decoding is reused, including malformed
  failure; no second decoder/schema or metadata mirror.
* IMPL-12 / AGENT-6: no copied pair procedure, helper/mixin or forwarding split.
  The original predicate is composed with original transport preservation;
  the concrete new-case witness is bind-before-ready on either paired address.

Only ``client.py`` and ``tests/test_owned_startup.py`` changed in this correction.
No ACK-private, config, message, transport, viewer or waiter file changed.
Parent owns remaining ACK delivery/integration. Broader archive author remains
unidentified; BLOCKED S1 is not resumed or certified.

Actual bounded evidence
-----------------------

Paired source shard with existing Python3.12 ``-B``, explicit own OpenHCS root
plus all eight child src paths, thread1, plugin autoload disabled and shared Fiji
cache/downloadfalse: **197 passed, no skips/deselections, 5.39s**; process
6.20s/264612KiB RSS/exit0, under the original30s shell bound. Controlled endpoint
and process boundaries; no native socket/server/MCP/JVM/GUI/science or provider.
The six merged viewer-state controls use controlled visualizers and one short
lock-probe thread, not an actual viewer. Two warnings are disabled asyncio-plugin
configuration, not discarded cases.

The original pre-fix receipt retained six failures/six passes. Its first path
assertion caught mis-cased PolyStore/ObjectState paths (not accepted as complete
path authority). A corrected all-nine-path assertion then succeeded before
the unchanged pre-fix controls reproduced six failures/six passes in0.99s,
process1.23s/53548KiB/exit1. Both originals retained; no hidden pass relabeling.
Root receipts are in ``docs/validation/runtime_bootstrap_20260930/``
``reserved-child-takeover/`` on paired OpenHCS PR256.

Authenticated original packaged agent-comms ratchet, original CLI/SourceRevision
owners, whole ``src/zmqruntime`` root, main2aa6d21c -> production0ca331a:
**PASS188 metrics, no positive deltas, client excess delta-1**; process
4.79s/47992KiB/exit0. JSON/resources accompany this receipt. The earlier +142
failure remains retained; this is introduced-growth evidence, not zero existing
debt, global NRA/R1/all-detector, performance or live proof. No detector copy.

Remaining boundary
------------------

Parent owns merge/install and affected installed/native journey. No new native
attempt was authorized or dispatched here. Historical attempts01/02/03 remain
terminal partial/failed/interrupted, including original inputs and dispositions.
Valid-volume first/chained full/reorder/reduced/singleton execution with strict
durable image/label/CSV/source-address/ROI inventory, cross-process uncertainty
and installed bootstrap acceptance remain unfinished. No Closes251/257 claim.
Resource warning (home19.9GiB, RAM17.1GiB, swap9.5GiB) did not admit a native
allocation, large/parallel scan, package/skill change or validation-lock use.
