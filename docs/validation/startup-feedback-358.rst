Request-scoped delivery of original startup statuses
====================================================

Schrodinger/Codex, existing ZMQRuntime PR13 paired with OpenHCS PR358.
Production checkpoint:423b1417aa1fe7971ea1023627a4a4adf3b541d3.
OpenHCS production:4ab076890cb2ff9568e005abae2bc4d1e77997ed.

3 production lines deleted,27 added. EndpointStartupStatus owns callback_scope
and publish. ZMQClient retains sequence/construction and delivers its one
original object through publish instead of an explicit-callback-only branch.
Explicit UI observers remain active; request-local observers bind through the
existing Python ContextVar mechanism, reset on exit/error and propagate through
asyncio.to_thread. A shared/reused client never stores the MCP request callback.
No child journal reader, readiness polling, timeout, process store, phase switch,
wire format or native lifecycle change. Repeated identical callback objects are
delivered once. The unrelated unused dataclasses.replace import is deleted.

IMPL-12/13: extend the original status/emission mechanism, not a transport-local
startup clone. IMPL-1/3/4/5 and MEMB-1/2/5: no kind/type switches, catalogs or
authority mirrors. TIME-1/3/9: no old API facade, fallback or second wire shape.
Current canonical refactor-audit catalog and installed NRA skill were read.
No ornamental product MI; the new-case experiments use real cooperative C3.

tests/test_startup_callback_scope.py exercises an independent StatusAudit
capability composed before/after a new client declaration sharing ZMQClient.
The matching _emit_connection_status super hook runs once and both explicit UI
and request-local observers receive the SAME original objects. Nested scopes,
exception cleanup, duplicate observer avoidance and concurrent two-request
worker contexts on ONE reused client pass without consumer edits.

Authoritative paired evidence and runner are versioned in OpenHCS PR358:
docs/validation/startup-feedback-358.rst and startup-feedback-evidence/.
Initial red10 includes four dependency cases. Final feedback selection25passed
(9.86s,324296KiB); startup/lifecycle/UI-source selection50passed
(4.36s,233496KiB), four scope cases overlap. FIRST_COMPLETED repair of the
OpenHCS notification wait follows preserved failed wrapper diagnostics; no
dependency context-copy workaround was needed. No current global R0/R1 claim.

Source-only CPU0/thread pools1,512MiB/no swap,60s per shard, exact read-only
Python3.12.3 interpreter. No native/UI/MCP server, install, provider or science
operation. Current installed/live slot remains parent/Dalton-owned. Parent must
pin this dependency source and finish paired viewer integration before installed
acceptance; OpenHCS's old gitlink has no callback_scope and is not sufficient.
Original355 cold failure and357 full-context deadline evidence remain.

Only the completed paired source scratch root startup-feedback-358-20261001
(228KiB) was removed after worker/handle/checksum proof. No backing/source/user
state or another owner's disposable directory was changed.
