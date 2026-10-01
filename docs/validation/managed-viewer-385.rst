Managed viewer exact-process ownership follow-through
=====================================================

Source owner: Schrodinger/Codex, existing ZMQRuntime PR13 / OpenHCS PR358.
Admission source: a6fa67a63a8973a1406ad1d7dd041d3e56b4dfa3.
Parent integration/reproducer owns OpenHCS issue385 and viewer/agent migration.
Original5992 incarnation/exit remains UNPROVED; separate5994 success and
different context do not establish contention or retrospectively prove exit.

Shared mechanism and scoped migration
-------------------------------------

IMPL-12/13: VisualizerProcessManager stores raw Popen and copies terminate,
wait/kill, then erases the handle. Original EndpointProcess/_ObservedEndpointProcess
already capture identity before reaping and retain exact ProcessExit. Reuse that
owner: the existing process field holds EndpointProcess or None; start returns
that owner, stop delegates once and retains the owner on completed/failed waits.
Initialization is cooperative; leaf hooks remain launch and readiness.
No new store, PID registry, bootstrap, observer table or timeout increase.

Shared source ownership: src/zmqruntime/streaming/process_manager.py and focused
tests only. Parent owns OpenHCS native-to-EndpointProcess launch adaptation,
same-handle consumption/group ownership and failed-launch DTO projection.
Coordination: https://github.com/OpenHCSDev/openhcs/issues/385#issuecomment-5937125372.
Do not ship this contract without the paired consumer migration.

New-case experiment: an independent StopAudit capability before/after the
declared viewer delegates through actual super/MRO, observes one shared stop
and preserves identity/exit. Another downstream initialization capability must
be reached. Generic consumers do not change for those declarations/hooks.
Tests cover ordinary observation timeout without cleanup, failed acquisition
cleanup, terminal child, unfinished stop and no fabricated pre-launch identity.
No actual child, native/UI/MCP/science or installed operation is authorized here.

Source bounds: exact frozen installed Python3.12 interpreter read-only; serial
CPU0/thread pools1, kernel512MiB/no swap/60s. Headroom helper warning2:
RAM18.5GiB/home15.6GiB/swap13.3GiB. Only tiny owned disposable source fixtures
under /home/ts/.cache/agent-scratch/zmqruntime-viewer-owner-385-20261001 are used.
Retain red/green logs, exact source checkpoint and cleanup before publishing.
Original R1 failure remains unchanged; no full/global or installed qualification.

Published source and bounded evidence
------------------------------------

Production checkpoint: a9d4ddd6f2eeb5fc4bf9c2c666857a2bdc2974b1, normally pushed
to the existing PR13 branch. The original red new-contract experiment at
a6fa67a had 11 failures and 2 passes (0.55s / 45644KiB); this demonstrates
typed-owner and cooperative-hook bypass, not the historical installed failure.
The repaired selection has 56 passes (0.58s / 46064KiB), including 13 new cases,
the existing manager/state cases and original startup/activity regression tests.
Original pinned R0 at base85c8284f to a9d4ddd passes (3.52s / 48148KiB), with
184 projections and zero positive debt deltas. R0 is not R1 or native proof.

IMPL-12/13 duplicated process cleanup and erased lifecycle authority are removed
in place; cooperative initialization/stop ownership is exercised, not asserted
by inheritance alone. Two empty new leaf declarations place the independent
audit capability on either side of the viewer in C3, without consumer changes.
No registry, string/type branch, compatibility facade or mirrored handle added.
This manually authored change does not claim an NRA native codemod proof.

Exact red/green/R0/resource logs are retained under issue385-evidence with
SHA256SUMS. Parent still owns paired OpenHCS migration and installed acceptance;
OpenHCS PR358's gitlink is intentionally not advanced to an unmigrated contract.
No larger timeout, environment mutation or native process was used.

Cleanup completed: removed ONLY the verified owned disposable directory
/home/ts/.cache/agent-scratch/zmqruntime-viewer-owner-385-20261001 (316KiB).
Its canonical path matched, lsof and scoped process checks found no users;
all four retained log checksums passed before removal and the directory is
absent afterwards. Source, failed evidence and parent artifacts are unchanged.
