ZMQRuntime publisher failure: in-process endpoint shutdown
=========================================================

Owner: Schrodinger/Codex. Base: reviewed merged main6800561579ae38a359bbae81e46648548846ee10.
Original publisher36952353836/job110667830937, attempt1, failed its test step
with exit143 after80 progress dots. Build, PyPI and GitHub release were skipped.
Original annotated v0.3.0 remains immutable at6800561; do not rerun or move it.
The sole pyproject version authority advances to0.3.1 for a NEW release tag.

Concrete diagnosis and original failure
---------------------------------------

Collection of the original source places test_shutdown_result_distinguishes_worker_stop_from_endpoint_termination
at test81. The retained intercept_original_case.py invokes that unchanged case
through the existing source environment and intercepts only ProcessIdentity.terminate,
before any OS signal. Original-intercept.log shows SAME_INCARNATION True:
target and caller were both pid2900417/create_time1790908801.41.
The FORCE RPC was acknowledged and endpoint_terminated=True, but its host process
remained alive. The original operation attempted to terminate that very host and,
when safely intercepted, returned succeeded=False/process_exited=False.
The original assertion failed. This witnesses a concrete self-termination defect
and matches the hosted exit143/test order; the hosted log itself has no stack.

Original hosted job logs, collection and resource receipts remain at
/home/ts/wt/openhcs-knowledge-declaration-source-376-20261001/docs/validation/required-runtime-qt-publication-20261002/.
No original workflow, immutable tag, failed evidence or test assertion is changed.

Declaration owners and removed mistake
---------------------------------------

ProcessIdentity owns exact local incarnation identity and signal admission.
Its is_current query uses the original current-incarnation constructor, and
terminate refuses to signal that caller. It returns False, never host-exit proof.
The original PID-reuse checks, TERM/KILL escalation and single deadline remain.

_EndpointShutdownOperation remains the sole dispatch, ACK uncertainty, wait,
transport cleanup and result implementation. Replace its unconditional child-exit
success expression with one behavior hook: _process_exit_satisfied.
Endpoint-only shutdown can close an in-process endpoint while explicitly recording
process_exited=False. An occupied endpoint still fails. Unknown or other live
incarnations are not treated as successful exit.

_OwnedProcessShutdownOperation inherits every mechanism and supplies just that
hook, requiring exited is True. EndpointShutdownMode.close_owned_process selects
this declaration after the SAME local/pair-lock/reservation admission. Caller
identity never downgrades an owned-child close into endpoint-only success.
No mode/type/string switch, flag taxonomy, registry, duplicate loop or timeout.
No compilation, kernel, catalog, startup feedback or readiness-placement edits.

Current catalog review: IDEN-1/7 separate endpoint cessation from host exit;
IDEN-8 preserves exact incarnation comparisons; IMPL-12/13 preserve the original
shared signal/shutdown mechanisms; IMPL-2 retains member-owned completion rather
than adding consumer switches; TIME-5 has no production test switch or suppression.
There is no persisted schema/store change and no compatibility facade.

Behavioral evidence and extension experiment
---------------------------------------------

All source commands retain Python3.12.3 read-only, CPU0, one CPU quota, thread pools1,
512MiB/no swap and60s outer bound. No environment creation/install, provider,
OpenHCS native/MCP/UI, scientific replay or shared validation lock.
The original source-loopback server-thread fixture uses its existing paired TCP
authority, not any production endpoint. The new guards intercept OS signals.

Before production repair: guards-red.log has3 failures/2 passes; original-intercept
has1 failure. After repair: owner-green.log has43 passes in0.65s,
46372KiB/process0.88s/exit0. This includes the original actual-socket case now
unintercepted, retained child-close/PID-reuse/successor/unknown/budget controls,
typed messages, self-signal prevention and occupied-endpoint refusal.

New independent ExitAudit capability composes with the owned-close declaration
through genuine MI. Its matching _process_exit_satisfied hook calls cooperative
super and records the actual terminal disposition. The ORIGINAL FORCE.complete
consumer observes that C3 hook and the inherited algorithm on alive/dead cases.
No generic consumer edits or hook table are needed to add this capability.
The inherited strict leaf refuses alive-host success and admits exact exit.

Release and remaining gate
---------------------------

Publish the source repair for review before normal main integration. Then use
the original scripts/release.py and trusted tag publisher for NEW v0.3.1 only.
Observe the actual test/build/publication handle and original normal-index helper
before admitting parent PolyStore's publisher. Source controls alone are not a
public artifact, installer or affected installed user-journey qualification.
Original R1/global audit failure is preserved; no full R1 retry is admitted.
