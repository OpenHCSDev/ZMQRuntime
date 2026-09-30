Issue10 / existing PR12: final source delivery and ownership handoff
==================================================================

Owner and freeze
----------------

Assigned ACK-return-route worker is the sole ACK implementation owner for this
delivery. Parent is integration/live-acceptance owner and has explicitly taken
current-main review and the serialized installed slot. No further worker source,
package, pin, GUI/native-runtime or scientific actions after this handoff.
The original author/source/science, uncertain attempts, foreign worktrees and
parent displays remain preserved. No separate comms or reply polling.

Published source and exact pins
-------------------------------

* SAME native draft PR12: https://github.com/OpenHCSDev/ZMQRuntime/pull/12
  ACK source head 9e11f6f2102c2083b3255520a65ccbbeb4144af3, built from original
  PR12 cd5be769dbd7ad3337857fb0f28730fd097d6ce3 by normal merges of native main
  2aa6d21c000d18bd75647f998794015ba8f54710 and published bootstrap PR9
  112c240464df6e81c25b0f97d2b1376d43bbbbc8. Final receipt commit is a descendant
  with no further source changes. Published branch: fix/ack-return-route-owner-20260930.
* PolyStore draft PR19: https://github.com/OpenHCSDev/PolyStore/pull/19
  final head dc3417392cef19083a907c8c9b88a15466f81df4; base main
  05aff0089cf4d0186486746507b5ef4392b023d3. Includes the final historical Fiji
  ROI ACK crossing and derives the environment fallback from ZMQConfig, rather
  than copying its default. Branch: fix/ack-return-route-20260930.
* OpenHCS draft PR297: https://github.com/OpenHCSDev/openhcs/pull/297
  head ddadd59803530e44bcca1df50835eb51c9a0e7a6; base main
  d3a99c0d46dea979bba3f9076da87386e49cbed3. Same consumer branch name.

Persistent isolated trees, respectively:
/home/ts/wt/zmqruntime-ack-return-route-integration-20260930;
/home/ts/wt/polystore-ack-return-route-20260930;
/home/ts/wt/openhcs-ack-return-route-20260930.
No paired gitlink edits, installs, downloads or package/environment creation.
Parent integrates later main/feature merges in their own fresh persistent trees.

Owned files and implemented contract
------------------------------------

Native ACK-owned source: ack_listener.py, queue_tracker.py, streaming/server.py,
messages.py, config.py, transport_modes.py under src/zmqruntime, plus pyproject.toml,
tests/test_ack_listener.py, test_ack_return_route.py, test_config.py and retained
tests/diagnostics/check_ack_startup_failure.py. Startup/client/execution/transport
changes from PR9 are inherited, not a competing implementation. viewer_state.py
comes from native main; it was not edited here.

PolyStore: src/polystore/streaming/_streaming_backend.py,
streaming/handlers/fiji_rois.py, zmq_config.py, pyproject.toml,
tests/test_streaming_metadata.py and test_streaming_return_route.py.
OpenHCS: runtime/napari_viewer_server.py, runtime/fiji_viewer_server.py,
__init__.py, pyproject.toml, and tests/unit/test_fiji_viewer_server.py,
test_napari_transport_ownership.py, test_ack_return_route_journey.py.
Active PR16 does not overlap the final PolyStore handler file.

AckReturnRoute owns connectable URL, UUID listener incarnation and exact
ProcessIdentity. ImageTransferIdentity owns image ID, return route and producer.
ImageAck carries that same contract. Existing dataclass projection/decoder derives
the wire fields; no new parallel key catalog or legacy reader exists. Tracked
items require a complete contract; genuinely untracked items send no ACK.

GlobalAckListener publishes only after successful bind, refreshes after process
incarnation change and rejects stale destinations before callbacks. Auto-bind is
the native ZMQConfig policy; explicit occupied addresses fail truthfully. The
existing transport declarations own TCP resolution, IPC lock/bind/no-steal and
own-path cleanup. No socket/route registry or receiver destination reconstruction.

Existing QueueTracker owns pending/processed IDs and explicit delegated-worker
admission. Unknown local ACKs do not invent progress. Worker incarnation admission
and idempotent accounting share the batch-reset lock; reset revokes admission.
Default producer saves use their own route. Deliberate delegation is an explicit
register_worker contract, tested through the actual listener, not implicit trust
in any unknown ID. ACK send sockets belong to the calling thread with finite
send/linger bounds, including deferred success/error and ROI paths.

Public-format classification and reviewed patterns
---------------------------------------------------

Required NRA/current owner archive/refactor-audit and full pattern references
were read before edits. Actual reviewed catalog IDs and source witnesses:
IDEN-6 (Keyed by the wrong identity): process-local listeners sharing 7555 and
viewer-derived destinations; TIME-7 (Copies of another component's defaults):
receiver/config ACK destination copies; BOUND-2 (Bypassing the class that already
models the data) and BOUND-1: ACK wire/transfer uses bypassing typed owners;
MEMB-5: hand-written message field projections; IMPL-13: separate weaker ACK
transport bind/cleanup; TIME-1/3/9: old global socket and Fiji setup/send aliases.
These replaced paths are deleted, not retained behind compatibility shims.

New-case ownership: a route field is declared once on AckReturnRoute and derived
by existing dataclass projection/decoder; a transport implements ACK binding in
its original TransportDeclaration rather than adding viewer/producer switches;
another viewer inherits the existing sender, carrying the same typed transfer.
No new registry/store/controller or name/type dispatch was introduced.

D1 reviewed against owner archive 00-RULES.md and GOAL-SCOPE-REFRACTOR-20260930.md:
ACK transfer/queue/listener state is ephemeral runtime state, reset at paired
process restart. No saved pipeline, preferences, biological inputs or results
format changes; therefore no durable-data migration. Public source constructor/
send API and internal wire cutover are deliberately incompatible: native and
PolyStore 0.3.0 with consumer bounds >=0.3.0,<0.4. OpenHCS 0.8.7 changes exact
application identity so a new sender does not silently adopt an old viewer.
Parent alone owns installing/activating the compatible set.

Actual evidence and limits
--------------------------

Parent's frozen two-owned-producer7555 collision and all51 artifact hashes/cleanup
are accepted; no science rerun. Before edits, a controlled two-owned TCP producer
check on 45995 also observed one bound listener and the second EADDRINUSE, no
false readiness, both owned processes exiting. No foreign7555 owner was touched.

Final provider-free source reports are in docs/validation:

* issue10_native_final.txt/xml: 136 passed in 8.96s. Real TCP/IPC producer routes,
  exact IDs, unknown/late/duplicate ACKs, stale incarnation, actual spawned worker
  delegation, independent stop and occupied explicit address preserving its owner;
  startup/client/bootstrap/owned-close regressions included.
* issue10_startup_diagnostic.txt: all three original controlled bind-failure
  assertions pass; it creates no live endpoint.
* issue10_polystore_final.txt/xml: 36 passed; image/ROI preparation, shared-memory
  cleanup and all-production-source deleted-alias guard. The final environment
  fallback is derived from the native owner.
* issue10_openhcs_final.txt/xml: 179 passed in 27.17s, including the continuous
  actual PolyStore save -> native REQ/REP -> OpenHCS transport pump/shared-memory
  copy -> deferred typed ACK -> exact producer queue journey, TCP and IPC, with
  separate native producer interpreters, warm second batch, independent close
  and producer allocation unlink. No replacement UI/protocol implementation.
  The final PolyStore handler/default-only follow-up does not alter this path.

Existing interpreter: /home/ts/code/projects/openhcs/.venv/bin/python;
explicit PYTHONPATH is native/src:PolyStore/src:OpenHCS, native thread env vars
OPENBLAS_NUM_THREADS=1 and OMP_NUM_THREADS=1; OPENHCS_CPU_ONLY=true for viewer
imports. Native check timeout30s, consumer timeout20s, continuous viewer check
timeout45s. Native pytest list: test_ack_return_route, test_ack_listener,
test_queue_tracker, test_config, test_messages, test_transport, test_owned_startup,
test_owned_close, test_endpoint_ownership. Consumer tests: test_streaming_metadata,
test_streaming_cleanup, test_streaming_shared_memory, test_streaming_identity,
test_streaming_return_route. OpenHCS unit list: test_ack_return_route_journey,
test_fiji_viewer_server, test_napari_accepted_work_settlement,
test_napari_streaming_handlers, test_napari_transport_ownership.

Original installed agent-comms ratchet executable, not copied:
/home/ts/.local/share/agent-comms/runtime-native-budget-request-progress-20260930/bin/python
-m agent_comms.debt_ratchet --root <production root> --base <above main> --head HEAD.
Final JSONs: issue10_ratchet_native_final (218 metrics), polystore_final (265),
openhcs_final (5128): no positive deltas. Native inherited ZMQClient excess -1;
OpenHCS FijiViewerServer excess -15 and foreign absence probes -1. Initial
mapping-growth and viewer foreign-state-probe failures remain as witnesses in
issue10_ratchet_native.json and issue10_ratchet_openhcs.json; source ownership
was corrected, not waived or hidden by replacing lookup syntax.

Full NRA native package plus installed python_introspect/metaclass_registry
source context completed, report issue10_nra_source_final.json/resources:
7.36s wall, peak156884KiB RSS, exit0. Same scan's compact report
issue10_nra_loop_final.json has scan_status exact_cache, complete=true,
analyzed_detector_count79, omitted_detector_count0. Command uses current NRA
/home/ts/code/projects/nominal-refactor-advisor/.venv/bin/python -m nominal_refactor_advisor,
src/zmqruntime, three explicit context roots, --parse-workers1 --analysis-workers1
--json --raw-findings --json-payload full (then loop), timeout45s. Ten raw findings
remain; eight semantic-authority findings concern original non-ACK owners such
as ControlResponse, ProcessIdentity projection and WorkerState. They are review
leads, not protocol failure assertions or a global cleanliness certificate.
The current tool does not provide the requested named_mapping_read,
unmodeled_record_shape or redundant_type_check R1 detectors; no fabricated R1
coverage or waiver. No executable NRA codemod recipe was claimed for this new
protocol contract; direct nominal source changes and boundary witnesses are
reported explicitly. Native changed-file Ruff F check and all git diff checks pass.

Preserved failure/remaining gate: historical PolyStore streaming.handlers import
fails with ModuleNotFoundError: metaclass_registry.lazy, before ROI handler body;
issue10_polystore_handler_import_failure.txt/xml retain the actual failed import.
Current OpenHCS uses its own original Fiji payload authority and does not import
this historical package. Its two obsolete ACK calls are migrated in source,
but that package is NOT live-verified; parent integration owner must adjudicate
its removal/current-owner migration if it is required. No mock import, new
compatibility registry or package install concealed this unrelated failure.
The broad native suite was time-bounded at40s with partial output, exit143,
retained issue10_native_source.txt; no full-suite pass claim. Initial test-fixture
IPC-path and shared resource-tracker problems were corrected in fixtures, not
production timeout increases. Existing viewer lint debt was not swept in.

Parent owns remaining required acceptance: normal current-main integration,
paired source/gitlinks/install, actual installed raw-stream/compile/run/readback,
deferred display/UI ACK settlement and final own-resource cleanup. This delivery
is published source evidence, NOT merged, installed, live-ready or biological
acceptance. Hosted CI is not a worker waiting gate.

Scope incident: the initial terminal-handoff read accidentally included its full
contents, not only infrastructure sections. No assay pixels, scientific source,
analysis ledger or evaluator input was opened and no science execution followed.
This worker must not be treated as blinded for scientific validation.

Cleanup and resource ownership
------------------------------

Owned scratch purpose: disposable NRA parse/analysis cache, path
/home/ts/.cache/agent-scratch/zmqruntime-ack-return-route-20260930 (39MiB at end).
Final audit/validation reports are preserved in the assigned persistent native
tree. Owned scratch, PolyStore coverage output, and the three fresh trees'
pytest/Python caches were moved to recoverable desktop Trash (gio trash), not
irreversibly deleted. The scratch path is now absent; no test runs after cleanup.
Only own generated data is removed; source, original histories, scientific
artifacts, foreign worktrees, locks/bus and parent displays are preserved.
Source footprint before cache retirement: native8.2MiB, PolyStore11MiB,
OpenHCS273MiB, own scratch39MiB: total331.2MiB, below512MiB.
Post-cleanup source footprint: native6.7MiB, PolyStore4.8MiB, OpenHCS270MiB,
total281.5MiB. Latest headroom check: home16.5GiB free, RAM15.8GiB available, swap9.4GiB;
20GiB condition is warning-only, disk reserve exceeds2GiB. Tests' own children,
contexts/sockets and IPC endpoints closed; no ACK test/NRA worker remains.
