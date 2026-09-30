# Execution progress annotation closure

Implementation owner: Lovelace. Parent owns paired OpenHCS pin/integration,
offline installation, actual MCP pipeline execution and blind activation.
Refs https://github.com/OpenHCSDev/openhcs/issues/243,
OpenHCSDev/openhcs#230, #233 and #242.

Isolated source: /home/ts/wt/zmqruntime-progress-annotation-20260929,
branch fix/progress-annotation-20260929, based on recorded/fetched main
1d3b32f4fcead23d5d2079c975eb2ae7646a1478 (already up to date).
Only production change: execution/progress_stream.py imports the existing
responses.WireValue so recursive WireResponse annotations resolve in the
ExecutionProgressObservation declaration namespace. The targeted F401 comment
records this runtime dependency; no new alias or public export is introduced.

## Reproducer and preservation

Original get_type_hints(ExecutionProgressObservation, include_extras=True) raises
NameError: WireValue is not defined. The new test file proves the same failure
for all four public progress annotation sites, direct dataclass_from_mapping
of a progress observation, and actual nested OpenHCS ExecutionJobStatus in both
RUNNING and COMPLETE states. All seven fail with NameError before the fix.

After the import correction:13 passed in2.46s, process3.18s, peak223464KiB, exit0,
zero skips. Includes the seven regressions plus existing progress_stream and
progress_projection tests. Running/completed integration imports the real
OpenHCS DTO, serializes it through its ordinary to_jsonable, and decodes through
the unchanged python_introspect.dataclass_from_mapping. It asserts concrete
progress identity, terminality, exact serialized round-trip, and nested immutable
progress with detached input/output mappings. No copied JobStatus schema,
raw-status consumption, custom codec or type-hints namespace monkeypatch.
Standalone downstream cases are optional if OpenHCS is absent; here they actually
ran against the frozen source and are not skips.

Durable logs: issue243_progress_annotation_20260929/{before-bootstrap-pytest,
before-bootstrap-resources,after-pytest,after-resources}.txt.
Initial before-pytest.txt also preserves a diagnostic import-order mistake:
two downstream cases hit OpenHCS's stale external-source guard, while five
dependency regressions already showed the NameError. The corrected source
test launch imports OpenHCS first to run its real source-dependency bootstrap,
then puts this reviewed ZMQRuntime src first before importing that dependency.
It does not edit installed files, replace sys.modules or bypass the guard.
Corrected before-bootstrap run confirms all seven actual NameErrors.

The logs print verified imports:

- ZMQRuntime: this isolated tree/src/zmqruntime/__init__.py.
- OpenHCS: frozen /home/ts/wt/openhcs-custom-function-admission-20260929/openhcs.
- python_introspect: that tree's recorded external/python-introspect/src.

Existing /home/ts/code/projects/openhcs/.venv/bin/python, explicit source paths
for ObjectState, PolyStore, arraybridge, metaclass-registry, pycodify,
pyqt-reactive and python-introspect, threads1, shared Fiji cache/downloadfalse,
PYTHONDONTWRITEBYTECODE=1 and disabled pytest plugin autoload. No environment,
package install, download, JVM/GUI, server or scientific input. Bounded pytest
acquired validation.lock nonblocking. Initial guard /home21.7GiB RAM15.3GiB,
final /home21.6GiB RAM15.8GiB; only historicalswap13.9GiB warning, no non-swap
waiver. Checkout plus receipts <1MiB; disposable test scratch4KiB removed after
terminal runs. Source and all failed/passing receipts preserved.

Focused Ruff and git diff --check pass. No full-suite/global NRA proof claimed.

## Pattern/owner review

Read current NRA/refactor-audit archived skill and pattern README, boundaries,
identity and over-time chapters. Focused source/declaration/caller review:

- BOUND-1/BOUND-2: the existing ExecutionProgressObservation and recursive
  responses.WireValue remain declaration authorities. Type-hint resolution is
  repaired at that owner; consumers still decode once through the existing
  generic converter. The nested real-DTO tests are the concrete consumer witness.
- TIME-3/TIME-8/TIME-9: no compatibility alias, second recursive schema or adapter.
  WireValue is imported from its original owner, not redeclared. WireResponse,
  field names, recursive payload and freeze/thaw implementation stay unchanged.
  Existing as_wire/from_wire and real OpenHCS serialization round-trip attest to
  the one wire representation and preserved immutability.
- New-case witness: additional nested progress context values resolve through
  the one recursive owner without caller namespace injection or a new decoder.
  Tests cover lists/mappings/bool/null plus scalar progress fields and subsequent
  sequence evolution; they do not claim general converter correctness.

PR6 wait_policy/test_execution_waiter and PR7 viewer_state/test_viewer_state are
disjoint and untouched. responses.py needs no change; python-introspect needs no
patch because the corrected declaration decodes with its existing mechanism.

## Remaining acceptance

Source fix/regressions are proven, not an installed MCP execution claim. Parent
retains original attempt06 compile/job handles and terminal receipts; no resubmit
or replay was performed here. Parent will pin this exact dependency commit with
OpenHCS242, install offline, then prove actual installed status/execute/output
journey. This draft references #243 without claiming completed live acceptance.
The separate image_analysis_workflow17387/16000 context defect remains owned by
Lovelace as a deferred follow-up; no frozen OpenHCS edits were made.
