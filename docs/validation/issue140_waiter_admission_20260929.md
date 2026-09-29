# Waiter status-observation admission: bounded dependency repair

Implementation owner: H003/Codex waiter worker, assigned by the OpenHCS coordinator. Base audited: ZMQRuntime main `1d3b32f4fcead23d5d2079c975eb2ae7646a1478` (0.2.24), also the pinned OpenHCS dependency. Persistent isolated worktree: `/home/ts/wt/zmqruntime-issue140-waiter-20260929`, branch `fix/issue140-waiter-status-admission-20260929`.

Related investigation: [OpenHCS PR160 / issue140 dependency review](https://github.com/OpenHCSDev/openhcs/pull/160#issuecomment-5897408687). This is the paired dependency implementation for that specific finding, not a competing OpenHCS polling implementation and not proof of the cause of the original reported job hang. Do not close issue140 on these unit probes alone.

## Boundary and ownership

At base `src/zmqruntime/execution/wait_policy.py:53–59`, a successful `_poll_status` return reset `consecutive_errors` and the progress baseline before `ExecutionStatusSnapshot.from_dict` could reject the response. Repeated actual decoding failures were logged as attempt1/3 indefinitely without a liveness callback or advancing progress.

The socket receive and an admitted job status are different observations (IDEN-1/4); the existing snapshot decoder already owns the response boundary (BOUND-1/2). The replacement deletes the old pre-decode admission and moves that existing decode before the waiter's reset and progress-baseline update. It adds no schema copy, polling cache, deadline, new status carrier, dispatch roster, OpenHCS timeout or fallback (IMPL-13 prevention). Terminal execution state remains on the existing record/lifecycle owners. Existing known-live-process and advancing-progress retry exemptions are unchanged; this patch is not a new bounded policy for those cases.

Scope is decode failure accounting under the existing decoder. It does not add stricter acceptance of every currently tolerated response shape, alter job-ID validation, or refactor unrelated status enum/serialization behavior. Runtime-local counters only; no durable migration, public API, wire format or version/gitlink change.

Ownership checks before edits: dependency open PR list contained only Dependabot #5 touching `.github/workflows/publish.yml`; no competing waiter implementation or active dependency worktree was found. OpenHCS PR160 is investigation-only at `ef3c05ca8563234f9f9102c2b0a1d0188d622330`. PR159's viewer scope is the coordinator's, and Confucius owns the heavy validation lock/frozen installation. This worker did not edit OpenHCS production code or a shared gitlink.

## Provider-free behavior evidence

New tests import the actual waiter and actual `ExecutionStatusSnapshot` decoder, without a decoding stub. The poller is an in-memory bounded observation source; its overrun guard uses `pytest.fail` outside the waiter's `Exception` handler so the broken loop cannot hang the suite. `time.sleep` is patched out for every new probe, plugins are not auto-loaded, and each command has an external30s limit. No socket/server/process launch, provider call, model, MCP, JVM, viewer or environment installation is involved.

Command, from this worktree:

```sh
env PYTHONPATH=/home/ts/wt/zmqruntime-issue140-waiter-20260929/src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 timeout 30s /home/ts/code/projects/openhcs/.venv/bin/python -m pytest -q tests/test_execution_waiter.py
```

- Before production repair: **4 failed, 10 passed in0.27s**. Missing status, unknown response enum and incomplete execution each logged1/3 three times and exceeded the three-observation guard. Mixed transport/decode failures also exceeded it. This directly reproduces the reviewed accounting defect.
- After repair: **14 passed in0.15s**. Malformed statuses stop after exactly3 observations and logged attempts1/2/3; mixed transport/decode failures share the same counter. A valid RUNNING observation resets two prior errors, permitting a new independent error streak and successful completion.
- Unchanged behavior controls:100 healthy RUNNING observations with unchanged progress still complete (no poll-count deadline); known-live-process timeout tolerance and advancing progress on transport/decode failures remain; COMPLETE summary, FAILED diagnostics, CANCELLED response, explicit server ERROR and lost-process signal diagnostics remain. Unknown-process transport loss remains bounded.
- Existing waiter/client controls plus the new probes: **19 passed, 52 deselected in0.32s**, using the same command with `tests/test_execution.py -k 'execution_waiter or execution_client_composes_known_server_liveness_into_waiter'`. Selected existing cases have zero poll/backoff delays or live/progress exemption. The unrelated server/process integration tests were not executed.
- `ruff check tests/test_execution_waiter.py` and `git diff --check`: pass. Production `wait_policy.py` has pre-existing I001/UP035 import warnings on the base; the semantic patch does not modify imports or claim repository-wide lint cleanliness.

Resource check: RAM11.6GiB available; historical swap11.9GiB warning only. No heavy validation lock was acquired, no installed source was mutated, and the preserved H003 scientific work was not accessed or changed by this implementation task.

## Architectural evidence limits

Both required skills, archived current refactor-audit skill, catalog README and complete boundary/identity/implementation chapters were read. The current NRA CLI analyzed waiter/messages with explicit complete-package context root, raw findings, full JSON, no cache, one parse worker and20s internal/30s external budgets. The first terminal response truncated; a producer-side jq projection preserved the subsequent selected raw receipt. It reports six supporting structural findings but does not expose `scan_status` or detector coverage on that payload surface. **No complete zero-omission audit, native equivalence proof or synthesized codemod application is claimed.** The waiter error-accounting conclusion comes from verified source control flow and red/green behavior probes, not from promoting an unrelated structural finding.

Raw leads include mirrored `_LAZY_EXPORTS`/ControlResponse, WorkerState fields, ProcessIdentity projection and external enum recovery. They are outside this assigned repair. Execution-subpackage overlay reports raw shapes in server/status_poller, not a reason to create another waiter schema. Its locally reported dead modules are consumed outside that subdirectory, so they are not deleted. The census `--base HEAD` compares committed HEAD..HEAD and does not measure this uncommitted patch; no zero-debt delta is claimed from it. New-case check: a new decode rejection is handled by the existing decoder/Exception path, as the three malformed-shape cases demonstrate; no second caller-owned admission rule is needed.

## Remaining integration gate and shared gitlink

The source-level waiter defect is fixed and bounded tests pass. Actual ordinary-ZMQ injected worker failure, matching execution identity/status/progress observations, healthy long job, cancellation and owned-process loss through OpenHCS remain a separate serialized integration gate. The original issue140 diagnosis remains open until that evidence is available.

Coordinate dependency merge/release and OpenHCS gitlink acquisition with the parent, using existing PR160. Do not alter the shared/frozen installation or advance its gitlink from this worker. Parent owns159/212 followups and live-slot scheduling. The paired draft is source-validated only, **not merged, installed or live-verified**.
