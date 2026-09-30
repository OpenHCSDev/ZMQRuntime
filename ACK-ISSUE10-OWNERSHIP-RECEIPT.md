# Issue 10: source investigation and ownership checkpoint

Date: 2026-09-30. Independent task owner: Dirac.
Issue: https://github.com/OpenHCSDev/ZMQRuntime/issues/10
Worktree: /home/ts/wt/zmqruntime-ack-delivery-owner-20260930
Branch: fix/ack-delivery-owner-20260930
Base: 0f9e840a9ed526f0a2b87a660393f5b7c34fc9eb (origin/main).

## Safety and implementation status

Source investigation and diagnostic checkpoint only. No production edits,
installed changes, package/environment changes, native/viewer/MCP startup, or
real endpoint bind. The H003 scientific freeze, inputs, report and original warning remain
untouched. The absence of a current 7555 listener is not collision resolution.
Lovelace retains the sole live acceptance slot.

The resource guard returned exit 2 for historical swap usage (15.6 GiB).
Available RAM was 14.7 GiB; home disk headroom was 27.8 GiB. Follow-up vmstat
showed zero current swap-in/out and memory PSI averages were zero. Work was
limited to serial source reads and one short, thread-limited provider-free
diagnostic using the existing OpenHCS interpreter. No heavy suite or fleet ran.

## Required guidance read

Read the installed NRA and refactor-audit skills and the current archive at
/home/ts/code/projects/nominal-refactor-advisor/skills/refactor-audit.skill.
Relevant current chapters read fully: boundaries, identity, membership,
implementation and over-time, plus the surface-receipt instructions.
This is a source-backed diagnostic checkpoint, not an NRA proof or full scan.
The guidance requires ACK identity/routing and lifecycle facts to remain with
their original typed owners, rather than a second registry, decoder or launcher.

## Verified source trace

1. PolyStore's StreamingBackend._register_with_queue_tracker starts
   GlobalAckListener at transport_config.shared_ack_port, then registers each
   image ID in its process-local GlobalQueueTrackerRegistry. Its save_batch
   invokes this before sending the batch over REQ/REP.
2. ZMQConfig owns shared_ack_port=7555. GlobalAckListener is a Python singleton,
   not a cross-process owner. Its start marks _running=True before its listener
   thread binds. Already-running calls do not verify requested endpoint identity.
3. The listener logs fatal bind failures and closes its socket/context but does
   not clear _running or propagate the startup failure to the caller.
4. StreamingVisualizerServer creates a PUSH socket connected to its config's
   shared ACK endpoint. send_ack emits the original ImageAck declaration:
   image_id, viewer_port, viewer_type, status, timestamp and error. No producer
   incarnation/return-route identity appears in that declaration.
5. The default listener callback routes by viewer port into the local registry.
   QueueTracker.mark_processed deliberately counts an unregistered image
   retroactively, with a source comment attributing this to worker-local
   registries. Missing trackers instead update ViewerStateManager directly.
   Thus blindly rejecting unknown images or choosing an arbitrary new port would
   alter intended cross-worker progress semantics; neither is an established fix.

The collision mechanism and ownership risk are source-backed. Actual ACK
misdelivery or starvation in the frozen science run has NOT been demonstrated.
The per-batch REQ/REP transfer receipt is distinct from asynchronous per-image
PUSH/PULL progress ACKs; success of one does not establish health of the other.

## Provider-free reproducer (observed)

Executed against this worktree's ack_listener.py with the existing interpreter,
PYTHONDONTWRITEBYTECODE=1 and thread limits=1. unittest.mock replaced zmq.Context
with a controlled context/socket and threading.Thread with an inline test
thread. ControlledSocket.bind raises ZMQError(EADDRINUSE). The real listener
start and loop were called, then start was called again.

Observed JSON:

```json
{
  "real_endpoints_opened": 0,
  "start_returned": null,
  "fatal_bind_attempts": ["tcp://*:7555"],
  "thread_completed": true,
  "running_after_fatal_bind": true,
  "socket_closed": true,
  "context_terminated": true,
  "second_start_ignored": true
}
```

This establishes false readiness/stuck startup in source only. It does not
substitute for concurrent native or installed streaming evidence.

## Shared-file coordination blocker

PR9 is OPEN, head 28d9ed6a0121ebb524ae37fd97de5355308bda7f,
feat/exclusive-endpoint-bootstrap-20260930. It owns client.py,
execution/server.py, messages.py and transport_modes.py, related startup/close
tests, and its validation docs. PR7's published scope is viewer_state.py and
test_viewer_state.py. Unpublished claims still require direct confirmation.

Requested peer: Lovelace 01a0ee61-a6b6-7b81-af1f-154b3cddbbca.
No agent-session messaging tool is exposed here. The installed agent-comms CLI
resolves the owner-installed active route, but this ID is not a registered
thread, and the bounded owner metadata lookup found no Lovelace or Dirac route.
No guessed-root write, identity impersonation, foreign-tree edit, or agent-reply
polling was performed. Need Lovelace's registered comms name/root or a direct
ownership response delivered into this session.

Proposed scope to negotiate, NOT a finalized claim: ack_listener.py,
streaming/server.py, queue_tracker.py, their focused tests; declaration changes
in messages.py/config.py/transport_modes.py and PolyStore's streaming sender and
typed batch-envelope owner only if the traced delivery fix requires them.
Lovelace owns bootstrap; no competing bootstrap implementation will be made.

## Exact acceptance remaining

### Updated crossings and guidance confirmation

Rechecked published heads after the owner's latest scope clarification:
OpenHCS256 is 5ca21c798642e820f224eebf294fc54e0dc3f965;
ZMQRuntime9 remains 28d9ed6a0121ebb524ae37fd97de5355308bda7f.
OpenHCS159 is d45ebbb61da0463885e81e654a5c8ab895f41398.
The actual listener path is src/zmqruntime/ack_listener.py, not
src/zmqruntime/streaming/ack_listener.py.

| File/surface | Proposed single editor; current claim boundary |
| --- | --- |
| ACK listener, streaming/server.py, queue_tracker.py and focused ACK tests | Dirac, subject to Lovelace's unpublished-claim confirmation; not yet an agreed exclusive file claim |
| ZMQ config.py | No final editor agreed; endpoint semantics remain on ZMQConfig, not a new ACK config |
| ZMQ client.py, messages.py, transport_modes.py, execution/server.py | Lovelace/ZMQ9; request declaration/transport extensions from this owner rather than editing concurrently |
| ZMQ execution/wait_policy.py | Existing ZMQ6 owner; outside ACK edits |
| ZMQ viewer_state.py | Existing ZMQ7 owner; outside ACK edits |
| OpenHCS core/config.py, core/streaming_config_factory.py, core/viewer_streaming_service.py, runtime/viewer_protocol.py, runtime/fiji_stream_visualizer.py, runtime/fiji_viewer_server.py, runtime/napari_stream_visualizer.py, runtime/napari_viewer_server.py | Existing OpenHCS159 owner; identity/direct agreement not established here |
| PolyStore streaming sender and typed batch envelope | No final editor agreed; cannot silently change caller-owned delivery semantics |
| Paired dependency gitlink integration | Parent, explicitly assigned |
| Issue257 acceptance/versioned fixture | Parent only; no production runtime change inferred |

Separate archive-refactor agent identity: not known. Do not infer an agent
identity from the shared GitHub account or claim a peer agreement that has not
occurred. A fresh bounded owner-metadata lookup again found neither the supplied
Lovelace ID nor Lovelace/Dirac aliases in the selected installed comms route.
No comms message was sent merely to relay the parent's instruction.

The NRA skill, installed refactor-audit skill, current repo archive SKILL.md,
catalog README and complete identity, membership, implementation, boundaries
and over-time chapters were reread for this checkpoint. Relevant reviewed
patterns/guards:

- IDEN-6: inspect whether viewer-port routing answers the actual producer/ACK
  ownership question across processes. The existing worker retrofit behavior is
  part of the required relation, not permission to discard unknown IDs.
- IMPL-13: extend the existing transport/lifecycle ownership mechanism rather
  than build a second, weaker launcher or endpoint-cleanup authority.
- BOUND-1/BOUND-2: decode at the ACK/batch boundary using the original typed
  declaration; downstream consumers do not invent another shape/decoder.
- MEMB-5: derive wire projections from the original declaration rather than
  maintain a second handwritten schema or registry.
- TIME-7: consumers derive default endpoint semantics from their owner; real
  explicit user overrides remain overrides, not copied fallback defaults.
- TIME-1/TIME-9: any replacement delivery mechanism cuts over in lockstep and
  removes the replaced path, without a legacy route or compatibility adapter.

These are source-backed ownership risks and implementation guards, not a claim
that every pattern has been independently measured as an existing violation.
No new census, NRA scan or native proof is claimed at this checkpoint.

Ownership decision: original ZMQConfig/TransportDeclaration and ImageAck/batch
declarations own endpoint, transport and ACK semantics. Producers and viewers
consume/derive those declarations. No duplicate config, registry, arbitrary
port assignment or bootstrap implementation. Direct agreement remains required
before any shared source edit. Parent's projection fixture/provenance checks
are outside this ACK implementation and will not be weakened here.

### Visible provider-free checkpoint

The owner requested a visible draft PR after OpenHCS275 merged (reported
cd047c4, script/test/RST only). The new independent file is
tests/diagnostics/check_ack_startup_failure.py. It changes neither a shared
production file nor the original native/viewer owners' tests. The diagnostic
exercises the worktree's actual listener start/loop with a controlled failing
socket and inline test thread. It has no xfails, suppression of failed
assertions, real socket binds or real listener threads. It is not automatically
pytest-discovered: it is an explicitly failing acceptance witness, not a claim
of a merge-ready fix.

Resource guard immediately before this checkpoint still returned exit 2,
level=critical, swap=16.6 GiB, available RAM=18.5 GiB, home free=25.9 GiB.
No heavy job ran. Existing Python, thread limits=1, timeout=10 seconds:

```sh
timeout 10s env OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  /home/ts/code/projects/openhcs/.venv/bin/python \
  tests/diagnostics/check_ack_startup_failure.py
```

Three checks execute: the controlled socket/context cleanup check passes;
truthful startup failure propagation and failed-listener running-state checks
fail on the unmodified production baseline. A nonzero exit is the retained
defect evidence, not a timeout or a passing readiness claim. This checkpoint
does not resolve delivery ownership or issue 10 and must not close the issue.

After claims are reconciled: implement truthful bind readiness/failure and
correctly owned ACK delivery using existing typed declaration/transport owners;
delete the replaced delivery mechanism, without compatibility routing/defaults.
Add focused provider-free tests for failed bind, truthful healthy/dead state,
endpoint mismatch, correct ACK routing/queue counts, duplicates and cleanup.
Publish the coherent implementation promptly as a draft PR linked to issue 10.

Only after parent assigns the finite live slot: run two owned controlled
application/native streaming owners concurrently through the existing entrypoint
on TCP, then applicable IPC. Observe bind outcomes, each owner's own image IDs,
ACK receipts and queue progression; show no ACK stealing/misrouting or dead
listener reported healthy. Stop one owner and prove the other continues ACK
delivery; verify teardown releases only owned endpoints/resources. Include
controlled occupied-endpoint startup failure and truthful surfaced failure.
No foreign 7555 listener intervention, science rerun, timeout inflation or
installation mutation is authorized by this checkpoint.
