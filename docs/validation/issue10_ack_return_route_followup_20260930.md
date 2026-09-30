# Issue 10: ACK return-route follow-up

Status: superseded by tested source delivery on this same PR12. See
[final delivery receipt](../../ACK-RETURN-ROUTE-DELIVERY-20260930.rst).
The original checkpoint below is historical; its old ownership/resource holds
are not current. ACK ownership was explicitly reassigned to the fresh worker;
parent now owns final integration and serialized installed acceptance.

Historical status: ownership/acceptance checkpoint only, no delivery implementation.
Base: main 3374aa881dd9a8d0382ac6a56f764eb118744a7b, merged startup PR11.
Issue: https://github.com/OpenHCSDev/ZMQRuntime/issues/10
Owner: Dirac. Integration and paired gitlinks: parent.

## Accepted startup scope and remaining defect

PR11 fixes truthful startup and liveness on GlobalAckListener. Its production
change is ack_listener.py only. The original bind-failure witness is preserved;
the unchanged three assertions pass against the production fix, alongside 18
focused source tests. Parent independently verified those results and the exact
main-to-head packaged ratchet. None is installed or concurrent-delivery evidence.

The remaining ownership problem is distinct: process-local listener/queue
registries cannot independently bind one shared ACK endpoint. A different port
alone does not establish the sender's intended return destination. ImageAck's
existing image/viewer identity is not an explicit producer return-route identity.
The asynchronous per-image ACK is not the batch REQ/REP transfer receipt.
Actual frozen-run ACK misdelivery has not been demonstrated.

## File ownership before implementation

- Dirac retains ACK-private issue10 responsibility: src/zmqruntime/ack_listener.py,
  queue_tracker.py, streaming/server.py and ACK-specific tests. This checkpoint
  edits none of those production files; proposed queue/server extensions must
  first be checked against unpublished claims and the agreed shared contract.
- Lovelace retains PR9: src/zmqruntime/client.py, execution/server.py,
  messages.py and transport_modes.py, plus its bootstrap tests. The ZMQClient
  growth lead and factoring are Lovelace's work, not this follow-up's.
- src/zmqruntime/config.py and transport.py have no confirmed ACK-change editor.
  Default proposal: Lovelace remains the single shared-declaration editor;
  Dirac requests the required declaration extension and consumes it. No edit
  will occur until that owner accepts or explicitly assigns one editor.
- PR7's owner retains src/zmqruntime/viewer_state.py and its tests.
- PR159's owner retains OpenHCS core/config.py, streaming_config_factory.py,
  viewer_streaming_service.py and runtime viewer_protocol.py, Fiji/Napari
  visualizers and servers. Any return-route consumer extension is requested
  from that owner, not implemented as a competing viewer patch.
- PolyStore StreamingBackend sender/envelope changes need one explicitly named
  editor before editing; this checkpoint does not claim that tree.
- Parent alone owns coherent paired gitlink/integration changes.

Published claims checked at PR9 28d9ed6, PR7 40f9edb and PR159 d45ebbb.
Direct shared-file requests exist on PR9 and PR159. They are requests, not
confirmed agreements. The separate sept2026refact agent identity is unknown.

## Correct-maintenance decision boundary

Use original ZMQConfig, TransportEndpoint/TransportDeclaration and typed message
owners for any agreed address/return-route extension. Keep socket lifecycle and
startup failure with the existing listener/lifecycle owner. Decode external
input once through the original typed boundary. Preserve deliberate cross-worker
queue accounting rather than silently discard unknown image IDs.

Applicable catalog: IDEN-3/6 (address and producer identity), IMPL-13 (reuse
existing mechanisms), MEMB-5 and BOUND-1/2 (one typed record/decode authority),
TIME-1/3/7/9 (delete replaced paths, no aliases/default copies/adapters).
No parallel config map, routing registry, controller, private-port catalog,
compatibility shim or size-only helper. Shared behavior belongs to existing
ancestors; composed capabilities use declared MRO where genuinely applicable.
The specific delivery design remains undecided until shared owners agree.

## Required behavioral acceptance, not yet executed

Source contract: two producer incarnations with distinct outstanding image IDs
must retain their intended ACK destinations through the existing typed transfer
and ACK boundary; stopping one must not retire the other's accounting/resources.
Test original wrong-destination/collision behavior before and after the real fix.
Do not weaken the accepted startup failure, readiness or thread-ownership checks.

Live contract after explicit resource clearance and finite slot assignment:
run two owned concurrent streaming/application owners over TCP and applicable
IPC; verify ACK-ID delivery and queue progress without stealing/starvation,
truthful occupied-endpoint failure, independent shutdown and own-only cleanup.
Verify the installed affected entrypoint separately from source tests.

The resource guard still fails. No native/heavy/parallel/provider job, endpoint
bind, uncertain replay, installation or scientific rerun is authorized here.
Frozen pipelines, original warning, inputs and installed295e remain unchanged.
Full NRA package/dependency/R1 evidence is deferred, not reported as proof.
