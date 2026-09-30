# PR9 canonical endpoint ownership checkpoint

Implementation: `4434b11ed769cbc6a15d13754b06529ea6494428`, after normal
integration of ACK main `3374aa881dd9a8d0382ac6a56f764eb118744a7b`.
Parent retains paired-pin review, integration, installation and live acceptance.
This is a source checkpoint, not activation or closure of OpenHCS issue251.

## Behavior owners and replaced paths

The existing TransportEndpoint owns the address pair. Its startup_lock now holds
both original declaration-owned locks in canonical order. Its admission,
publication, exact-incarnation check and provisional reservation transaction
consume the original TransportDeclaration record reader/writer/release methods.
The corresponding client procedures are deleted, not retained as another store,
launcher, lock resolver or rollback controller. Both explicit startup and ordinary
connect consume the pair owner. Ordinary connect cannot spawn through a pending
owner at either address; attach-only connect_existing remains non-starting.

Pre-spawn publication/deadline/cancellation is the only rollback scope. Both locks
remain held; release matches the original ProcessIdentity and truncates the
existing inode. Unknown, partial, changed and child claims survive. Spawn and
child-publication failures remain outside rollback, preserving uncertainty and
the original child handle. No automatic launch/shutdown replay was introduced.

The existing _EndpointShutdownOperation previously owned completion state but
the client owned request admission/dispatch. The operation now owns this entire
one-send procedure, using the same deadline, identity, socket and acknowledgement
disposition. The existing EndpointShutdownMode admits exact-owned close through
the pair owner, then completes that same operation. The client boundary selects
the nominal mode/config/deadline; no new helper service or lifecycle family.
FORCE still proves exact process exit separately from listener disappearance;
GRACEFUL still retains the server. The replaced client procedure is deleted.

The existing TransportEndpoint now owns discovery_response and scan. Its ping
and control-port declarations supply typed heartbeat/address projection; the
old client closure/raw ping and copied projection are deleted. Original query
order, absent/pending outcomes, 32-worker cap and nonblocking executor teardown
are retained. No discovery registry, cached endpoint mirror or second decoder.

New-case witnesses:

- A changed pair topology/transport record semantics updates the existing pair
  or transport declaration. Startup, ordinary connect and close all consume it;
  tests use TCP/IPC with control offsets1/17, exact config/deadline and inode proof.
- A new shutdown acknowledgement/terminal disposition belongs to the existing
  operation/mode, not independent dispatch and completion procedures in client.
  Original owned-close controls retain one-send/unknown/changed-incarnation cases.
- Specialized discovery heartbeat behavior changes the endpoint ping owner.
  A test endpoint subclass is consumed by the collector without another client
  parser/address formula; absent, duplicate and pending results retain ordering.

## Applicable catalog review and crossings

Current NRA skill and authoritative archived refactor-audit skill/catalog were
read before these decisions. IMPL-12: deleted replaced client procedures;
IMPL-13 and IDEN-8: canonical lock/record and exact process-incarnation ownership,
no process registry or lock-inode replacement; BOUND-2: existing record decoder
and typed PongResponse, no copied wire parser; AGENT-6: behavior moved to three
pre-existing semantic owners with new-case witnesses, not a size-only mixin or
new forwarding controller. This is a scoped review, not all-detector/global proof.

Whole PR9 production claim: client.py, execution/server.py, messages.py,
transport_modes.py and the additional TransportEndpoint surface in transport.py.
No edits to ACK-private listener/server/queue files, config.py, return-route,
viewer_state or execution/wait_policy. PR6/7 published files are disjoint.
Direct additional TransportEndpoint crossing sent to Dirac in
[PR11 comment](https://github.com/OpenHCSDev/ZMQRuntime/pull/11#issuecomment-5910632999).
Open PR inspection found no competing claim; direct acknowledgement/unpublished
claim clearance is NOT verified. Do not claim a prior agreement or merge this
crossing on that assumption. The broader sept2026refact archive author remains
unidentified. BLOCKED S1 is not resumed.

## Actual provider-free source evidence

Existing Python3.12, explicit own native/metaclass/python-introspect/pycodify src
paths, -B, plugin autoload disabled and CPU thread1. No install/environment change.
Bounded30s pytest shard: test_owned_startup.py, test_owned_close.py,
test_endpoint_ownership.py and test_startup.py.

**81 passed, 0 skips/deselections,0.44s; process0.63s/44776KiB RSS/exit0.**
Popen/process/native I/O intercepted; original file locks/reservation records
are real. Discovery uses controlled synchronous futures, not executor threads.
No sockets, native/MCP/JVM/GUI/provider/scientific launch. Initial79-case XML
retained separately before ordinary-connect pending-owner closure.
Ruff undefined-name checks and git diff --check pass.
Durable XML in the paired OpenHCS tree:
[native-owner-tests-b.xml](https://github.com/OpenHCSDev/openhcs/blob/feat/owned-runtime-bootstrap-20260930/docs/validation/runtime_bootstrap_20260930/native-owner-tests-b.xml).

After normal OpenHCS main4a9c9b3e integration, paired actual-source suite:
**181 passed, 0 skips/deselections,5.00s; process5.63s/257380KiB RSS/exit0**.
Includes100 OpenHCS and81 native controls. Recorded parent gitlink remained
28d9ed6 during this source check; PYTHONPATH used reviewed child4434b11 explicitly.
This distinction is not an installed/pin-adoption claim.

## Original packaged native ratchet

Original agent-comms debt_ratchet.py at3b03785f45df2ef5dc62ba6aed99294192ecbb01,
SHA256 e323c94d49c2b72d9524a5169f123e64b4a6e46a41035ca9fb4497e49b6ca562;
used its existing package, not a copied measure or heuristic substitute.
Existing Python3.14,30s shell bound:

```sh
python3.14 -B -m agent_comms.debt_ratchet --root src/zmqruntime \
 --base 3374aa881dd9a8d0382ac6a56f764eb118744a7b --head 4434b11
```

**PASS186 projected metrics, zero positive deltas,4.42s/48180KiB RSS/exit0.**
ZMQClient excess127->124 (-3); only nonzero projected delta. Whole native-root
inventory plus all five changed production paths, not a selected-class-only pass.
ACK is unchanged against the merged ACK baseline. JSON/resources committed next
to this receipt. Parent's original0f9e840->28d9ed6 +142 failure remains historical
evidence, not relabeled passed. Existing client excess124 remains debt; this
closes introduced growth, not complete decomposition or semantic/live correctness.
No R1/global NRA scan or increased timeout.

## Boundaries and remaining acceptance

Latest guard was warning/exit2: swap14.6GiB, RAM18.4GiB, /home22.0GiB.
Only bounded serial lightweight checks ran; no native slot or parallel fleet.
Original native attempts01/02/03 and their exact terminal/uncertain dispositions
are unchanged. No attempt04, input/source dispatch replay, foreign close or
installed/managed-skill change. Actual source-native startup/preparation/register/
identity-proven close are historical partial evidence, not valid3D acceptance.
Remaining: Dirac shared-surface acknowledgement, parent reviewed dependency pin,
successful corrected volume compile/execute/full readback under admission, and
actual installed user-entrypoint acceptance. No Closes251 or global clean claim.

## Subsequent normal viewer-main integration and scoped comment correction

Normally merged native main2aa6d21c000d18bd75647f998794015ba8f54710 at6e4242d3.
Incoming source only viewer_state.py and its tests, disjoint from the bootstrap
write set. No competing viewer/ACK edit. Corrected one misleading own docstring
at84b93a0: record_startup_owner publishes an exact provisional invoker OR child,
not only a spawned child. The real callers are TransportEndpoint.reserve_startup_owner
and post-spawn record_startup_owner. Their existing identity/rollback controls
prove both stages; no test of docstring spelling or new policy/codec/reader.
Retained useful uncertainty, incarnation, lock-inode and one-budget explanations.
This is a scoped source-comment drift correction, not a global slop census.

At paired OpenHCS source260e5941b (normally integrated currentmaincad1ed2bd),
**187 source casesPASS5.51s**, process6.41s/265012KiB/exit0, zero skips/deselections.
Original181 bootstrap/pair controls plus6 original viewer-state controls.
Controlled visualizers and one bounded lock-probe thread, not a GUI/native viewer;
source roots explicitly override the separately installed viewer-live authority.
JUnit viewer-main-source-tests.xml/resources retained in paired OpenHCS receipt.

Original packaged native ratchet against2aa6d21c->84b93a0: **PASS188 metrics,
zero positive deltas,4.48s/48772KiB/exit0**, client excess delta-3 only.
JSON/resources issue251_viewer_main_ratchet_20260930.json and
issue251_viewer_main_ratchet_resources_20260930.txt. The parent's earlier native
viewer screening lead is not waived/reclassified; this comparison uses current
viewer main as its base and measures own introduced delta. No R1/full NRA proof.
An initial ad-hoc report summary queried plural deltas and failed KeyError;
the original ratchet itself exited0. Corrected read of its actual delta field
with jq confirmed188 metrics/no positives. Original tool output remains distinct.

Resource helper WARNING/exit2, swap14.4GiB/RAM17.4GiB/home20.1GiB.
No native/MCP/JVM/GUI, heavy/parallel job, install, ABI copy or foreign process
interaction. Original attempts and BLOCKED S1 remain preserved. R0/L0 landed
does not complete full archive S1-S8. Wider owner unknown; direct TransportEndpoint
crossing acknowledgement remains unverified. Parent integration/live ownership
and remaining valid-volume/installed acceptance gates remain as above.
