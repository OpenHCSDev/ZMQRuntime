## Working ACK-private startup/liveness checkpoint; issue 10 remains incomplete

Tracks #10 without closing it. This draft now includes a production readiness
fix, bounded real-thread controlled-socket tests, the retained original witness
and the ownership/validation receipt.

### Claims and declaration owners

Dirac owns src/zmqruntime/ack_listener.py, tests/test_ack_listener.py, the
retained diagnostic and these receipts on fix/ack-delivery-owner-20260930.
Fresh published ZMQ9/159 production claims are disjoint. Parent also confirmed
the relevant external trees clean and notified Lovelace of the private boundary.

- Lovelace/ZMQ9 retains client.py, execution/server.py, messages.py,
  transport_modes.py and paired declaration work.
- No config.py, transport declaration, queue routing, streaming server,
  PolyStore, OpenHCS viewer/caller or paired gitlink change is made here.
- Shared routing/config/caller changes still need one agreed editor.
- Parent owns coherent paired gitlink integration.

Existing TransportEndpoint/TransportDeclaration own address derivation.
Existing EndpointStartupStatus/Phase, OperationDeadline and OperationCancellation
own lifecycle/budget/cancellation semantics. Future conveys the worker bind
outcome, not another registry or lifecycle family. No second endpoint authority,
port/default catalog, decoder, launcher or compatibility route is introduced.

### Working behavior

- start waits for bind and propagates the original failure after cleanup.
- _running derives from the original lifecycle declaration: binding and fatal
  failures never report ready; fatal receive errors retire the worker.
- Same-address starts share the outcome; different-address reuse is rejected.
- A short joining-caller wait cannot cancel the launch owner's attempt.
- Explicit retry is safe after ownership retires; no automatic replay.
- Stop cancels and joins only its worker, finitely and without self-join.
- Context/socket create, bind, receive and close remain on the listener thread.
- Launch, Future wait and join occur outside the lifecycle lock.
- Premature running flag, separate host/port/mode fields and fatal-error retry
  path are removed in place.

Per-call waits are bounded by 5000ms unless an existing typed operation deadline
is supplied. This is an ACK operation budget, not a changed endpoint default.

### Executed source evidence

17 focused checks PASS in 0.39s: 12 real-thread controlled-socket ACK cases plus
existing queue/config tests. Retained inline witness: all 3 PASS in 0.013s.
Original two failures and cleanup pass remain preserved at baseline 4a7864b.
No xfail or weakened readiness assertion. Fixtures check every socket operation
and context termination occurs on its original worker; no real endpoint opens.

Run with the existing OpenHCS interpreter, PYTHONPATH pointing to this tree's
src, PYTHONDONTWRITEBYTECODE=1, numerical-library thread limits=1, pytest plugin
autoload disabled and cache provider disabled, bounded by timeout 10s:

    python -m pytest -q -p no:cacheprovider tests/test_ack_listener.py tests/test_queue_tracker.py tests/test_config.py
    python tests/diagnostics/check_ack_startup_failure.py

Critical swap remains (16.4 GiB). No heavy/native/provider job, installed change,
foreign process close, scientific replay or biology artifact change occurred.
Full NRA scan and installed/live acceptance are deferred, not certified.

### Shared coordination and remaining acceptance

Existing substantive ownership requests:
[Lovelace/native PR9](https://github.com/OpenHCSDev/ZMQRuntime/pull/9#issuecomment-5908226428),
[viewer PR159](https://github.com/OpenHCSDev/openhcs/pull/159#issuecomment-5908226994).
Posting is confirmed; shared editor agreement is not yet confirmed.

Cross-process ACK return-route/config semantics and any PolyStore/OpenHCS caller
changes remain unfinished. After direct file agreement and explicit resource
gate/slot release, verify concurrent TCP/applicable IPC streaming owners,
their own ACK IDs/queue counts, occupied-endpoint failure and stopping one while
the other continues. Source checks do not prove installed readiness or issue10
completion. Frozen biology, installed295e and user viewer5690 remain untouched.
