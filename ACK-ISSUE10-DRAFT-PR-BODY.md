## Diagnostic checkpoint only; not merge-ready or the ACK fix

Tracks #10. This draft makes the source reproducer and ownership receipt
visible while direct production-file agreements and the finite live slot are
pending. It does **not** fix or close #10.

Changed files only:

- `tests/diagnostics/check_ack_startup_failure.py`
- `ACK-ISSUE10-OWNERSHIP-RECEIPT.md`
- this draft body

Production source, configuration, transport declarations, PolyStore, viewers,
installed packages and paired gitlinks are unchanged. Parent owns paired
gitlink integration. Lovelace retains bootstrap and the assigned native slot;
the existing OpenHCS159 owner retains viewer configuration/protocol/server work.

## Executed evidence

The existing OpenHCS interpreter runs three serial provider-free checks against
this worktree's real ACK start/loop with mocked Context and inline test Thread:

- controlled socket/context cleanup passes;
- caller-visible bind failure fails (`ZMQError not raised`);
- truthful failed-listener state fails (`_running` remains true).

The failing checks are retained without xfail or weaker assertions. The script
is an explicit diagnostic, not part of automatic pytest discovery. Its nonzero
exit demonstrates the current defect, not a completed fix. No real endpoint,
listener thread, native process, viewer or provider call is opened.

The resource guard remains critical (swap 16.6 GiB). No native/heavy job ran;
the diagnostic is bounded by 10 seconds and numerical-library thread limits=1.

## Shared-file agreement required before production edits

Proposed ACK implementation owner: Dirac, with unpublished claims still to be
confirmed for `ack_listener.py`, `streaming/server.py`, `queue_tracker.py` and
focused ACK tests.

- Lovelace / ZMQ9 retains `client.py`, `messages.py`, `transport_modes.py`,
  `execution/server.py`. Any ACK declaration/transport extension must have one
  agreed editor; no competing bootstrap will be implemented.
- `config.py` needs an explicit editor agreement if endpoint semantics change.
- OpenHCS159 retains its config/factory/streaming service, viewer protocol and
  Fiji/Napari visualizers/servers. Any caller change needs that owner's agreement.
- PolyStore sender/batch-envelope changes also need a named single editor.
- ZMQ6 wait policy and ZMQ7 viewer state are outside this checkpoint.

NRA/refactor-audit ownership decision: original endpoint/transport/message
declarations own semantics; consumers derive them. No copied defaults,
duplicate registry/decoder/launcher, arbitrary port assignment or compatibility
delivery path. Relevant reviewed patterns are recorded in the receipt.

## Remaining acceptance

After direct file agreements, implement truthful startup/liveness and correctly
owned ACK delivery, then run focused source tests. After an explicit resource
gate and slot release, verify concurrent controlled TCP and applicable IPC
streaming owners, each owner's ACK IDs/queue counts, bind failure visibility,
and stopping one owner while the other continues. Source diagnostics do not
establish installed or concurrent-native readiness. Frozen science, original
collision evidence and the user's preserved viewer remain untouched.
