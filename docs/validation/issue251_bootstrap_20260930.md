# Canonical exclusive startup owner (OpenHCS issue251)

Extends the existing ZMQClient startup owner with explicit local empty-pair
startup, exact native ProcessIdentity capture, and both-address pre-bind reservation in the
existing transport startup lock. No new launcher, registry, future, catalogue
warmup, takeover/kill/retry, or timeout increase. Post-spawn reservation failure
preserves the exact native handle as uncertainty. Existing fake nominal process
implementations migrated with their identity contract; no NotImplemented stub.

Paired [OpenHCS256](https://github.com/OpenHCSDev/openhcs/pull/256) references issue251;
[metaclass-registry1](https://github.com/OpenHCSDev/metaclass-registry/pull/1) owns
non-creating cache projection. Combined source shard66 passed including9 native-owner
focused cases, existing startup cases and offline CLI projection; no native
runtime launched. Full native/installed acceptance and cross-process endpoint-pair
race evidence remain explicit pending released serial slot.
Parent integrates paired revisions; worker does not merge or install.
