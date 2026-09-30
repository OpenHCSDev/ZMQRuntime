# Canonical exclusive startup owner (OpenHCS issue251)

Extends the existing ZMQClient startup owner with explicit local empty-pair
startup, exact native ProcessIdentity capture, and pre-bind reservation in the
existing transport startup lock. No new launcher, registry, future, catalogue
warmup, takeover/kill/retry, or timeout increase. Post-spawn reservation failure
preserves the exact native handle as uncertainty. Existing fake nominal process
implementations migrated with their identity contract; no NotImplemented stub.

Paired OpenHCS draft will link https://github.com/OpenHCSDev/openhcs/issues/251.
Combined source shard62 passed including7 native-owner focused cases and existing
startup cases; no native runtime launched. Full native/installed acceptance and
endpoint-pair concurrency hardening remain explicit pending released serial slot.
Parent integrates paired revisions; worker does not merge or install.
