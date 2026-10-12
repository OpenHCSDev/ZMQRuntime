# Changelog

## 0.6.5

Requires `python-introspect>=0.3.0,<0.4` (was `<0.3`).

Wire form:
- No message declared by zmqruntime changes on the wire: none of them has a field
  whose declared type admits several classes, so a 0.6.4 peer and a 0.6.5 peer
  exchange the same bytes for zmqruntime's own messages.
- An application message does change if it declares such a field (a union of two
  or more dataclasses, an abstract dataclass, or an `AutoRegisterMeta` class): the
  value is sent as `{tag: fields}` and read by its tag. There is no reader for the
  untagged form. zmqruntime has no protocol version of its own; a client refuses
  a server of another application version only when the application declares an
  `EndpointApplication(identifier, version)` and requires it, so applications
  whose messages changed form must raise that version.
- A field declared as one concrete dataclass carries that class only. A subclass
  instance with more fields is refused when decoded.

Fixed:
- A control request encodes its body by the declared field types, the same way it
  is decoded. Before, each body value was encoded on its own, so under
  python-introspect 0.3 a request field admitting several classes would have
  been sent untagged and then refused by the receiver.
- `ViewerInstance.visualizer` resolves at run time (`VisualizerProcessManager`
  was imported only for type checkers).
