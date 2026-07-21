Viewer Protocol API
===================

``zmqruntime.viewer_protocol`` is the source of truth for the viewer transport
vocabulary and the builders that validate its JSON-compatible portions.  The
aliases ``ViewerBatchWireField``, ``ViewerBatchItemWireField``,
``ViewerDisplayConfigWireField``, and ``ViewerBatchContextWireField`` all name
the same authoritative :class:`~zmqruntime.viewer_protocol.ViewerWireField`
declaration; they provide type context without duplicating field lists.

Applications remain responsible for assigning domain meaning to component and
axis values.  The protocol classes preserve, validate, and transmit those
declared values; they do not inspect image arrays to infer semantic axes.

.. automodule:: zmqruntime.viewer_protocol
   :members: ViewerProtocolStatus, ViewerControlResponseField, ViewerBatchMessageType, ViewerWireField, ViewerWirePayload, ViewerSourceSpatialWireField, ViewerComponentMode, ViewerComponentModeGroups, ViewerBatchMessageExtraPayload, ViewerSourceSpatialDomainPayload, ViewerBatchMessageWirePayload, viewer_wire_key, ViewerBatchDisplayPayload, viewer_component_mode_value, ViewerBatchItemPayload, ViewerComponentMetadataPayload, ViewerBatchMessagePayload, ViewerTransportEndpoint, ViewerControlReplyHeader, ViewerControlReplyPayload, ViewerAckResponsePayload, ViewerAckPolicy
   :member-order: bysource
   :show-inheritance:
