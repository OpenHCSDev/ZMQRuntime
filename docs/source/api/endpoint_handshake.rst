Endpoint handshake
==================

Every successful client connection retains the ``PongResponse`` that proved
the endpoint was ready. Read it through ``ZMQClient.connected_endpoint`` rather
than issuing a second probe or copying endpoint metadata into application
state.

Applications can identify their endpoint by passing an
``EndpointApplication`` to ``ZMQServer`` or ``ExecutionServer``. The identity
is optional so clients remain compatible with endpoints that predate the
field.

.. code-block:: python

   from zmqruntime import EndpointApplication
   from zmqruntime.execution import ExecutionServer

   application = EndpointApplication(
       identifier="example-application",
       version="1.4.0",
   )
   server = ConcreteExecutionServer(application=application)

After connecting, the application layer can compare its own declaration with
the authoritative handshake:

.. code-block:: python

   if client.connect():
       observed = client.connected_endpoint.application

``wait_for_endpoint_ready`` exposes the same typed response to process owners
that need readiness metadata. ``wait_for_server_ready`` remains the Boolean
compatibility boundary for callers that only need readiness.
