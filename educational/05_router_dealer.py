"""
The ZeroMQ concept

A ZeroMQ socket’s type determines how it handles messages. ROUTER and DEALER are two socket types; both can send and receive messages.

A ROUTER lets your code address messages to specific connected peers:

When receiving, it adds the sender’s identity before the message body, so your code knows who sent it.

When sending, your code supplies the recipient’s identity before the message body. The ROUTER uses that identity to choose the connection, then sends only the body.

A DEALER lets your code send and receive messages without specifying a recipient for each message. If it has one connection, messages go there. If it has several connections, ZeroMQ distributes outgoing messages among them.

This example

Run one API process and two worker processes, called engines:

The API uses a ROUTER socket.

Each engine uses a DEALER socket connected to the API and sets its identity to "0" or "1".

Each engine sends READY. The API receives the engine’s identity alongside that message—for example, [b"0", b"READY"].

After both engines are ready, the API sends work to engine "1", then engine "0". It selects each engine by identity, regardless of which became ready first. Each engine receives only its work payload and replies with DONE.

READY and DONE are messages defined by this example, not special ZeroMQ commands.

Run each command in a separate terminal:

Terminal 1: python 05_router_dealer.py api
Terminal 2: python 05_router_dealer.py 0
Terminal 3: python 05_router_dealer.py 1
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "api":
        with ctx.socket(zmq.ROUTER) as socket:
            socket.bind("tcp://127.0.0.1:5505")
            for _ in range(2):
                identity, body = socket.recv_multipart()
                assert body == b"READY"
                print("ready:", identity)
            for rank in (1, 0):  # Explicit routing, not arrival order.
                payload = json.dumps({"tokens": [rank, 10]}).encode()
                socket.send_multipart([str(rank).encode(), payload])
            for _ in range(2):
                print("reply:", socket.recv_multipart())
    else:
        with ctx.socket(zmq.DEALER) as socket:
            socket.setsockopt(zmq.IDENTITY, role.encode())
            socket.connect("tcp://127.0.0.1:5505")
            socket.send(b"READY")  # Our protocol, not a ZMQ keyword.
            print("engine", role, "received:", socket.recv_json())
            socket.send(b"DONE")
