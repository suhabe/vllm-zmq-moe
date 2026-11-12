"""Address specific peers with ROUTER / DEALER.

ZMQ concepts
------------
ROUTER and DEALER both send and receive without strict send/receive alternation.
A ROUTER prepends the sender's routing identity on receive. On send, it consumes
the first frame as the destination identity and forwards the remaining frames.
An identity selects a peer connection, not an individual request. Peers need
distinct identities at a given ROUTER; set an explicit identity before connect().

A DEALER sends without a destination frame. With one connection, messages go
there; with several, it distributes outgoing messages among available peers.
When connected directly to a ROUTER it receives only the payload frames.
A readiness handshake is an application protocol for learning a peer's route;
READY and DONE are ordinary payloads, not built-in ZeroMQ commands.

This example
------------
The API binds a ROUTER. Two engines connect DEALERs with identities b"0" and b"1"
and send READY. The API receives [identity, b"READY"] from each, then addresses
work to engine 1 before engine 0 regardless of readiness order. Each engine
receives its JSON payload and replies DONE. Terminal print order may vary.
The handshake establishes request routes; it does not certify GPU worker setup.

Run in separate terminals:
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
