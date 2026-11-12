"""Choose which program receives work.

ZMQ concepts
------------
When one program talks to several others, it needs a way to tell them apart.
A ROUTER socket gives your code that choice. Each connected program has a
name, called its identity. Received messages include the sender's identity.
To send a message, your code supplies the recipient's identity and the message.

A DEALER socket can connect to the ROUTER and exchange messages with it. With
just that one connection, the DEALER sends there without naming a recipient.
Both socket types let your code send more messages before receiving a reply.

Your programs can agree to send READY when they connect. Once the ROUTER
receives it, your code knows that sender's identity and can address work to it.
READY is a word your programs agree on; ZeroMQ gives it no special meaning.

This example
------------
The API uses a ROUTER. Two engines use DEALERs and set their identities to
b"0" and b"1" before connecting. Each sends READY. After hearing from both,
the API sends work to engine 1, then engine 0. Each engine prints its work and
sends back DONE. An engine is a separate program that handles work.

Each engine receives a token list containing its own number. Printed order may
vary. The ROUTER adds identity as the first frame when receiving and removes it
when sending, so each engine receives only its work. Each engine needs a distinct
identity. See README.md for the exact message parts.

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
