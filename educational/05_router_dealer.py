"""Address engines explicitly; observe ROUTER adding/removing identity.

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
