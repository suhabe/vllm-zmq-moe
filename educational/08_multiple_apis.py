"""Choose the return path for each API.

ZMQ concepts
------------
Separate request/result sockets need an application-defined relationship. A map
from incoming socket to outgoing socket can choose the return path; ZeroMQ does
not infer it from a request ID. One PUSH connected to several APIs distributes
messages among them. A dedicated PUSH per API permits explicit path selection.
Routing identities belong to each ROUTER's own set of peers.

This example
------------
The engine connects one DEALER/PUSH pair per API and polls both DEALERs. Both use
identity b"0", each at a different ROUTER. replies[request] maps the socket that
received work to the correct result socket. API 0 receives id api-0 with values
[0]; API 1 receives id api-1 with values [1]. The mapping chooses the destination.

Run in separate terminals:
Terminal 1: python 08_multiple_apis.py 0
Terminal 2: python 08_multiple_apis.py 1
Terminal 3: python 08_multiple_apis.py engine
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "engine":
        poller, replies = zmq.Poller(), {}
        for api in range(2):
            request = ctx.socket(zmq.DEALER)
            request.setsockopt(zmq.IDENTITY, b"0")
            request.connect(f"tcp://127.0.0.1:{5510 + 2 * api}")
            result = ctx.socket(zmq.PUSH)
            result.connect(f"tcp://127.0.0.1:{5511 + 2 * api}")
            poller.register(request, zmq.POLLIN)
            replies[request] = result
            request.send(b"READY")
        received = 0
        while received < 2:
            for request in dict(poller.poll()):
                batch = request.recv_json()
                replies[request].send_json({"id": batch["id"], "values": batch["tokens"]})
                received += 1
        for request, result in replies.items():
            request.close()
            result.close()
    else:
        api = int(role)
        with ctx.socket(zmq.ROUTER) as request, ctx.socket(zmq.PULL) as result:
            request.bind(f"tcp://127.0.0.1:{5510 + 2 * api}")
            result.bind(f"tcp://127.0.0.1:{5511 + 2 * api}")
            identity, ready = request.recv_multipart()
            assert ready == b"READY"
            body = json.dumps({"id": f"api-{api}", "tokens": [api]}).encode()
            request.send_multipart([identity, body])
            print("API", api, "received:", result.recv_json())
