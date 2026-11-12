"""Send each result back to the right API.

ZMQ concepts
------------
When work arrives from several programs, each result must go back to the one
that submitted it. With separate work and result connections, your code must
remember which connections belong together.

A dictionary can pair each receiving socket with the socket used to return its
results. After reading work, look up the return socket and send there. A request
ID labels the work; it does not tell ZeroMQ which connection to use.

This example
------------
The engine has one DEALER for receiving work and one PUSH for returning results
to each API. It records each pair with replies[request] = result. When work
arrives on request, replies[request].send_json(...) returns the answer to that
API.

Each PUSH connects to only one API. A single PUSH connected to both APIs would
spread results between them without regard to who submitted the work.

API 0 receives ID api-0 with values [0]; API 1 receives ID api-1 with values
[1].

Both DEALERs use identity b"0". That works because they connect to different
ROUTERs: each ROUTER only needs to distinguish its own connected programs.

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
