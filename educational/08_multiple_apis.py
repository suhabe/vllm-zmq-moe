"""One engine, two APIs: replies[request_socket] selects the return path.

The engine creates a separate request socket and result socket for each API.
Its replies dictionary maps the socket receiving work to the correct return socket.
ZMQ does not infer this relationship from the request id; our code chooses it.

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
