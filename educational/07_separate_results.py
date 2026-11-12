"""Send work and collect results separately.

ZMQ concepts
------------
Requests and results can share one connection, as in lesson 5, or use separate
connections. Both designs let your code send several requests before reading
results.

To tell which result belongs to which request, put a request ID in the message
and copy it into the result. Your code is responsible for using that ID to match
them. ZeroMQ simply carries the messages.

This example
------------
The API sends work through ROUTER to the engine's DEALER. The engine returns
results through PUSH to the API's PULL.

After the engine sends READY, the API sends requests 100 and 101 before reading
either result. The engine copies each request's tokens into values and keeps
the request's id. It does no computation yet.

The API prints two result dictionaries: IDs 100 and 101, each with values
[1, 2]. The copied IDs make matching possible; this program only prints them.

Run in separate terminals:
Terminal 1: python 07_separate_results.py api
Terminal 2: python 07_separate_results.py engine
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "api":
        with ctx.socket(zmq.ROUTER) as requests, ctx.socket(zmq.PULL) as results:
            requests.bind("tcp://127.0.0.1:5508")
            results.bind("tcp://127.0.0.1:5509")
            identity, ready = requests.recv_multipart()
            assert ready == b"READY"
            # Two outstanding requests; IDs correlate results with requests.
            for request_id in (100, 101):
                body = json.dumps({"id": request_id, "tokens": [1, 2]}).encode()
                requests.send_multipart([identity, body])
            for _ in range(2):
                print(results.recv_json())
    elif role == "engine":
        with ctx.socket(zmq.DEALER) as requests, ctx.socket(zmq.PUSH) as results:
            requests.setsockopt(zmq.IDENTITY, b"0")
            requests.connect("tcp://127.0.0.1:5508")
            results.connect("tcp://127.0.0.1:5509")
            requests.send(b"READY")
            for _ in range(2):
                batch = requests.recv_json()
                results.send_json({"id": batch["id"], "values": batch["tokens"]})
