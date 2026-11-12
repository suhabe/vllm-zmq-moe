"""Separate channels for requests and results.

ZMQ concepts
------------
Applications can send requests on ROUTER/DEALER and results on PUSH/PULL. Separate
channels are a design choice; ROUTER/DEALER can also carry replies. Request IDs
let application code associate results with outstanding requests. ZeroMQ neither
performs that matching nor requires a result before another request is sent.

This example
------------
The API binds a ROUTER and a PULL; the engine connects a DEALER and a PUSH.
After READY, the API sends requests 100 and 101 before receiving either result.
The engine echoes each request's tokens as values and copies its id into the
result. The API prints both dictionaries; it does not implement an ID lookup.

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
