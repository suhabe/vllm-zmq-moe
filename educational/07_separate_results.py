"""Requests use ROUTER/DEALER; results use a separate PUSH/PULL channel.

The API sends two requests before waiting for results on its separate PULL socket.
The engine copies each request's id into its result so the API can match them.
Separate channels are our design choice; ROUTER/DEALER can also carry replies.

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
