"""Several byte frames form ONE message; no routing involved yet.

Terminal 1: python 04_multipart.py receive
Terminal 2: python 04_multipart.py send
"""
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "receive":
        with ctx.socket(zmq.PULL) as socket:
            socket.bind("tcp://127.0.0.1:5504")
            frames = socket.recv_multipart()
            print(frames)  # [b'header', b'body']: one message, two frames.
    elif role == "send":
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect("tcp://127.0.0.1:5504")
            socket.send_multipart([b"header", b"body"])
