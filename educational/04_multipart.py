"""Several byte frames in one message.

ZMQ concepts
------------
A frame is one byte sequence. A multipart message groups frames in order as one
message; recv_multipart() returns its frames together as a list. The application
or socket pattern assigns their meaning; arbitrary labels do not cause routing.

This example
------------
The PUSH sender sends [b"header", b"body"] in one call. The PULL receiver prints
[b'header', b'body']: one message with two frames. These labels are just data.
Later lessons use multipart messages for a routing identity plus a payload.

Run in separate terminals:
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

