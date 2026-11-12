"""Keep several parts in one message.

ZMQ concepts
------------
Sometimes you want to send a label and some data together while keeping them
separate. ZeroMQ calls each part a frame. Each frame contains bytes.

send_multipart() sends a list of frames as one message. recv_multipart()
returns that message's frames together, in the same order. The receiver does not
have to guess where one part ends and the next begins.

This example
------------
The sender sends [b"header", b"body"] through PUSH. The receiver reads both
parts through PULL in one call. These words are just data chosen for this lesson;
they have no special meaning to ZeroMQ.

The receiver prints [b'header', b'body']: one message with two parts.
The next lesson uses the first part to say who should receive a message.

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

