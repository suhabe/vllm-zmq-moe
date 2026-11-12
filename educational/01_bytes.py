"""One message of bytes.

ZMQ concepts
------------
A Context manages messaging resources and creates sockets. Socket types define
behavior: PUSH sends complete byte messages; PULL receives them. bind() listens
at an endpoint and connect() connects to it, independently of who sends.
Connection setup happens in the background. send() queues data without confirming
application processing; recv() waits for one complete message.

This example
------------
The receiver binds a PULL socket at tcp://127.0.0.1:5501. The sender connects a
PUSH socket and sends b"hello". The receiver prints b'hello'. Default linger can
keep shutdown waiting for queued data if the receiver has not connected yet.

Run in separate terminals:
Terminal 1: python 01_bytes.py receive
Terminal 2: python 01_bytes.py send
"""
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "receive":
        with ctx.socket(zmq.PULL) as socket:
            socket.bind("tcp://127.0.0.1:5501")
            print(socket.recv())  # b'hello': a complete message of bytes.
    elif role == "send":
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect("tcp://127.0.0.1:5501")
            socket.send(b"hello")
