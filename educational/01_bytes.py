"""Send one message.

ZMQ concepts
------------
To exchange messages, each program creates a socket: an object with send or
receive methods. A PUSH socket sends; a PULL socket receives. A context is
the ZeroMQ object used to create these sockets.

The programs need an address where they can meet. One calls bind() to listen
there; the other calls connect() to reach it.

send() sends bytes, Python's representation of raw data. recv() waits for and
returns one complete message. Sending puts data in a queue; it does not mean the
other program has read it yet.

This example
------------
The receiver listens at tcp://127.0.0.1:5501 using PULL. The sender connects
using PUSH and sends b"hello". This address keeps communication on your machine.

The receiver prints b'hello'. The b means the value is bytes.

You can also start the sender first. ZeroMQ connects in the background. With the
default settings, the sender may wait during shutdown until a receiver connects;
lesson 9 explains how to control that wait.

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
