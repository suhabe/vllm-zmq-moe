"""One message: Context, PUSH/PULL, bind/connect, send/recv.

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
