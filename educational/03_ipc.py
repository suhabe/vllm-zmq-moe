"""Use a local socket path.

ZMQ concepts
------------
ZeroMQ can carry the same messages over different kinds of connections.
A tcp:// address uses a host and port and can connect programs across machines.
An ipc:// address uses a Unix socket path to connect programs on one machine.
IPC means interprocess communication.

Changing the connection type does not change how PUSH and PULL work. Both
programs still need to use the same address. An IPC path names a communication
socket, not a file whose contents you read with open().

This example
------------
The programs use ipc:///tmp/zmq-lesson-03.sock instead of a TCP address.
send_string() and recv_string() handle converting between Python text and
the bytes carried by ZeroMQ. Run this lesson on Linux or macOS.

The receiver prints hello over IPC.

Use IPC when the programs will stay on one machine; the socket directory must be
accessible to them. Use TCP when they need to communicate across machines.

Run in separate terminals:
Terminal 1: python 03_ipc.py receive
Terminal 2: python 03_ipc.py send
"""
import sys
import zmq

role = sys.argv[1]
endpoint = "ipc:///tmp/zmq-lesson-03.sock"

with zmq.Context() as ctx:
    if role == "receive":
        with ctx.socket(zmq.PULL) as socket:
            socket.bind(endpoint)
            print(socket.recv_string())
    elif role == "send":
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect(endpoint)
            socket.send_string("hello over IPC")
