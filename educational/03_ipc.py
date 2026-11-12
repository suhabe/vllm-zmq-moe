"""Local IPC transport (Linux/macOS).

ZMQ concepts
------------
The transport is separate from the socket's messaging pattern. An ipc:// address
names a Unix socket path shared by processes on one machine; tcp:// uses a host
and port and also works across machines. Both peers must use the same endpoint.
The IPC path is a socket endpoint, not a data file to read with open().

Choose IPC for processes that stay on one host, accounting for directory access
and OS path-length limits. Choose TCP for distributed services or a consistent
addressing model between local development and deployment. TCP can use loopback
(127.0.0.1); binding to * listens on all available interfaces.

This example
------------
The receiver binds ipc:///tmp/zmq-lesson-03.sock and the sender connects there.
PUSH/PULL works as before; string helpers encode and decode the payload. The
receiver prints hello over IPC.

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
