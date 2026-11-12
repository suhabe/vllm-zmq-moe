"""The same messaging pattern over a local IPC endpoint (Linux/macOS).

An ipc:// address names a Unix socket path shared by processes on one machine.
Both sides must use the same path, just as TCP peers must use the same port.
Changing the transport leaves the PUSH/PULL messaging pattern unchanged.

Terminal 1: python 03_ipc.py receive
Terminal 2: python 03_ipc.py send

When to choose IPC vs TCP:
- Choose IPC for a local worker pool or daemon where all processes will stay
on one host. Account for socket-directory access and OS path-length limits.
- Choose TCP for distributed services, or when keeping the same addressing
model between local development and deployment is useful. TCP also works
locally; bind to 127.0.0.1 for loopback, while * binds all available interfaces.
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
