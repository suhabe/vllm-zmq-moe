"""JSON messages from several senders.

ZMQ concepts
------------
ZeroMQ transports bytes. PyZMQ's send_json() serializes JSON-compatible values;
recv_json() reconstructs them. Several PUSH peers can send to one PULL socket.
An id inside a JSON payload is application data, not a ZMQ routing identity;
ZeroMQ does not interpret it or impose a fixed order between independent senders.

This example
------------
Senders alice and bob connect to one receiver. Each sends a dictionary with its
name as id and [1, 2, 3] as tokens. The receiver prints both names and token lists;
the order may vary.

Run in separate terminals:
Terminal 1: python 02_json_many_senders.py receive
Terminal 2: python 02_json_many_senders.py alice
Terminal 3: python 02_json_many_senders.py bob
"""
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "receive":
        with ctx.socket(zmq.PULL) as socket:
            socket.bind("tcp://127.0.0.1:5502")
            for _ in range(2):
                message = socket.recv_json()
                print(message["id"], message["tokens"])
    else:
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect("tcp://127.0.0.1:5502")
            socket.send_json({"id": role, "tokens": [1, 2, 3]})
