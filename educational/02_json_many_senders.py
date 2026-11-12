"""Send a dictionary from two programs.

ZMQ concepts
------------
Messages often contain several pieces of information. JSON is a text format
that can represent dictionaries, lists, numbers, and strings. send_json() turns
such a value into a message; recv_json() turns it back into a Python value.

One PULL socket can receive from several PUSH sockets. Each message arrives
whole, but you should not rely on which sender's message arrives first.

A field named id can help your code identify a message. You choose its meaning;
ZeroMQ does not use JSON fields to decide where messages go.

This example
------------
Two senders, alice and bob, connect to one receiver. Each sends a dictionary
containing its name as id and [1, 2, 3] as tokens. Here, tokens is simply
the name we give a list of numbers.

The receiver prints alice [1, 2, 3] and bob [1, 2, 3]. Their order may vary.

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
