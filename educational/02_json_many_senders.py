"""Two senders share one receiver; JSON and application request IDs.

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
