"""Wait for either of two sockets, rather than blocking on the wrong one.

Terminal 1: python 06_poller.py engine
Terminal 2: python 06_poller.py 1
Terminal 3: python 06_poller.py 0
"""
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "engine":
        poller = zmq.Poller()
        sources = {}
        for api in range(2):
            socket = ctx.socket(zmq.PULL)
            socket.bind(f"tcp://127.0.0.1:{5506 + api}")
            poller.register(socket, zmq.POLLIN)
            sources[socket] = api
        received = 0
        while received < 2:
            events = dict(poller.poll(10_000))  # Milliseconds.
            if not events:
                raise TimeoutError("Start both senders within 10 seconds")
            for socket in events:
                print("from API", sources[socket], ":", socket.recv_json())
                received += 1
        for socket in sources:
            socket.close()
    else:
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect(f"tcp://127.0.0.1:{5506 + int(role)}")
            socket.send_json({"id": role, "tokens": [int(role)]})
