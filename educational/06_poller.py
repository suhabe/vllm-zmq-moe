"""Read whichever socket has a message.

ZMQ concepts
------------
Calling recv() on one socket makes your program wait there, even if another
socket already has a message. A Poller solves this by watching several sockets
at once and telling your code which ones have something to read.

Registering a socket with POLLIN means "watch this socket for incoming
messages." poll() waits and reports the ready sockets. Your code then calls a
receive method to read the messages. A timeout lets it stop waiting if nothing
arrives.

This example
------------
The engine watches two PULL sockets, one for each API. Its sources dictionary
records which API belongs to each socket. Each poll waits up to 10,000
milliseconds, or ten seconds.

Start both senders within ten seconds. The engine prints each API's number and
message. Start API 1 first and briefly delay API 0: API 1's message can be read
without waiting for API 0. If a poll times out, this program raises TimeoutError.

Run in separate terminals:
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
