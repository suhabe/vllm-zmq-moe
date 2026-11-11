# ZMQ lessons for the API / engine / worker demo

Ten small, independent programs. Read and run them in order. Each file contains
all of its own code; there are no shared helpers hiding the ZMQ calls.

## Setup

Use Python 3.10+ on Linux or macOS. Lessons 3 and 10 use Unix IPC.
No CUDA, PyTorch, cuTile, NCCL, Modal, or GPUs are needed for these messaging lessons.

```bash
python -m pip install -r requirements.txt
```

Open the terminals in this directory and use the same Python environment in each.
For lessons 1–8, run the commands below in separate terminals, in the listed order.
All processes exit after receiving their expected number of messages. A receiver
waiting silently is normal: start its sender in another terminal. Run one copy of
each lesson at a time. TCP addresses listen only on the local machine.

## Learning path

| Lesson | New concept | Connection to the full demo |
| --- | --- | --- |
| 01_bytes.py | Context, socket, bind/connect, bytes | Basic process-to-process messaging |
| 02_json_many_senders.py | JSON, several senders, message IDs | Several engines return structured results |
| 03_ipc.py | Local IPC transport | The demo's ipc:// endpoints |
| 04_multipart.py | Several frames in one message | Routing envelope plus request payload |
| 05_router_dealer.py | Engine identity, routing, READY handshake | API selects a specific engine |
| 06_poller.py | Wait for input from several sockets | Engine watches connections from all APIs |
| 07_separate_results.py | Separate request/result channels | ROUTER/DEALER requests, PUSH/PULL results |
| 08_multiple_apis.py | Map input sockets to output sockets | replies[request] returns to the right API |
| 09_timeouts_shutdown.py | Receive timeout, linger, cleanup | Avoid waiting forever; understand shutdown |
| 10_process_tree.py | spawn, pipes, assembled messaging | Two APIs, two engines, one worker per engine |

## 1. Send one message of bytes

```bash
# Terminal 1
python 01_bytes.py receive
# Terminal 2
python 01_bytes.py send
```

The receiver prints `b'hello'`. `b` denotes bytes, the underlying message format.
The context creates sockets. One side binds an address; the other connects to it.
`PUSH` sends and `PULL` receives. You do not need an external message broker.

Try starting the sender first, then the receiver. Connection establishment happens
in the background. Sending queues a message; it does not prove the other program
has processed it. This example keeps the default shutdown behavior, which can
wait for queued outgoing messages to transfer if no receiver has connected yet.

## 2. Send JSON from two processes

```bash
# Terminal 1
python 02_json_many_senders.py receive
# Terminal 2
python 02_json_many_senders.py alice
# Terminal 3
python 02_json_many_senders.py bob
```

The receiver prints `alice [1, 2, 3]` and `bob [1, 2, 3]`; order may vary.
One receiving socket accepts messages from both senders. JSON serialization turns
a Python dictionary into bytes; `recv_json()` reconstructs it. The `id` field is
ordinary application data, not a ZMQ routing identity.

## 3. Change the transport to IPC

```bash
# Terminal 1
python 03_ipc.py receive
# Terminal 2
python 03_ipc.py send
```

The receiver prints `hello over IPC`. The pattern is unchanged; the address now
names a local Unix socket path instead of a TCP host and port. The endpoint path
is not a data file you read with `open()`. Both processes must use the same path.

## 4. Send a multipart message

```bash
# Terminal 1
python 04_multipart.py receive
# Terminal 2
python 04_multipart.py send
```

The receiver prints `[b'header', b'body']`. These are two frames in one message,
received together rather than as two independent messages. A frame is just one
byte sequence. The labels `header` and `body` have no special meaning to ZMQ.

## 5. Address an engine with ROUTER / DEALER

```bash
# Terminal 1
python 05_router_dealer.py api
# Terminal 2
python 05_router_dealer.py 0
# Terminal 3
python 05_router_dealer.py 1
```

The API waits for both engines and deliberately sends to engine 1 before engine 0.
Each engine prints its own rank in its received token list. Printed output across
processes can interleave; send order does not control terminal print order.

Watch the frame transformations:

| Sender call | Receiver sees |
| --- | --- |
| DEALER sends `b"READY"` | ROUTER receives `[engine_identity, b"READY"]` |
| ROUTER sends `[engine_identity, json_bytes]` | DEALER receives only `json_bytes` |
| DEALER sends `b"DONE"` | ROUTER receives `[engine_identity, b"DONE"]` |

The ROUTER prepends identity on receive and consumes identity on send. Identity
selects a connected engine; it does not identify an individual request. Engines
must have distinct identities at a given API's ROUTER. Set identity before connect.

`READY` and `DONE` are words chosen by this program, not built-in ZMQ commands.
This handshake establishes the engine's request route; it does not certify that a
separate GPU worker has initialized. The READY handshake also avoids guessing
connection readiness using sleeps. ROUTER/DEALER can send and receive repeatedly
without the strict send/receive alternation of REQ/REP.

## 6. Poll more than one socket

```bash
# Terminal 1
python 06_poller.py engine
# Terminal 2
python 06_poller.py 1
# Terminal 3
python 06_poller.py 0
```

Start both senders within ten seconds of starting the engine. The engine prints
both APIs' messages. Start API 1 before API 0: the engine can receive API 1's work
without waiting for API 0. If you delay the second sender, you can observe this.

A blocking receive on socket 0 would wait there even if socket 1 had a message.
`Poller` waits for any registered socket. `POLLIN` means a message is available.
Polling does not read the message: `recv_json()` does that afterward.

This lesson uses PULL sockets to isolate polling. The full demo applies the same
polling technique to DEALER sockets. Each socket in `sources` is a Python socket
object; the dictionary associates that connection with its API number.

## 7. Separate request and result channels

```bash
# Terminal 1
python 07_separate_results.py api
# Terminal 2
python 07_separate_results.py engine
```

The API prints two result dictionaries with IDs 100 and 101. It sends both requests
before receiving either result. Requests use ROUTER/DEALER; results use PUSH/PULL.
The engine echoes the tokens so the communication remains the only new idea.

Separate channels are a design choice, not a requirement of ROUTER/DEALER.
Lesson 5 returned replies on the same connection. Request IDs let an application
associate returned results with outstanding requests; ZMQ does not do that matching.

## 8. Return results to the API that submitted the request

```bash
# Terminal 1
python 08_multiple_apis.py 0
# Terminal 2
python 08_multiple_apis.py 1
# Terminal 3
python 08_multiple_apis.py engine
```

API 0 receives ID `api-0` with values `[0]`; API 1 receives ID `api-1` with values
`[1]`. The engine connects a separate DEALER and PUSH pair to each API.

Focus on these two lines:

```python
replies[request] = result
replies[request].send_json(...)
```

The dictionary maps the socket on which work arrived to the socket through which
its answer must leave. This is the full demo's return-routing logic. ZMQ does not
infer this mapping from the request ID. The same engine identity `b"0"` is valid
at two different ROUTERs: each API has its own set of peers.

Each PUSH socket connects to exactly one API. Connecting a single PUSH to both
APIs would distribute messages among them, rather than select the request's origin.

## 9. Understand a timeout and shutdown

```bash
python 09_timeouts_shutdown.py
```

It prints `No message arrived within 100 ms` and exits. There is deliberately no
sender. A receive timeout raises `zmq.Again`; your application decides what to do.

Default `recv()` waits indefinitely. `RCVTIMEO` limits an individual receive;
the poll timeout in lesson 6 instead limits the wait for readiness across sockets.

`close(linger=0)` discards any pending outgoing messages when closing. This PULL
socket has none, but the same option matters for sending sockets. It does not
acknowledge delivery or tell peers to exit. Close sockets before `ctx.term()`.
`ctx.destroy(linger=0)`, used in our original demo, closes all context sockets and
then terminates the context. Use it only when you own those sockets in this thread.
The earlier lessons use context managers and default linger so queued messages
are not deliberately discarded at normal shutdown.

## 10. Assemble the process tree

```bash
python 10_process_tree.py
```

This one command spawns two APIs and two engines; each engine spawns one worker.
Every API sends one batch to each engine, so there are four results in any
interleaving. The workers double the input values:

| Request ID | Input tokens | Returned values |
| --- | --- | --- |
| api-0/engine-0 | [0, 0] | [0, 0] |
| api-0/engine-1 | [0, 1] | [0, 2] |
| api-1/engine-0 | [1, 0] | [2, 0] |
| api-1/engine-1 | [1, 1] | [2, 2] |

All API/engine pairs are connected. For each API there is one bound ROUTER for
requests and one bound PULL for results. For each API, every engine creates one
connecting DEALER and one connecting PUSH. Each engine therefore polls two
request sockets and uses two corresponding result sockets.

Read `api_server()`, then `engine()`, then `worker()`, then the main block.
`spawn` starts fresh interpreters. Each child creates its own context and sockets;
we pass ranks and endpoint paths to children, not ZMQ socket objects. Sockets
should also stay with their owning thread.

The engine/worker link uses a Python Pipe. `None` is this program's worker-stop
message. These workers do ordinary Python arithmetic solely to keep the ZMQ lesson
independent of GPU setup. The original cuTile/NCCL demo still uses GPU workers.

## Mapping back to the MoE demo

| Original code | Lesson |
| --- | --- |
| `ctx = zmq.Context()` | 1 |
| `bind(...)`, `connect(...)`, `ipc://...` | 1, 3 |
| `send_json(...)`, `recv_json()` | 2 |
| `send_multipart(...)`, `recv_multipart()` | 4, 5 |
| `zmq.ROUTER`, `zmq.DEALER`, `zmq.IDENTITY` | 5 |
| Waiting until every engine has sent `READY` | 5, 10 |
| `poller.register(request, zmq.POLLIN)` | 6 |
| `dict(poller.poll(...))` | 6 |
| Engine PUSH / API PULL results | 2, 7 |
| `replies[request] = result` | 8 |
| `ctx.destroy(linger=0)` | 9 |
| `mp.Process`, `spawn`, engine/worker `Pipe` | 10 |

NCCL expert dispatch/combine and cuTile expert computation do not use ZMQ.
Their synchronization is a separate subject. These lessons demonstrate a finite
number of messages, rather than a production server's retries, admission control,
failure recovery, or endless request loop. ZMQ routes messages; it does not
coordinate the GPU workers' collective participation.

Two details to remember when extending the code: sends queue messages rather than
confirming execution, and queues have limits. PUSH/DEALER can block under pressure;
ROUTER can drop unroutable messages by default. Set ROUTER_MANDATORY if you want
unroutable sends to fail visibly. Neither READY nor a request ID supplies automatic
retries or exactly-once execution.

## References

- [ZeroMQ guide: sockets and patterns](https://zguide.zeromq.org/docs/chapter2/)
- [ZeroMQ guide: ROUTER / DEALER and identities](https://zguide.zeromq.org/docs/chapter3/)
- [PyZMQ API](https://pyzmq.readthedocs.io/en/latest/api/zmq.html)


# Complete source, in lesson order

## 01_bytes.py

```python
"""One message: Context, PUSH/PULL, bind/connect, send/recv.

The receiver binds an address, and the sender connects to that same address.
PUSH sends complete messages of bytes; PULL receives them one at a time.
Sending queues a message, while recv() waits until a message is available.

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
```

## 02_json_many_senders.py

```python
"""Two senders share one receiver; JSON and application request IDs.

send_json() converts a dictionary to bytes, and recv_json() reconstructs it.
Both senders connect to one PULL socket, so their messages arrive in one place.
The id field identifies each message in our application; it does not route it.

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
```

## 03_ipc.py

```python
"""The same messaging pattern over a local IPC endpoint (Linux/macOS).

An ipc:// address names a Unix socket path shared by processes on one machine.
Both sides must use the same path, just as TCP peers must use the same port.
Changing the transport leaves the PUSH/PULL messaging pattern unchanged.

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
```

## 04_multipart.py

```python
"""Several byte frames form ONE message; no routing involved yet.

A frame is one byte sequence, and a multipart message groups several frames.
recv_multipart() returns all frames of one message together as a list.
Our API will later use this format to pair an engine address with a payload.

Terminal 1: python 04_multipart.py receive
Terminal 2: python 04_multipart.py send
"""
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "receive":
        with ctx.socket(zmq.PULL) as socket:
            socket.bind("tcp://127.0.0.1:5504")
            frames = socket.recv_multipart()
            print(frames)  # [b'header', b'body']: one message, two frames.
    elif role == "send":
        with ctx.socket(zmq.PUSH) as socket:
            socket.connect("tcp://127.0.0.1:5504")
            socket.send_multipart([b"header", b"body"])
```

## 05_router_dealer.py

```python
"""Address engines explicitly; observe ROUTER adding/removing identity.

Each DEALER names itself so the API's ROUTER can select a particular engine.
ROUTER adds that identity when receiving and removes it when sending a payload.
The API waits for our READY messages before sending work to both engines.

Terminal 1: python 05_router_dealer.py api
Terminal 2: python 05_router_dealer.py 0
Terminal 3: python 05_router_dealer.py 1
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "api":
        with ctx.socket(zmq.ROUTER) as socket:
            socket.bind("tcp://127.0.0.1:5505")
            for _ in range(2):
                identity, body = socket.recv_multipart()
                assert body == b"READY"
                print("ready:", identity)
            for rank in (1, 0):  # Explicit routing, not arrival order.
                payload = json.dumps({"tokens": [rank, 10]}).encode()
                socket.send_multipart([str(rank).encode(), payload])
            for _ in range(2):
                print("reply:", socket.recv_multipart())
    else:
        with ctx.socket(zmq.DEALER) as socket:
            socket.setsockopt(zmq.IDENTITY, role.encode())
            socket.connect("tcp://127.0.0.1:5505")
            socket.send(b"READY")  # Our protocol, not a ZMQ keyword.
            print("engine", role, "received:", socket.recv_json())
            socket.send(b"DONE")
```

## 06_poller.py

```python
"""Wait for either of two sockets, rather than blocking on the wrong one.

The engine has one receiving socket for each API and watches both with Poller.
POLLIN means a socket has a message ready; recv_json() then reads that message.
This lets one thread handle whichever API sends first without waiting on the other.

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
```

## 07_separate_results.py

```python
"""Requests use ROUTER/DEALER; results use a separate PUSH/PULL channel.

The API sends two requests before waiting for results on its separate PULL socket.
The engine copies each request's id into its result so the API can match them.
Separate channels are our design choice; ROUTER/DEALER can also carry replies.

Terminal 1: python 07_separate_results.py api
Terminal 2: python 07_separate_results.py engine
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "api":
        with ctx.socket(zmq.ROUTER) as requests, ctx.socket(zmq.PULL) as results:
            requests.bind("tcp://127.0.0.1:5508")
            results.bind("tcp://127.0.0.1:5509")
            identity, ready = requests.recv_multipart()
            assert ready == b"READY"
            # Two outstanding requests; IDs correlate results with requests.
            for request_id in (100, 101):
                body = json.dumps({"id": request_id, "tokens": [1, 2]}).encode()
                requests.send_multipart([identity, body])
            for _ in range(2):
                print(results.recv_json())
    elif role == "engine":
        with ctx.socket(zmq.DEALER) as requests, ctx.socket(zmq.PUSH) as results:
            requests.setsockopt(zmq.IDENTITY, b"0")
            requests.connect("tcp://127.0.0.1:5508")
            results.connect("tcp://127.0.0.1:5509")
            requests.send(b"READY")
            for _ in range(2):
                batch = requests.recv_json()
                results.send_json({"id": batch["id"], "values": batch["tokens"]})
```

## 08_multiple_apis.py

```python
"""One engine, two APIs: replies[request_socket] selects the return path.

The engine creates a separate request socket and result socket for each API.
Its replies dictionary maps the socket receiving work to the correct return socket.
ZMQ does not infer this relationship from the request id; our code chooses it.

Terminal 1: python 08_multiple_apis.py 0
Terminal 2: python 08_multiple_apis.py 1
Terminal 3: python 08_multiple_apis.py engine
"""
import json
import sys
import zmq

role = sys.argv[1]
with zmq.Context() as ctx:
    if role == "engine":
        poller, replies = zmq.Poller(), {}
        for api in range(2):
            request = ctx.socket(zmq.DEALER)
            request.setsockopt(zmq.IDENTITY, b"0")
            request.connect(f"tcp://127.0.0.1:{5510 + 2 * api}")
            result = ctx.socket(zmq.PUSH)
            result.connect(f"tcp://127.0.0.1:{5511 + 2 * api}")
            poller.register(request, zmq.POLLIN)
            replies[request] = result
            request.send(b"READY")
        received = 0
        while received < 2:
            for request in dict(poller.poll()):
                batch = request.recv_json()
                replies[request].send_json({"id": batch["id"], "values": batch["tokens"]})
                received += 1
        for request, result in replies.items():
            request.close()
            result.close()
    else:
        api = int(role)
        with ctx.socket(zmq.ROUTER) as request, ctx.socket(zmq.PULL) as result:
            request.bind(f"tcp://127.0.0.1:{5510 + 2 * api}")
            result.bind(f"tcp://127.0.0.1:{5511 + 2 * api}")
            identity, ready = request.recv_multipart()
            assert ready == b"READY"
            body = json.dumps({"id": f"api-{api}", "tokens": [api]}).encode()
            request.send_multipart([identity, body])
            print("API", api, "received:", result.recv_json())
```

## 09_timeouts_shutdown.py

```python
"""A receive timeout and explicit cleanup; runs in one terminal.

No sender connects, so recv() raises zmq.Again after the configured timeout.
The finally block closes the socket before terminating its context.
Using linger=0 allows close to discard queued outgoing messages rather than wait.

Run: python 09_timeouts_shutdown.py
"""
import zmq

ctx = zmq.Context()
socket = ctx.socket(zmq.PULL)
socket.setsockopt(zmq.RCVTIMEO, 100)  # Receive waits at most 100 ms.
socket.bind("tcp://127.0.0.1:5514")

try:
    socket.recv()  # Nobody sends, so this deliberately times out.
except zmq.Again:
    print("No message arrived within 100 ms")
finally:
    socket.close(linger=0)  # Discard any pending outgoing messages on close.
    ctx.term()

# Equivalent cleanup for all sockets owned by this thread:
# ctx.destroy(linger=0)
```

## 10_process_tree.py

```python
"""Assemble the messaging: two APIs, two engines, one worker per engine.

Each API can address either engine, and each engine polls requests from both APIs.
Each engine passes work to its own worker through a Python Pipe, then returns results.
The spawn method starts fresh processes that create their own ZMQ contexts and sockets.

Run: python 10_process_tree.py
Workers double numbers with Python so this ZMQ lesson needs no GPUs.
"""
import json
import multiprocessing as mp
import tempfile
import zmq

APIS = ENGINES = 2


def address(root, channel, api):
    return f"ipc://{root}/{channel}-{api}"


def worker(pipe):
    while (tokens := pipe.recv()) is not None:
        pipe.send([2 * token for token in tokens])
    pipe.close()


def engine(rank, root):
    parent, child = mp.Pipe()
    process = mp.Process(target=worker, args=(child,))
    process.start()
    child.close()

    with zmq.Context() as ctx:
        poller, replies = zmq.Poller(), {}
        for api in range(APIS):
            request = ctx.socket(zmq.DEALER)
            request.setsockopt(zmq.IDENTITY, str(rank).encode())
            request.connect(address(root, "requests", api))
            result = ctx.socket(zmq.PUSH)
            result.connect(address(root, "results", api))
            poller.register(request, zmq.POLLIN)
            replies[request] = result
            request.send(b"READY")

        # Each API sends exactly one request to each engine.
        received = 0
        while received < APIS:
            for request in dict(poller.poll()):
                batch = request.recv_json()
                parent.send(batch["tokens"])
                values = parent.recv()
                replies[request].send_json({
                    "id": batch["id"], "engine": rank, "values": values,
                })
                received += 1
        for request, result in replies.items():
            request.close()
            result.close()

    parent.send(None)
    parent.close()
    process.join()
    if process.exitcode != 0:
        raise RuntimeError("Worker failed")


def api_server(api, root):
    with zmq.Context() as ctx:
        with ctx.socket(zmq.ROUTER) as requests, ctx.socket(zmq.PULL) as results:
            requests.bind(address(root, "requests", api))
            results.bind(address(root, "results", api))
            ready = set()
            while len(ready) < ENGINES:
                identity, message = requests.recv_multipart()
                assert message == b"READY"
                ready.add(identity)

            for rank in range(ENGINES):
                batch = {"id": f"api-{api}/engine-{rank}", "tokens": [api, rank]}
                requests.send_multipart([
                    str(rank).encode(), json.dumps(batch).encode(),
                ])
            for _ in range(ENGINES):
                print("API", api, "received:", results.recv_json(), flush=True)


if __name__ == "__main__":
    mp.set_start_method("spawn")  # Each child creates its own ZMQ sockets.
    with tempfile.TemporaryDirectory(prefix="zmq-lesson-") as root:
        processes = (
            [mp.Process(target=api_server, args=(api, root)) for api in range(APIS)]
            + [mp.Process(target=engine, args=(rank, root)) for rank in range(ENGINES)]
        )
        try:
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=20)
                if process.exitcode != 0:
                    raise RuntimeError(f"Process failed or timed out: {process.pid}")
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join()
```
