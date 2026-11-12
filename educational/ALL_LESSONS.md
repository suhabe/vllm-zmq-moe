# ZMQ lessons for the API / engine / worker demo

Ten small, independent programs. Read and run them in order. Each file contains
all of its own code; there are no shared helpers hiding the ZMQ calls.

Each lesson starts with **ZMQ concepts**, explained independently of the program.
Then **This example** shows how the program applies those concepts, how to run it,
and what to look for. The same split appears in each script's opening docstring.

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

### ZMQ concepts

A **context** manages ZeroMQ's messaging resources and creates **sockets**. A
socket's type determines its messaging behavior: `PUSH` sends messages and `PULL`
receives them. These peers communicate directly without an external broker.

An **endpoint** is an address. `bind()` makes a socket listen at an endpoint;
`connect()` asks a socket to connect to it. These roles do not determine which
side sends: the socket type does that. Connection establishment happens in the
background.

ZeroMQ preserves message boundaries. `send()` accepts bytes, and `recv()` returns
one complete message, waiting if none is available. A successful send queues data;
it does not confirm that the receiving application has processed it.

### This example

The receiver creates a PULL socket and binds `tcp://127.0.0.1:5501`. The sender
creates a PUSH socket, connects to that address, and sends the bytes `b"hello"`.

```bash
# Terminal 1
python 01_bytes.py receive
# Terminal 2
python 01_bytes.py send
```

The receiver prints `b'hello'`; the `b` marks a Python bytes value.

Try starting the sender first, then the receiver. This example keeps the default
shutdown behavior, which can wait for queued outgoing messages to transfer if no
receiver has connected yet. Lesson 9 explains that shutdown behavior.

## 2. Send JSON from two processes

### ZMQ concepts

ZeroMQ transports bytes. PyZMQ's `send_json()` serializes a JSON-compatible Python
value into bytes; `recv_json()` decodes the received message back into a Python
value. The fields inside that value are defined by the application.

Several PUSH peers can connect to one PULL socket. That socket receives their
messages through one receive interface; independent senders do not establish a
single predictable order between their messages.

An application can include an `id` field to label a message or correlate a result
with a request. ZeroMQ does not interpret that field or use it to select a peer.

### This example

One receiver binds a PULL socket. Two senders, `alice` and `bob`, each send a
dictionary containing their name as `id` and `[1, 2, 3]` as `tokens`.

```bash
# Terminal 1
python 02_json_many_senders.py receive
# Terminal 2
python 02_json_many_senders.py alice
# Terminal 3
python 02_json_many_senders.py bob
```

The receiver reads two messages and prints `alice [1, 2, 3]` and
`bob [1, 2, 3]`; order may vary. These names are payload data. Lesson 5 introduces
a separate kind of identity used for routing connections.

## 3. Change the transport to IPC

### ZMQ concepts

A **transport** carries messages between sockets. The messaging pattern, such as
PUSH/PULL, is separate from the transport used to carry those messages.

A `tcp://` endpoint uses a host and port. TCP works both across machines and
locally; `127.0.0.1` limits a listener to loopback, while `*` binds all available
interfaces. An `ipc://` endpoint names a Unix socket path for communication
between processes on one machine. That path is a socket endpoint, not a data file
to read with `open()`. Both peers must use the same endpoint.

Choose IPC for processes that will stay on one host, accounting for directory
permissions and OS path-length limits. Choose TCP for distributed services or to
keep the same addressing model between local development and deployment.

### This example

The sender and receiver still use PUSH/PULL. They replace the TCP address from
lesson 1 with `ipc:///tmp/zmq-lesson-03.sock` and send a string using PyZMQ's
`send_string()` / `recv_string()` helpers.

```bash
# Terminal 1
python 03_ipc.py receive
# Terminal 2
python 03_ipc.py send
```

The receiver prints `hello over IPC`. Run this lesson on Linux or macOS. The
messaging pattern stays the same even though the endpoint and transport change.

## 4. Send a multipart message

### ZMQ concepts

A **frame** is one byte sequence. A **multipart message** groups several frames
into one message, preserving their order and boundaries.

`send_multipart()` sends a list of frames as one message; `recv_multipart()` returns
all of that message's frames as a list. Frames from separate messages are not
interleaved. Keeping metadata and payload in separate frames avoids inventing a
separator inside a single byte sequence.

The application or socket pattern gives each frame its meaning. Arbitrary frame
contents do not automatically become routing instructions.

### This example

The PUSH sender sends `[b"header", b"body"]` in one call. The PULL receiver reads
both frames in one call.

```bash
# Terminal 1
python 04_multipart.py receive
# Terminal 2
python 04_multipart.py send
```

The receiver prints `[b'header', b'body']`. The words `header` and `body` are just
payload labels chosen here. Lesson 5 shows how a ROUTER gives the first frame a
specific routing role.

## 5. Address an engine with ROUTER / DEALER

### ZMQ concepts

ROUTER and DEALER are bidirectional socket types. They allow repeated sends and
receives without the strict send/receive alternation of REQ/REP.

A **ROUTER** lets application code select a connected peer by its **routing
identity**. On receive, it prepends the sender's identity as a frame. On send, it
consumes the first frame as the destination identity and forwards the remaining
frames to that peer. An identity selects a connection; it does not identify an
individual request. Peers need distinct identities at a given ROUTER. Set an
explicit DEALER identity before connecting.

A **DEALER** sends without an explicit destination frame. With one connection,
messages go there; with several, it distributes outgoing messages among available
peers. When paired directly with a ROUTER, it receives the payload frames without
the ROUTER's destination frame.

A readiness handshake is an application protocol: a peer sends an agreed message
so the receiver can learn its route before addressing work to it. Words such as
`READY` and `DONE` have no built-in ZeroMQ meaning.

### This example

The API binds one ROUTER. Two engines connect DEALER sockets with identities
`b"0"` and `b"1"`, then each sends `READY`. After receiving both, the API sends
work to engine 1, then engine 0, selecting them by identity rather than readiness
order. Each engine receives its JSON payload and sends `DONE` on the same socket.

```bash
# Terminal 1
python 05_router_dealer.py api
# Terminal 2
python 05_router_dealer.py 0
# Terminal 3
python 05_router_dealer.py 1
```

Watch the frame transformations:

| Sender call | Receiver sees |
| --- | --- |
| DEALER sends `b"READY"` | ROUTER receives `[engine_identity, b"READY"]` |
| ROUTER sends `[engine_identity, json_bytes]` | DEALER receives only `json_bytes` |
| DEALER sends `b"DONE"` | ROUTER receives `[engine_identity, b"DONE"]` |

Each engine prints its own rank in its received token list. Output across processes
can interleave; send order does not control terminal print order. This handshake
establishes each engine's request route. It does not certify that a separate GPU
worker has initialized, and it avoids using sleeps to guess connection readiness.

## 6. Poll more than one socket

### ZMQ concepts

A blocking receive waits on one socket, even if another socket already has work.
A **Poller** lets one thread wait for readiness on several registered sockets.

Register a socket with `zmq.POLLIN` to watch for incoming messages. `poll()` returns
socket/event pairs; `POLLIN` indicates that a message is ready to read. Polling does
not consume it: a subsequent receive call does that. A poll timeout, expressed in
milliseconds, limits that wait and returns no events when it expires.

Application code can associate each socket object with metadata, such as which
peer or service the connection represents.

### This example

The engine binds two PULL sockets, one per API, and registers both with a Poller.
Its `sources` dictionary maps each receiving socket to its API number. The full
demo later applies the same polling technique to DEALER sockets.

```bash
# Terminal 1
python 06_poller.py engine
# Terminal 2
python 06_poller.py 1
# Terminal 3
python 06_poller.py 0
```

Start both senders within ten seconds of starting the engine. It prints both
APIs' messages, reading each with `recv_json()` after polling reports readiness.
Start API 1 first and briefly delay API 0 to see that API 1's message can be handled
without waiting on API 0's socket. A poll with no events for ten seconds makes
this program raise `TimeoutError`.

## 7. Separate request and result channels

### ZMQ concepts

An application can use separate sockets for requests and results. This is a
protocol design choice: ROUTER/DEALER can also carry replies on the request
connection, as in lesson 5. PUSH/PULL supplies a one-way path suitable for results.

Having several requests **outstanding** means sending more work before collecting
the earlier results. An application request ID allows each returned result to be
associated with its request. The application must preserve and interpret that ID;
ZeroMQ does not perform the matching.

### This example

The API binds a ROUTER for requests and a PULL for results. The engine connects a
DEALER and a PUSH respectively, then sends `READY` on its request socket. The API
sends requests 100 and 101 before reading either result. The engine copies each
request's `id` and tokens into a result on the separate PUSH socket.

```bash
# Terminal 1
python 07_separate_results.py api
# Terminal 2
python 07_separate_results.py engine
```

The API prints two dictionaries with IDs 100 and 101 and values `[1, 2]`. The
engine echoes the tokens to keep the focus on communication. The IDs make it
possible to match results; this small program simply prints them.

## 8. Return results to the API that submitted the request

### ZMQ concepts

When results use a separate channel, application code must choose the return path.
ZeroMQ does not infer a relationship between independently created request and
result sockets, or use a payload's request ID to find the originating API.

A mapping from incoming socket to outgoing socket can encode that relationship.
A dedicated PUSH connection to each API gives the application control over where
results go. A single PUSH connected to several APIs would distribute messages
among them, rather than select the request's origin.

Routing identities are local to a ROUTER's set of peers. A peer may use the same
identity at separate ROUTER sockets without a conflict between those ROUTERs.

### This example

Two APIs each bind a ROUTER for requests and a PULL for results. The engine creates
one DEALER/PUSH pair per API, registers both DEALERs with a Poller, and records
the return socket for each request socket:

```python
replies[request] = result
replies[request].send_json(...)
```

Both DEALERs use identity `b"0"`, each at a different API's ROUTER. When work
arrives, the engine uses the receiving socket as the dictionary key to select the
correct PUSH. This is the full demo's return-routing logic.

```bash
# Terminal 1
python 08_multiple_apis.py 0
# Terminal 2
python 08_multiple_apis.py 1
# Terminal 3
python 08_multiple_apis.py engine
```

API 0 receives ID `api-0` with values `[0]`; API 1 receives ID `api-1` with values
`[1]`. The request IDs label the results; the socket mapping chooses their return
paths.

## 9. Understand a timeout and shutdown

### ZMQ concepts

By default, `recv()` waits indefinitely. `RCVTIMEO` sets a maximum wait in
milliseconds for an individual receive. If no message arrives, PyZMQ raises
`zmq.Again`, and the application decides how to respond. A Poller timeout instead
limits the wait for readiness across registered sockets.

**Linger** controls what happens to pending outgoing messages when a socket
closes. `close(linger=0)` discards them instead of waiting for transfer. It does
not acknowledge delivery or tell peers to exit. The default linger can leave
context termination waiting for queued messages to transfer.

Close sockets before calling `ctx.term()`. Alternatively, `ctx.destroy(linger=0)`
closes the context's sockets and terminates the context, discarding pending sends.
Use `destroy()` only when no sockets in that context are active in other threads;
socket closure is not thread-safe.

### This example

One PULL socket binds an endpoint, sets `RCVTIMEO` to 100 ms, and deliberately has
no sender. Its receive times out, the exception handler prints a message, and the
`finally` block closes the socket before terminating the context.

```bash
python 09_timeouts_shutdown.py
```

It prints `No message arrived within 100 ms` and exits. The PULL socket has no
outgoing queue to discard, so the `linger=0` call illustrates cleanup syntax; this
run does not demonstrate dropped outgoing messages. Earlier lessons use context
managers and default linger so normal shutdown does not deliberately discard
queued sends.

## 10. Assemble the process tree

### ZMQ concepts

A multiprocess application gives each process its own ZeroMQ context and sockets.
Pass configuration such as endpoint paths and identities to child processes;
create their sockets inside those processes. Sockets should also stay with their
owning thread.

The earlier concepts compose: ROUTER identities select peers, a Poller watches
several input sockets, and an application mapping selects a separate result path.
These mechanisms handle messaging; they do not launch processes or perform the
workers' computation.

Python's `multiprocessing` provides the other pieces here. `spawn` starts fresh
interpreters, and a Python `Pipe` exchanges data between a parent and child.
Neither is a ZeroMQ feature. A stop value sent through a Pipe is an application
protocol, just as `READY` is on the ZeroMQ connections.

### This example

One command spawns two APIs and two engines; each engine spawns one worker.
All API/engine pairs are connected over IPC. Each API binds one ROUTER for
requests and one PULL for results. Each engine creates one DEALER/PUSH pair per
API, polls its two request sockets, and maps each to the corresponding result
socket.

Each API waits for both engines' `READY` messages, then sends one batch to each
engine. Each engine passes the tokens to its worker through a Python Pipe,
waits for the doubled values, and returns a result to the originating API.

```bash
python 10_process_tree.py
```

There are four results, printed in any interleaving:

| Request ID | Input tokens | Returned values |
| --- | --- | --- |
| api-0/engine-0 | [0, 0] | [0, 0] |
| api-0/engine-1 | [0, 1] | [0, 2] |
| api-1/engine-0 | [1, 0] | [2, 0] |
| api-1/engine-1 | [1, 1] | [2, 2] |

Read `api_server()`, then `engine()`, then `worker()`, then the main block.
The engine sends `None` through its Pipe to stop its worker and joins that process.
The workers use ordinary Python arithmetic so this lesson needs no GPUs. The
original cuTile/NCCL demo still uses GPU workers.

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
"""One message of bytes.

ZMQ concepts
------------
A Context manages messaging resources and creates sockets. Socket types define
behavior: PUSH sends complete byte messages; PULL receives them. bind() listens
at an endpoint and connect() connects to it, independently of who sends.
Connection setup happens in the background. send() queues data without confirming
application processing; recv() waits for one complete message.

This example
------------
The receiver binds a PULL socket at tcp://127.0.0.1:5501. The sender connects a
PUSH socket and sends b"hello". The receiver prints b'hello'. Default linger can
keep shutdown waiting for queued data if the receiver has not connected yet.

Run in separate terminals:
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
```

## 03_ipc.py

```python
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
```

## 04_multipart.py

```python
"""Several byte frames in one message.

ZMQ concepts
------------
A frame is one byte sequence. A multipart message groups frames in order as one
message; recv_multipart() returns its frames together as a list. The application
or socket pattern assigns their meaning; arbitrary labels do not cause routing.

This example
------------
The PUSH sender sends [b"header", b"body"] in one call. The PULL receiver prints
[b'header', b'body']: one message with two frames. These labels are just data.
Later lessons use multipart messages for a routing identity plus a payload.

Run in separate terminals:
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
"""Address specific peers with ROUTER / DEALER.

ZMQ concepts
------------
ROUTER and DEALER both send and receive without strict send/receive alternation.
A ROUTER prepends the sender's routing identity on receive. On send, it consumes
the first frame as the destination identity and forwards the remaining frames.
An identity selects a peer connection, not an individual request. Peers need
distinct identities at a given ROUTER; set an explicit identity before connect().

A DEALER sends without a destination frame. With one connection, messages go
there; with several, it distributes outgoing messages among available peers.
When connected directly to a ROUTER it receives only the payload frames.
A readiness handshake is an application protocol for learning a peer's route;
READY and DONE are ordinary payloads, not built-in ZeroMQ commands.

This example
------------
The API binds a ROUTER. Two engines connect DEALERs with identities b"0" and b"1"
and send READY. The API receives [identity, b"READY"] from each, then addresses
work to engine 1 before engine 0 regardless of readiness order. Each engine
receives its JSON payload and replies DONE. Terminal print order may vary.
The handshake establishes request routes; it does not certify GPU worker setup.

Run in separate terminals:
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
"""Wait for input from several sockets.

ZMQ concepts
------------
A blocking receive waits on one socket even if another has data. A Poller waits
for events across registered sockets. POLLIN indicates a message is ready;
polling does not consume it, so a receive call follows. A poll timeout is in
milliseconds. Application dictionaries can associate sockets with peer metadata.

This example
------------
The engine binds two PULL sockets, registers both, and maps them to API numbers
in sources. It receives whichever API is ready and prints both messages. Start
API 1 first to see it handled without waiting for API 0. A poll with no events
for ten seconds raises TimeoutError. The full demo polls DEALERs the same way.

Run in separate terminals; start both senders within ten seconds:
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
"""Separate channels for requests and results.

ZMQ concepts
------------
Applications can send requests on ROUTER/DEALER and results on PUSH/PULL. Separate
channels are a design choice; ROUTER/DEALER can also carry replies. Request IDs
let application code associate results with outstanding requests. ZeroMQ neither
performs that matching nor requires a result before another request is sent.

This example
------------
The API binds a ROUTER and a PULL; the engine connects a DEALER and a PUSH.
After READY, the API sends requests 100 and 101 before receiving either result.
The engine echoes each request's tokens as values and copies its id into the
result. The API prints both dictionaries; it does not implement an ID lookup.

Run in separate terminals:
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
"""Choose the return path for each API.

ZMQ concepts
------------
Separate request/result sockets need an application-defined relationship. A map
from incoming socket to outgoing socket can choose the return path; ZeroMQ does
not infer it from a request ID. One PUSH connected to several APIs distributes
messages among them. A dedicated PUSH per API permits explicit path selection.
Routing identities belong to each ROUTER's own set of peers.

This example
------------
The engine connects one DEALER/PUSH pair per API and polls both DEALERs. Both use
identity b"0", each at a different ROUTER. replies[request] maps the socket that
received work to the correct result socket. API 0 receives id api-0 with values
[0]; API 1 receives id api-1 with values [1]. The mapping chooses the destination.

Run in separate terminals:
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
"""Receive timeout and explicit cleanup.

ZMQ concepts
------------
RCVTIMEO limits each receive's wait in milliseconds; expiry raises zmq.Again.
A poll timeout instead limits waiting for readiness. Linger controls pending
outgoing messages at close: linger=0 discards them. It does not confirm delivery
or stop peers. Close sockets before ctx.term(). ctx.destroy(linger=0) closes all
context sockets and terminates it; do not use it with active sockets in other
threads, because socket closure is not thread-safe.

This example
------------
No sender connects to the PULL socket, so recv() times out after 100 ms and prints
No message arrived within 100 ms. The finally block closes the socket before
terminating its context. This PULL has no outgoing messages to discard, so the
run illustrates cleanup syntax rather than demonstrating discarded sends.

Run in one terminal: python 09_timeouts_shutdown.py
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
"""Assemble the API / engine / worker messaging.

ZMQ concepts
------------
Each process creates its own context and sockets; pass configuration to children
rather than socket objects. Sockets stay with their owning thread. ROUTER
identities, polling, IPC transport, and separate result paths can be combined.
Python's spawn starts fresh interpreters, and multiprocessing.Pipe connects a
parent and child. Process creation, Pipes, and worker computation are not ZMQ
features; stop messages are defined by the application.

This example
------------
Two APIs each bind a request ROUTER and result PULL. Two engines each connect a
DEALER/PUSH pair to both APIs, send READY, and poll their request sockets. Every
API sends one batch to each engine. Each engine passes tokens through a Python
Pipe to its own worker, waits for doubled values, and returns them using the
request-to-result socket map. Four results print in any interleaving. Each engine
sends None through its Pipe to stop its worker. No GPUs are needed.

Run in one terminal: python 10_process_tree.py
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
