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

