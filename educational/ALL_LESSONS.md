# ZMQ lessons for the API / engine / worker demo

Ten small, independent programs. Read and run them in order. Each file contains
all of its own code; there are no shared helpers hiding the ZMQ calls.

Each lesson explains the idea under **ZMQ concepts**, then shows it working under
**This example**. Each script has the same explanation at the top.

In these lessons, an **API** submits work and an **engine** handles it. The final
lesson adds **workers**, separate processes that perform the calculation.

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
| 04_multipart.py | Several frames in one message | Choose a recipient and send work |
| 05_router_dealer.py | Name connected programs and choose one | API selects a specific engine |
| 06_poller.py | Wait for input from several sockets | Engine watches connections from all APIs |
| 07_separate_results.py | Separate request/result channels | ROUTER/DEALER requests, PUSH/PULL results |
| 08_multiple_apis.py | Remember where to return each result | replies[request] returns to the right API |
| 09_timeouts_shutdown.py | Receive timeout, linger, cleanup | Avoid waiting forever; understand shutdown |
| 10_process_tree.py | Start processes and give work to a worker | Two APIs, two engines, one worker per engine |

## 1. Send one message

### ZMQ concepts

To exchange messages, each program creates a **socket**: an object with send or
receive methods. A `PUSH` socket sends; a `PULL` socket receives. A **context** is
the ZeroMQ object used to create these sockets.

The programs need an address where they can meet. One calls `bind()` to listen
there; the other calls `connect()` to reach it.

`send()` sends bytes, Python's representation of raw data. `recv()` waits for and
returns one complete message. Sending puts data in a queue; it does not mean the
other program has read it yet.

### This example

The receiver listens at `tcp://127.0.0.1:5501` using PULL. The sender connects
using PUSH and sends `b"hello"`. This address keeps communication on your machine.

```bash
# Terminal 1
python 01_bytes.py receive
# Terminal 2
python 01_bytes.py send
```

The receiver prints `b'hello'`. The `b` means the value is bytes.

You can also start the sender first. ZeroMQ connects in the background. With the
default settings, the sender may wait during shutdown until a receiver connects;
lesson 9 explains how to control that wait.

## 2. Send a dictionary from two programs

### ZMQ concepts

Messages often contain several pieces of information. JSON is a text format
that can represent dictionaries, lists, numbers, and strings. `send_json()` turns
such a value into a message; `recv_json()` turns it back into a Python value.

One PULL socket can receive from several PUSH sockets. Each message arrives
whole, but you should not rely on which sender's message arrives first.

A field named `id` can help your code identify a message. You choose its meaning;
ZeroMQ does not use JSON fields to decide where messages go.

### This example

Two senders, `alice` and `bob`, connect to one receiver. Each sends a dictionary
containing its name as `id` and `[1, 2, 3]` as `tokens`. Here, `tokens` is simply
the name we give a list of numbers.

```bash
# Terminal 1
python 02_json_many_senders.py receive
# Terminal 2
python 02_json_many_senders.py alice
# Terminal 3
python 02_json_many_senders.py bob
```

The receiver prints `alice [1, 2, 3]` and `bob [1, 2, 3]`. Their order may vary.

## 3. Use a local socket path

### ZMQ concepts

ZeroMQ can carry the same messages over different kinds of connections.
A `tcp://` address uses a host and port and can connect programs across machines.
An `ipc://` address uses a Unix socket path to connect programs on one machine.
IPC means *interprocess communication*.

Changing the connection type does not change how PUSH and PULL work. Both
programs still need to use the same address. An IPC path names a communication
socket, not a file whose contents you read with `open()`.

### This example

The programs use `ipc:///tmp/zmq-lesson-03.sock` instead of a TCP address.
`send_string()` and `recv_string()` handle converting between Python text and
the bytes carried by ZeroMQ. Run this lesson on Linux or macOS.

```bash
# Terminal 1
python 03_ipc.py receive
# Terminal 2
python 03_ipc.py send
```

The receiver prints `hello over IPC`.

Use IPC when the programs will stay on one machine; the socket directory must be
accessible to them. Use TCP when they need to communicate across machines.

## 4. Keep several parts in one message

### ZMQ concepts

Sometimes you want to send a label and some data together while keeping them
separate. ZeroMQ calls each part a **frame**. Each frame contains bytes.

`send_multipart()` sends a list of frames as one message. `recv_multipart()`
returns that message's frames together, in the same order. The receiver does not
have to guess where one part ends and the next begins.

### This example

The sender sends `[b"header", b"body"]` through PUSH. The receiver reads both
parts through PULL in one call. These words are just data chosen for this lesson;
they have no special meaning to ZeroMQ.

```bash
# Terminal 1
python 04_multipart.py receive
# Terminal 2
python 04_multipart.py send
```

The receiver prints `[b'header', b'body']`: one message with two parts.
The next lesson uses the first part to say who should receive a message.

## 5. Choose which program receives work

### ZMQ concepts

When one program talks to several others, it needs a way to tell them apart.
A **ROUTER** socket gives your code that choice. Each connected program has a
name, called its **identity**. Received messages include the sender's identity.
To send a message, your code supplies the recipient's identity and the message.

A **DEALER** socket can connect to the ROUTER and exchange messages with it. With
just that one connection, the DEALER sends there without naming a recipient.
Both socket types let your code send more messages before receiving a reply.

Your programs can agree to send `READY` when they connect. Once the ROUTER
receives it, your code knows that sender's identity and can address work to it.
`READY` is a word your programs agree on; ZeroMQ gives it no special meaning.

### This example

The API uses a ROUTER. Two engines use DEALERs and set their identities to
`b"0"` and `b"1"` before connecting. Each sends `READY`. After hearing from both,
the API sends work to engine 1, then engine 0. Each engine prints its work and
sends back `DONE`. An engine is a separate program that handles work.

```bash
# Terminal 1
python 05_router_dealer.py api
# Terminal 2
python 05_router_dealer.py 0
# Terminal 3
python 05_router_dealer.py 1
```

Each engine receives a token list containing its own number. Output from separate
programs may print in a different order from the API's sends.

Here is how the identity appears as a separate frame:

| Message sent | Message received |
| --- | --- |
| Engine sends `b"READY"` | API gets `[identity, b"READY"]` |
| API sends `[identity, json_bytes]` | Selected engine gets `json_bytes` |
| Engine sends `b"DONE"` | API gets `[identity, b"DONE"]` |

The ROUTER adds the sender's identity when receiving and removes the destination
identity when sending. `json_bytes` is the JSON dictionary encoded as bytes.
Each engine needs a different identity at this ROUTER. A message's JSON `id`
field, introduced in lesson 2, serves a separate purpose: identifying the work.

## 6. Read whichever socket has a message

### ZMQ concepts

Calling `recv()` on one socket makes your program wait there, even if another
socket already has a message. A **Poller** solves this by watching several sockets
at once and telling your code which ones have something to read.

Registering a socket with `POLLIN` means "watch this socket for incoming
messages." `poll()` waits and reports the ready sockets. Your code then calls a
receive method to read the messages. A timeout lets it stop waiting if nothing
arrives.

### This example

The engine watches two PULL sockets, one for each API. Its `sources` dictionary
records which API belongs to each socket. Each poll waits up to 10,000
milliseconds, or ten seconds.

```bash
# Terminal 1
python 06_poller.py engine
# Terminal 2
python 06_poller.py 1
# Terminal 3
python 06_poller.py 0
```

Start both senders within ten seconds. The engine prints each API's number and
message. Start API 1 first and briefly delay API 0: API 1's message can be read
without waiting for API 0. If a poll times out, this program raises `TimeoutError`.

## 7. Send work and collect results separately

### ZMQ concepts

Requests and results can share one connection, as in lesson 5, or use separate
connections. Both designs let your code send several requests before reading
results.

To tell which result belongs to which request, put a request ID in the message
and copy it into the result. Your code is responsible for using that ID to match
them. ZeroMQ simply carries the messages.

### This example

The API sends work through ROUTER to the engine's DEALER. The engine returns
results through PUSH to the API's PULL.

After the engine sends `READY`, the API sends requests 100 and 101 before reading
either result. The engine copies each request's tokens into `values` and keeps
the request's `id`. It does no computation yet.

```bash
# Terminal 1
python 07_separate_results.py api
# Terminal 2
python 07_separate_results.py engine
```

The API prints two result dictionaries: IDs 100 and 101, each with values
`[1, 2]`. The copied IDs make matching possible; this program only prints them.

## 8. Send each result back to the right API

### ZMQ concepts

When work arrives from several programs, each result must go back to the one
that submitted it. With separate work and result connections, your code must
remember which connections belong together.

A dictionary can pair each receiving socket with the socket used to return its
results. After reading work, look up the return socket and send there. A request
ID labels the work; it does not tell ZeroMQ which connection to use.

### This example

The engine has one DEALER for receiving work and one PUSH for returning results
to each API. It records each pair with `replies[request] = result`. When work
arrives on `request`, `replies[request].send_json(...)` returns the answer to that
API.

Each PUSH connects to only one API. A single PUSH connected to both APIs would
spread results between them without regard to who submitted the work.

```bash
# Terminal 1
python 08_multiple_apis.py 0
# Terminal 2
python 08_multiple_apis.py 1
# Terminal 3
python 08_multiple_apis.py engine
```

API 0 receives ID `api-0` with values `[0]`; API 1 receives ID `api-1` with values
`[1]`.

Both DEALERs use identity `b"0"`. That works because they connect to different
ROUTERs: each ROUTER only needs to distinguish its own connected programs.

## 9. Stop waiting and close resources

### ZMQ concepts

A receive normally waits until a message arrives. Set `RCVTIMEO` to limit that
wait in milliseconds. If time runs out, PyZMQ raises `zmq.Again`; your code can
catch it and decide what to do next.

At shutdown, ZeroMQ may still have outgoing messages waiting to be sent.
**Linger** controls how long to keep trying to send them. Closing a socket with
`linger=0` discards those messages immediately. This can lose unsent data.

Close your sockets first, then call `ctx.term()` to release the context's
resources. Neither step tells other programs to exit.

### This example

One PULL socket waits for a message, but no sender connects. Its 100 ms timeout
raises `zmq.Again`, which the program catches. The `finally` block closes the
socket and then the context, even if receiving fails.

```bash
python 09_timeouts_shutdown.py
```

The program prints `No message arrived within 100 ms` and exits.

This receiving socket has no outgoing messages to discard. Its `linger=0` call
shows the cleanup syntax; the data-loss tradeoff matters for sending sockets.
The commented alternative, `ctx.destroy(linger=0)`, closes all sockets in the
context before releasing it. Use that only when none are active in other threads.

## 10. Put the programs together

### ZMQ concepts

A **process** is a separately running Python program. Create a new ZeroMQ context
and sockets inside each process that uses messaging. Pass addresses and settings
to child processes so they can build their own connections. Keep each socket in
the thread that created it.

Python's `multiprocessing` library starts the processes. Its `spawn` option starts
a fresh Python interpreter for each child. A **Pipe** is another Python facility:
it lets a parent and child exchange values directly. It does not use ZeroMQ.

### This example

One command starts two APIs and two engines. Each engine also starts one worker.
The roles are:

- Each API sends one request to each engine using ROUTER.
- Each engine watches a DEALER connection from each API using a Poller.
- The engine sends the numbers to its worker over a Pipe and waits for the answer.
- The worker doubles the numbers. The engine returns them through the PUSH socket
  paired with the API that submitted the request.

The API/engine connections use IPC. All computation uses ordinary Python; no GPU
is needed.

```bash
python 10_process_tree.py
```

The APIs print four results, in any order:

| Request ID | Input tokens | Returned values |
| --- | --- | --- |
| api-0/engine-0 | [0, 0] | [0, 0] |
| api-0/engine-1 | [0, 1] | [0, 2] |
| api-1/engine-0 | [1, 0] | [2, 0] |
| api-1/engine-1 | [1, 1] | [2, 2] |

After handling its requests, each engine sends `None` through its Pipe to tell
its worker to stop. Read `api_server()`, then `engine()`, then `worker()` to follow
the work from submission to result.

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

The full MoE demo uses ZMQ to carry requests and results between APIs and engines.
Its GPU workers use NCCL to exchange data and cuTile to compute; those operations
are outside these messaging lessons.

These examples handle a fixed amount of work and then exit. A long-running
service would also need a plan for failures and retries. A successful send or a
`READY` message does not prove that work finished. Queues can fill, and a ROUTER
can silently drop a message if its destination is unknown. The
`ROUTER_MANDATORY` option makes that last error visible to your code.

## References

- [ZeroMQ guide: sockets and patterns](https://zguide.zeromq.org/docs/chapter2/)
- [ZeroMQ guide: ROUTER / DEALER and identities](https://zguide.zeromq.org/docs/chapter3/)
- [PyZMQ API](https://pyzmq.readthedocs.io/en/latest/api/zmq.html)


# Complete source, in lesson order

## 01_bytes.py

```python
"""Send one message.

ZMQ concepts
------------
To exchange messages, each program creates a socket: an object with send or
receive methods. A PUSH socket sends; a PULL socket receives. A context is
the ZeroMQ object used to create these sockets.

The programs need an address where they can meet. One calls bind() to listen
there; the other calls connect() to reach it.

send() sends bytes, Python's representation of raw data. recv() waits for and
returns one complete message. Sending puts data in a queue; it does not mean the
other program has read it yet.

This example
------------
The receiver listens at tcp://127.0.0.1:5501 using PULL. The sender connects
using PUSH and sends b"hello". This address keeps communication on your machine.

The receiver prints b'hello'. The b means the value is bytes.

You can also start the sender first. ZeroMQ connects in the background. With the
default settings, the sender may wait during shutdown until a receiver connects;
lesson 9 explains how to control that wait.

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
```

## 03_ipc.py

```python
"""Use a local socket path.

ZMQ concepts
------------
ZeroMQ can carry the same messages over different kinds of connections.
A tcp:// address uses a host and port and can connect programs across machines.
An ipc:// address uses a Unix socket path to connect programs on one machine.
IPC means interprocess communication.

Changing the connection type does not change how PUSH and PULL work. Both
programs still need to use the same address. An IPC path names a communication
socket, not a file whose contents you read with open().

This example
------------
The programs use ipc:///tmp/zmq-lesson-03.sock instead of a TCP address.
send_string() and recv_string() handle converting between Python text and
the bytes carried by ZeroMQ. Run this lesson on Linux or macOS.

The receiver prints hello over IPC.

Use IPC when the programs will stay on one machine; the socket directory must be
accessible to them. Use TCP when they need to communicate across machines.

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
"""Keep several parts in one message.

ZMQ concepts
------------
Sometimes you want to send a label and some data together while keeping them
separate. ZeroMQ calls each part a frame. Each frame contains bytes.

send_multipart() sends a list of frames as one message. recv_multipart()
returns that message's frames together, in the same order. The receiver does not
have to guess where one part ends and the next begins.

This example
------------
The sender sends [b"header", b"body"] through PUSH. The receiver reads both
parts through PULL in one call. These words are just data chosen for this lesson;
they have no special meaning to ZeroMQ.

The receiver prints [b'header', b'body']: one message with two parts.
The next lesson uses the first part to say who should receive a message.

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
"""Choose which program receives work.

ZMQ concepts
------------
When one program talks to several others, it needs a way to tell them apart.
A ROUTER socket gives your code that choice. Each connected program has a
name, called its identity. Received messages include the sender's identity.
To send a message, your code supplies the recipient's identity and the message.

A DEALER socket can connect to the ROUTER and exchange messages with it. With
just that one connection, the DEALER sends there without naming a recipient.
Both socket types let your code send more messages before receiving a reply.

Your programs can agree to send READY when they connect. Once the ROUTER
receives it, your code knows that sender's identity and can address work to it.
READY is a word your programs agree on; ZeroMQ gives it no special meaning.

This example
------------
The API uses a ROUTER. Two engines use DEALERs and set their identities to
b"0" and b"1" before connecting. Each sends READY. After hearing from both,
the API sends work to engine 1, then engine 0. Each engine prints its work and
sends back DONE. An engine is a separate program that handles work.

Each engine receives a token list containing its own number. Printed order may
vary. The ROUTER adds identity as the first frame when receiving and removes it
when sending, so each engine receives only its work. Each engine needs a distinct
identity. See README.md for the exact message parts.

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
```

## 07_separate_results.py

```python
"""Send work and collect results separately.

ZMQ concepts
------------
Requests and results can share one connection, as in lesson 5, or use separate
connections. Both designs let your code send several requests before reading
results.

To tell which result belongs to which request, put a request ID in the message
and copy it into the result. Your code is responsible for using that ID to match
them. ZeroMQ simply carries the messages.

This example
------------
The API sends work through ROUTER to the engine's DEALER. The engine returns
results through PUSH to the API's PULL.

After the engine sends READY, the API sends requests 100 and 101 before reading
either result. The engine copies each request's tokens into values and keeps
the request's id. It does no computation yet.

The API prints two result dictionaries: IDs 100 and 101, each with values
[1, 2]. The copied IDs make matching possible; this program only prints them.

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
"""Send each result back to the right API.

ZMQ concepts
------------
When work arrives from several programs, each result must go back to the one
that submitted it. With separate work and result connections, your code must
remember which connections belong together.

A dictionary can pair each receiving socket with the socket used to return its
results. After reading work, look up the return socket and send there. A request
ID labels the work; it does not tell ZeroMQ which connection to use.

This example
------------
The engine has one DEALER for receiving work and one PUSH for returning results
to each API. It records each pair with replies[request] = result. When work
arrives on request, replies[request].send_json(...) returns the answer to that
API.

Each PUSH connects to only one API. A single PUSH connected to both APIs would
spread results between them without regard to who submitted the work.

API 0 receives ID api-0 with values [0]; API 1 receives ID api-1 with values
[1].

Both DEALERs use identity b"0". That works because they connect to different
ROUTERs: each ROUTER only needs to distinguish its own connected programs.

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
"""Stop waiting and close resources.

ZMQ concepts
------------
A receive normally waits until a message arrives. Set RCVTIMEO to limit that
wait in milliseconds. If time runs out, PyZMQ raises zmq.Again; your code can
catch it and decide what to do next.

At shutdown, ZeroMQ may still have outgoing messages waiting to be sent.
Linger controls how long to keep trying to send them. Closing a socket with
linger=0 discards those messages immediately. This can lose unsent data.

Close your sockets first, then call ctx.term() to release the context's
resources. Neither step tells other programs to exit.

This example
------------
One PULL socket waits for a message, but no sender connects. Its 100 ms timeout
raises zmq.Again, which the program catches. The finally block closes the
socket and then the context, even if receiving fails.

The program prints No message arrived within 100 ms and exits.

This receiving socket has no outgoing messages to discard. Its linger=0 call
shows the cleanup syntax; the data-loss tradeoff matters for sending sockets.
The commented alternative, ctx.destroy(linger=0), closes all sockets in the
context before releasing it. Use that only when none are active in other threads.

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
"""Put the programs together.

ZMQ concepts
------------
A process is a separately running Python program. Create a new ZeroMQ context
and sockets inside each process that uses messaging. Pass addresses and settings
to child processes so they can build their own connections. Keep each socket in
the thread that created it.

Python's multiprocessing library starts the processes. Its spawn option starts
a fresh Python interpreter for each child. A Pipe is another Python facility:
it lets a parent and child exchange values directly. It does not use ZeroMQ.

This example
------------
One command starts two APIs and two engines. Each engine also starts one worker.
The roles are:

- Each API sends one request to each engine using ROUTER.
- Each engine watches a DEALER connection from each API using a Poller.
- The engine sends the numbers to its worker over a Pipe and waits for the answer.
- The worker doubles the numbers. The engine returns them through the PUSH socket
  paired with the API that submitted the request.

The API/engine connections use IPC. All computation uses ordinary Python; no GPU
is needed.

The APIs print four results, one for every API/engine pair. Values are twice the
input numbers; output order may vary. Each engine sends None through its Pipe to
stop its worker after the work is done. See README.md for all four results.

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
