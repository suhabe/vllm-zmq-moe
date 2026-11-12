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
