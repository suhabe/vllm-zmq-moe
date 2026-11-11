"""Assemble the messaging: two APIs, two engines, one worker per engine.

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
