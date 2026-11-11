"""Two APIs -> two engines -> two GPU workers; MoE expert parallelism via NCCL.

Requirements: 2 NVIDIA GPUs, CUDA PyTorch, pyzmq, cuda-tile.
Run: CUDA_VISIBLE_DEVICES=0,1 python moe_cutile_nccl_demo.py

Each worker owns one GPU and two experts (TP=1, DP=EP=2).
This is a communication demo, not a full model: expert e computes ReLU(x * (e+1)).
"""

import json
import multiprocessing as mp
import os
import tempfile

import cuda.tile as ct
import torch
import torch.distributed as dist
import zmq

DP, EXPERTS, TOP_K = 2, 4, 2
PER_GPU, HIDDEN, CAP = EXPERTS // DP, 16, 8


@ct.kernel
def expert_kernel(x, expert_ids, weights, out,
                  base: ct.Constant[int], width: ct.Constant[int]):
    row = ct.bid(0)
    expert = ct.load(expert_ids, (row,), shape=())
    local = ct.maximum(expert - base, 0)  # Padded entries have x=0.
    values = ct.load(x, (row, 0), shape=(1, width))
    w = ct.load(weights, (local, 0), shape=(1, width))
    ct.store(out, (row, 0), ct.maximum(values * w, 0.0))


def address(root, kind, api):
    return f"ipc://{root}/{kind}-{api}"


def worker(rank, pipe, root):
    torch.cuda.set_device(rank)
    device = torch.device("cuda", rank)
    dist.init_process_group("nccl", init_method=f"file://{root}/rendezvous",
                            rank=rank, world_size=DP)

    weights = (torch.arange(rank * PER_GPU + 1, (rank + 1) * PER_GPU + 1,
                            device=device, dtype=torch.float32)
               .view(PER_GPU, 1).expand(PER_GPU, HIDDEN).contiguous())
    print(f"GPU worker {rank}: pid={os.getpid()}, experts="
          f"{list(range(rank * PER_GPU, (rank + 1) * PER_GPU))}", flush=True)

    try:
        while (tokens := pipe.recv()) is not None:
            # Fixed capacity ensures that idle ranks join the same collectives.
            send_x = torch.zeros((DP * CAP, HIDDEN), device=device)
            send_e = torch.full((DP * CAP,), -1, device=device, dtype=torch.int32)
            positions = torch.full((DP * CAP,), -1, device=device, dtype=torch.long)
            counts = [0] * DP

            for pos, token in enumerate(tokens):
                for expert in (token % EXPERTS, (token + 1) % EXPERTS):
                    owner = expert // PER_GPU
                    assert counts[owner] < CAP, "Increase CAP for larger batches"
                    slot = owner * CAP + counts[owner]
                    send_x[slot].fill_(float(token))
                    send_e[slot] = expert
                    positions[slot] = pos
                    counts[owner] += 1

            # NCCL dispatch: every GPU sends CAP rows to each GPU.
            recv_x, recv_e = torch.empty_like(send_x), torch.empty_like(send_e)
            dist.all_to_all_single(recv_x, send_x)
            dist.all_to_all_single(recv_e, send_e)

            # cuTile executes the experts owned by this GPU.
            processed = torch.empty_like(recv_x)
            ct.launch(torch.cuda.current_stream(device), (DP * CAP, 1, 1),
                      expert_kernel,
                      (recv_x, recv_e, weights, processed, rank * PER_GPU, HIDDEN))
            torch.cuda.synchronize(device)

            # NCCL combine: results return to the GPUs that sent each token.
            returned = torch.empty_like(processed)
            dist.all_to_all_single(returned, processed)
            output = torch.zeros((len(tokens), HIDDEN), device=device)
            valid = positions >= 0
            output.index_add_(0, positions[valid], returned[valid] / TOP_K)
            pipe.send(output[:, 0].cpu().tolist())
    finally:
        pipe.close()
        dist.destroy_process_group()


def engine(rank, root):
    parent, child = mp.Pipe()
    gpu = mp.Process(target=worker, args=(rank, child, root))
    gpu.start()
    child.close()
    print(f"engine {rank}: pid={os.getpid()}, worker={gpu.pid}", flush=True)

    ctx = zmq.Context()
    poller, replies = zmq.Poller(), {}
    for api in range(DP):
        request = ctx.socket(zmq.DEALER)
        request.setsockopt(zmq.IDENTITY, str(rank).encode())
        request.connect(address(root, "requests", api))
        poller.register(request, zmq.POLLIN)
        result = ctx.socket(zmq.PUSH)
        result.connect(address(root, "results", api))
        replies[request] = result
        request.send(b"READY")

    try:
        events = dict(poller.poll(120_000))
        if not events:
            raise TimeoutError("No API request")
        request = next(iter(events))
        batch = request.recv_json()
        parent.send(batch["tokens"])
        replies[request].send_json({"id": batch["id"], "values": parent.recv()})
    finally:
        try:
            parent.send(None)
        except (BrokenPipeError, EOFError):
            pass
        gpu.join(timeout=10)
        if gpu.is_alive():
            gpu.terminate()
            gpu.join()
        parent.close()
        ctx.destroy(linger=0)


def api_server(api, root):
    ctx = zmq.Context()
    request = ctx.socket(zmq.ROUTER)
    request.bind(address(root, "requests", api))
    result = ctx.socket(zmq.PULL)
    result.bind(address(root, "results", api))

    try:
        ready = set()
        while len(ready) < DP:
            identity, msg = request.recv_multipart()
            assert msg == b"READY"
            ready.add(identity)

        # Rank 1 has no tokens, yet participates in NCCL to serve rank 0.
        tokens = [1, 2, 3] if api == 0 else []
        request.send_multipart([str(api).encode(),
                                json.dumps({"id": api, "tokens": tokens}).encode()])
        print(f"API {api}: {tokens} -> {result.recv_json()['values']}", flush=True)
    finally:
        ctx.destroy(linger=0)


if __name__ == "__main__":
    if torch.cuda.device_count() < DP:
        raise SystemExit("This example requires two visible NVIDIA GPUs")

    mp.set_start_method("spawn")
    with tempfile.TemporaryDirectory(prefix="moe-nccl-") as root:
        processes = ([mp.Process(target=engine, args=(i, root)) for i in range(DP)]
                     + [mp.Process(target=api_server, args=(i, root)) for i in range(DP)])
        try:
            for p in processes:
                p.start()
            for p in processes:
                p.join(timeout=180)
                if p.exitcode != 0:
                    raise RuntimeError(f"Process {p.pid} failed (exitcode={p.exitcode})")
        finally:
            for p in processes:
                if p.is_alive():
                    p.terminate()
                    p.join()
