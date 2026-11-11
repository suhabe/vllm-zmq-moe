"""DP APIs -> DP engines -> DP GPU workers; MoE expert parallelism via NCCL.

Requirements: DP NVIDIA GPUs, CUDA PyTorch, pyzmq, cuda-tile.
Run: CUDA_VISIBLE_DEVICES=0,1 python moe_cutile_nccl_demo.py --dp 2 --experts 5

Each worker owns one GPU (TP=1, EP=DP) and experts with expert_id % DP == rank.
This is a communication demo, not a full model: expert e computes ReLU(x * (e+1)).
"""

import argparse
import json
import multiprocessing as mp
import os
import tempfile

import cuda.tile as ct
import torch
import torch.distributed as dist
import zmq

TOP_K, HIDDEN, CAP = 2, 16, 8


def log(role, message):
    print(f"[{role}] {message}", flush=True)


@ct.kernel
def expert_kernel(x, expert_ids, weights, out,
                  ep: ct.Constant[int], width: ct.Constant[int]):
    row = ct.bid(0)
    expert = ct.load(expert_ids, (row,), shape=())
    local = ct.maximum(expert, 0) // ep  # Padded entries have x=0.
    values = ct.load(x, (row, 0), shape=(1, width))
    w = ct.load(weights, (local, 0), shape=(1, width))
    ct.store(out, (row, 0), ct.maximum(values * w, 0.0))


def address(root, kind, api):
    return f"ipc://{root}/{kind}-{api}"


def worker(rank, pipe, root, dp, experts):
    print(f"GPU worker {rank}: starting on CUDA device {rank}", flush=True)
    torch.cuda.set_device(rank)
    device = torch.device("cuda", rank)
    print(f"GPU worker {rank}: initializing NCCL rank {rank}/{dp}", flush=True)
    dist.init_process_group("nccl", init_method=f"file://{root}/rendezvous",
                            rank=rank, world_size=dp)
    print(f"GPU worker {rank}: NCCL initialized", flush=True)

    local_experts = list(range(rank, experts, dp))
    weights = (torch.tensor([e + 1 for e in local_experts],
                            device=device, dtype=torch.float32)
               .view(-1, 1).expand(-1, HIDDEN).contiguous())
    print(f"GPU worker {rank}: pid={os.getpid()}, experts={local_experts}",
          flush=True)

    try:
        while (tokens := pipe.recv()) is not None:
            log(f"GPU {rank}", f"Pipe <- engine {rank}: tokens={tokens}")
            # Each rank sends CAP slots to each peer; unused slots are zero padded.
            send_x = torch.zeros((dp * CAP, HIDDEN), device=device)
            send_e = torch.full((dp * CAP,), -1, device=device, dtype=torch.int32)
            positions = torch.full((dp * CAP,), -1, device=device, dtype=torch.long)
            counts = [0] * dp
            outgoing = [[] for _ in range(dp)]

            for pos, token in enumerate(tokens):
                chosen = (token % experts, (token + 1) % experts)
                log(f"GPU {rank}", f"route token[{pos}]={token} -> "
                    f"experts={chosen}, owner GPUs={[e % dp for e in chosen]}")
                for expert in chosen:
                    owner = expert % dp
                    assert counts[owner] < CAP, "Increase CAP for larger batches"
                    slot = owner * CAP + counts[owner]
                    send_x[slot].fill_(float(token))
                    send_e[slot] = expert
                    positions[slot] = pos
                    counts[owner] += 1
                    outgoing[owner].append(f"token[{pos}]={token}/E{expert}")

            for peer, assignments in enumerate(outgoing):
                log(f"GPU {rank}", f"NCCL dispatch -> GPU {peer}: {assignments}")

            recv_x, recv_e = torch.empty_like(send_x), torch.empty_like(send_e)
            dist.all_to_all_single(recv_x, send_x)
            dist.all_to_all_single(recv_e, send_e)

            # Inspect received token/expert pairs, grouped by sending GPU.
            received_tokens = recv_x[:, 0].cpu().tolist()
            received_experts = recv_e.cpu().tolist()
            for peer in range(dp):
                slots = range(peer * CAP, (peer + 1) * CAP)
                arrivals = [
                    f"token={received_tokens[i]:g}/E{received_experts[i]}"
                    for i in slots if received_experts[i] >= 0
                ]
                log(f"GPU {rank}", f"NCCL received <- GPU {peer}: {arrivals}")

            # cuTile executes this GPU's experts, including tokens from other APIs.
            processed = torch.empty_like(recv_x)
            ct.launch(torch.cuda.current_stream(device), (dp * CAP, 1, 1),
                      expert_kernel,
                      (recv_x, recv_e, weights, processed, dp, HIDDEN))
            torch.cuda.synchronize(device)

            computed = processed[:, 0].cpu().tolist()
            for peer in range(dp):
                slots = range(peer * CAP, (peer + 1) * CAP)
                details = [
                    f"token={received_tokens[i]:g}, E{received_experts[i]} -> {computed[i]:g}"
                    for i in slots if received_experts[i] >= 0
                ]
                log(f"GPU {rank}", f"cuTile experts for origin GPU {peer}: {details}")

            # NCCL returns each expert result to the rank owning its input.
            returned = torch.empty_like(processed)
            dist.all_to_all_single(returned, processed)
            returned_values = returned[:, 0].cpu().tolist()
            original_positions = positions.cpu().tolist()
            original_experts = send_e.cpu().tolist()
            for peer in range(dp):
                slots = range(peer * CAP, (peer + 1) * CAP)
                results = [
                    f"token[{original_positions[i]}]/E{original_experts[i]}={returned_values[i]:g}"
                    for i in slots if original_positions[i] >= 0
                ]
                log(f"GPU {rank}", f"NCCL combine <- GPU {peer}: {results}")

            output = torch.zeros((len(tokens), HIDDEN), device=device)
            valid = positions >= 0
            output.index_add_(0, positions[valid], returned[valid] / TOP_K)
            values = output[:, 0].cpu().tolist()
            log(f"GPU {rank}", f"top-{TOP_K} averaged outputs={values} -> engine Pipe")
            pipe.send(values)
    finally:
        pipe.close()
        dist.destroy_process_group()


def engine(rank, root, dp, experts):
    parent, child = mp.Pipe()
    gpu = mp.Process(target=worker, args=(rank, child, root, dp, experts))
    gpu.start()
    child.close()
    print(f"engine {rank}: pid={os.getpid()}, worker={gpu.pid}", flush=True)

    ctx = zmq.Context()
    poller, replies, sources = zmq.Poller(), {}, {}
    for api in range(dp):
        request = ctx.socket(zmq.DEALER)
        request.setsockopt(zmq.IDENTITY, str(rank).encode())
        request.connect(address(root, "requests", api))
        poller.register(request, zmq.POLLIN)
        result = ctx.socket(zmq.PUSH)
        result.connect(address(root, "results", api))
        replies[request] = result
        sources[request] = api
        request.send(b"READY")

    try:
        events = dict(poller.poll(120_000))
        if not events:
            raise TimeoutError("No API request")
        request = next(iter(events))
        batch = request.recv_json()
        origin_api = sources[request]
        log(f"ENGINE {rank}", f"ZMQ request <- API {origin_api}: "
            f"id={batch['id']}, tokens={batch['tokens']}")
        log(f"ENGINE {rank}", f"Pipe -> GPU worker {rank}: tokens={batch['tokens']}")
        parent.send(batch["tokens"])
        values = parent.recv()
        log(f"ENGINE {rank}", f"Pipe <- GPU worker {rank}: outputs={values}")
        replies[request].send_json({"id": batch["id"], "values": values})
        log(f"ENGINE {rank}", f"ZMQ result -> API {origin_api}: outputs={values}")
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


def api_server(api, root, dp):
    ctx = zmq.Context()
    request = ctx.socket(zmq.ROUTER)
    request.bind(address(root, "requests", api))
    result = ctx.socket(zmq.PULL)
    result.bind(address(root, "results", api))

    try:
        ready = set()
        while len(ready) < dp:
            identity, msg = request.recv_multipart()
            assert msg == b"READY"
            ready.add(identity)
            log(f"API {api}", f"ZMQ READY <- engine {identity.decode()}")

        # Every API submits three distinct synthetic tokens to its own engine.
        tokens = [api * 3 + i for i in (1, 2, 3)]
        log(f"API {api}", f"ZMQ request -> engine {api}: id={api}, tokens={tokens}")
        request.send_multipart([str(api).encode(),
                                json.dumps({"id": api, "tokens": tokens}).encode()])
        response = result.recv_json()
        log(f"API {api}", f"ZMQ result <- engine {api}: "
            f"id={response['id']}, values={response['values']}")
    finally:
        ctx.destroy(linger=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dp", type=int, default=2, help="Data/expert parallel ranks")
    parser.add_argument("--experts", type=int, default=4, help="Total MoE experts")
    args = parser.parse_args()

    if args.dp < 1:
        parser.error("--dp must be positive")
    if args.experts <= args.dp:
        parser.error("--experts must be greater than --dp")
    visible = torch.cuda.device_count()
    if visible != args.dp:
        parser.error(f"--dp={args.dp} requires exactly {args.dp} visible GPUs; found {visible}")
    print(f"DP={args.dp}, EP={args.dp}, GPUs={visible}, experts={args.experts}",
          flush=True)

    mp.set_start_method("spawn")
    with tempfile.TemporaryDirectory(prefix="moe-nccl-") as root:
        processes = ([mp.Process(target=engine, args=(i, root, args.dp, args.experts))
                      for i in range(args.dp)]
                     + [mp.Process(target=api_server, args=(i, root, args.dp))
                        for i in range(args.dp)])
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


if __name__ == "__main__":
    main()
