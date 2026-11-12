"""Demonstrate MoE expert parallelism with ZeroMQ, pipes, cuTile, and NCCL.

Request path: API server -> engine -> GPU worker -> expert GPUs -> API server.

Requirements: DP NVIDIA GPUs, CUDA PyTorch, pyzmq, cuda-tile.
Run: CUDA_VISIBLE_DEVICES=0,1 python moe_cutile_nccl_demo.py --dp 2 --experts 5

Each worker owns one GPU (TP=1, EP=DP). Expert e belongs to GPU e % DP.
This is a communication demo, not a full model: expert e computes ReLU(x * (e+1)).
"""

import argparse
import json
import multiprocessing as mp
import tempfile

import cuda.tile as ct
import torch
import torch.distributed as dist
import zmq


TOP_K = 2
HIDDEN_SIZE = 16
CAPACITY_PER_PEER = 8


@ct.kernel
def expert_kernel(inputs, expert_ids, weights, output, expert_parallel_size: ct.Constant[int], width: ct.Constant[int]):
    row = ct.bid(0)
    expert_id = ct.load(expert_ids, (row,), shape=())

    # Padded rows have expert_id=-1 and zero input values.
    local_expert_index = ct.maximum(expert_id, 0) // expert_parallel_size

    input_values = ct.load(inputs, (row, 0), shape=(1, width))
    expert_weights = ct.load(weights, (local_expert_index, 0), shape=(1, width))
    output_values = ct.maximum(input_values * expert_weights, 0.0)

    ct.store(output, (row, 0), output_values)


def address(root, kind, api):
    return f"ipc://{root}/{kind}-{api}"


def worker(rank, pipe, root, dp, experts):
    """Route tokens to expert GPUs, run the experts, and average their results."""
    torch.cuda.set_device(rank)
    device = torch.device("cuda", rank)

    dist.init_process_group("nccl", init_method=f"file://{root}/rendezvous", rank=rank, world_size=dp)

    local_expert_ids = list(range(rank, experts, dp))
    local_expert_scales = []

    for expert_id in local_expert_ids:
        local_expert_scales.append(expert_id + 1)

    weights = torch.tensor(local_expert_scales, device=device, dtype=torch.float32)
    weights = weights.view(-1, 1)
    weights = weights.expand(-1, HIDDEN_SIZE)
    weights = weights.contiguous()

    try:
        while True:
            tokens = pipe.recv()

            if tokens is None:
                break

            # Reserve the same number of slots for every destination GPU.
            # Unused slots keep their zero inputs and expert_id=-1.
            total_slots = dp * CAPACITY_PER_PEER
            send_inputs = torch.zeros((total_slots, HIDDEN_SIZE), device=device)
            send_expert_ids = torch.full((total_slots,), -1, device=device, dtype=torch.int32)
            token_positions = torch.full((total_slots,), -1, device=device, dtype=torch.long)
            slots_used_per_gpu = [0] * dp

            for token_position, token in enumerate(tokens):
                first_expert_id = token % experts
                second_expert_id = (token + 1) % experts

                for expert_id in (first_expert_id, second_expert_id):
                    owner_rank = expert_id % dp
                    slots_used = slots_used_per_gpu[owner_rank]
                    assert slots_used < CAPACITY_PER_PEER, "Increase CAPACITY_PER_PEER for larger batches"

                    slot = owner_rank * CAPACITY_PER_PEER + slots_used
                    send_inputs[slot].fill_(float(token))
                    send_expert_ids[slot] = expert_id
                    token_positions[slot] = token_position

                    slots_used_per_gpu[owner_rank] += 1

            # Dispatch token inputs and expert IDs to their destination GPUs.
            received_inputs = torch.empty_like(send_inputs)
            received_expert_ids = torch.empty_like(send_expert_ids)

            dist.all_to_all_single(received_inputs, send_inputs)
            dist.all_to_all_single(received_expert_ids, send_expert_ids)

            # Run this GPU's experts, including tokens received from other APIs.
            processed_outputs = torch.empty_like(received_inputs)

            ct.launch(torch.cuda.current_stream(device), (total_slots, 1, 1), expert_kernel, (received_inputs, received_expert_ids, weights, processed_outputs, dp, HIDDEN_SIZE))
            torch.cuda.synchronize(device)

            # Return expert outputs to the GPU that supplied each input token.
            returned_outputs = torch.empty_like(processed_outputs)
            dist.all_to_all_single(returned_outputs, processed_outputs)

            # Copy the first output component to the CPU for the printed trace.
            returned_values = returned_outputs[:, 0].cpu().tolist()
            original_positions = token_positions.cpu().tolist()
            original_expert_ids = send_expert_ids.cpu().tolist()

            contributions = []

            for _ in tokens:
                contributions.append([])

            for slot, token_position in enumerate(original_positions):
                if token_position < 0:
                    continue

                contribution = {
                    "expert": original_expert_ids[slot],
                    "gpu": slot // CAPACITY_PER_PEER,
                    "value": returned_values[slot],
                }
                contributions[token_position].append(contribution)

            # Show each token's expert contributions in routing order.
            for token_position, token in enumerate(tokens):
                first_expert_id = token % experts
                contributions[token_position].sort(key=lambda part: (part["expert"] - first_expert_id) % experts)

            # Average the two expert outputs for each original token.
            output = torch.zeros((len(tokens), HIDDEN_SIZE), device=device)
            valid_slots = token_positions >= 0
            output.index_add_(0, token_positions[valid_slots], returned_outputs[valid_slots] / TOP_K)

            values = output[:, 0].cpu().tolist()
            pipe.send({"values": values, "contributions": contributions})

    finally:
        pipe.close()
        dist.destroy_process_group()


def engine(rank, root, dp, experts):
    """Forward one API request to a GPU worker and send its response back."""
    engine_pipe, worker_pipe = mp.Pipe()
    gpu_process = mp.Process(target=worker, args=(rank, worker_pipe, root, dp, experts))
    gpu_process.start()
    worker_pipe.close()

    context = zmq.Context()
    poller = zmq.Poller()
    reply_sockets = {}

    for api in range(dp):
        request_socket = context.socket(zmq.DEALER)
        request_socket.setsockopt(zmq.IDENTITY, str(rank).encode())
        request_socket.connect(address(root, "requests", api))
        poller.register(request_socket, zmq.POLLIN)

        result_socket = context.socket(zmq.PUSH)
        result_socket.connect(address(root, "results", api))
        reply_sockets[request_socket] = result_socket

        request_socket.send(b"READY")

    try:
        events = dict(poller.poll(120_000))

        if not events:
            raise TimeoutError("No API request")

        request_socket = next(iter(events))
        batch = request_socket.recv_json()

        engine_pipe.send(batch["tokens"])
        worker_result = engine_pipe.recv()

        response = {"id": batch["id"], **worker_result}
        reply_sockets[request_socket].send_json(response)

    finally:
        try:
            engine_pipe.send(None)
        except (BrokenPipeError, EOFError):
            pass

        gpu_process.join(timeout=10)

        if gpu_process.is_alive():
            gpu_process.terminate()
            gpu_process.join()

        engine_pipe.close()
        context.destroy(linger=0)


def api_server(api, root, dp, results):
    """Wait for the engines, submit one synthetic request, and collect its result."""
    context = zmq.Context()

    request_socket = context.socket(zmq.ROUTER)
    request_socket.bind(address(root, "requests", api))

    result_socket = context.socket(zmq.PULL)
    result_socket.bind(address(root, "results", api))

    try:
        ready_engines = set()

        while len(ready_engines) < dp:
            identity, message = request_socket.recv_multipart()
            assert message == b"READY"
            ready_engines.add(identity)

        # Each API sends three distinct synthetic tokens to its own engine.
        tokens = []

        for token_offset in (1, 2, 3):
            token = api * 3 + token_offset
            tokens.append(token)

        batch = {"id": api, "tokens": tokens}
        engine_identity = str(api).encode()
        request_body = json.dumps(batch).encode()

        request_socket.send_multipart([engine_identity, request_body])

        response = result_socket.recv_json()
        results.put((api, tokens, response))

    finally:
        context.destroy(linger=0)


def show_trace(api, tokens, response):
    """Print one complete, ordered request trace after all processes finish."""
    values = response["values"]
    contributions = response["contributions"]

    print(f"\n===== REQUEST {api} =====")
    print(f"INPUT          API {api}: {tokens}")
    print(f"1. ZMQ         API {api} -> Engine {api}")
    print(f"2. Pipe        Engine {api} -> GPU {api}")

    print("3. ROUTE / NCCL dispatch       (token -> expert on GPU)")

    for token, parts in zip(tokens, contributions):
        route_parts = []

        for part in parts:
            route_parts.append(f"E{part['expert']}@GPU{part['gpu']}")

        route = ", ".join(route_parts)
        print(f"   token {token:<3} -> {route}")

    print("4. cuTile / NCCL return        (expert result -> average)")

    for token, parts, value in zip(tokens, contributions, values):
        detail_parts = []

        for part in parts:
            detail = f"E{part['expert']}@GPU{part['gpu']}={part['value']:g}"
            detail_parts.append(detail)

        details = " + ".join(detail_parts)
        print(f"   token {token:<3} <- {details} -> {value:g}")

    print(f"5. Pipe        GPU {api} -> Engine {api}")
    print(f"6. ZMQ         Engine {api} -> API {api}")
    print(f"OUTPUT         API {api}: {values}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dp", type=int, default=2, help="Data/expert parallel ranks")
    parser.add_argument("--experts", type=int, default=4, help="Total MoE experts")
    args = parser.parse_args()

    if args.dp < 1:
        parser.error("--dp must be positive")

    if args.experts <= args.dp:
        parser.error("--experts must be greater than --dp")

    visible_gpus = torch.cuda.device_count()

    if visible_gpus != args.dp:
        parser.error(f"--dp={args.dp} requires exactly {args.dp} visible GPUs; found {visible_gpus}")

    print(f"DP={args.dp}, EP={args.dp}, GPUs={visible_gpus}, experts={args.experts}", flush=True)

    mp.set_start_method("spawn")

    with tempfile.TemporaryDirectory(prefix="moe-nccl-") as root:
        results = mp.SimpleQueue()
        processes = []

        # Create all engine processes first, then all API server processes.
        for rank in range(args.dp):
            engine_process = mp.Process(target=engine, args=(rank, root, args.dp, args.experts))
            processes.append(engine_process)

        for api in range(args.dp):
            api_process = mp.Process(target=api_server, args=(api, root, args.dp, results))
            processes.append(api_process)

        try:
            for process in processes:
                process.start()

            for process in processes:
                process.join(timeout=180)

                if process.exitcode != 0:
                    raise RuntimeError(f"Process {process.pid} failed (exitcode={process.exitcode})")

            traces = {}

            for _ in range(args.dp):
                api, tokens, response = results.get()
                traces[api] = (tokens, response)

            for api in sorted(traces):
                tokens, response = traces[api]
                show_trace(api, tokens, response)

        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join()


if __name__ == "__main__":
    main()
