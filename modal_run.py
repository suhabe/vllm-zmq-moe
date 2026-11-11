"""Run the GPU-only ZMQ + NCCL + cuTile MoE demo on Modal.

pip install modal
modal setup
modal run modal_run.py --dp 2 --experts 5
"""

from pathlib import Path

import modal

app = modal.App("vllm-zmq-moe")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements(
        str(Path(__file__).with_name("requirements.txt"))
    )
    .add_local_file(
        str(Path(__file__).with_name("moe_cutile_nccl_demo.py")),
        "/app/moe_cutile_nccl_demo.py",
    )
)


@app.function(image=image, timeout=600)
def run_demo(dp: int, experts: int):
    import subprocess
    import sys

    subprocess.run(
        [
            sys.executable, "-u", "/app/moe_cutile_nccl_demo.py",
            "--dp", str(dp),
            "--experts", str(experts),
        ],
        check=True,
    )


@app.local_entrypoint()
def main(dp: int = 2, experts: int = 4):
    if dp < 1 or dp > 8:
        raise ValueError("Modal supports 1 to 8 B200 GPUs per container")
    if experts <= dp:
        raise ValueError("--experts must be greater than --dp")

    # EP = DP, TP = 1, one B200 per engine/worker.
    run_demo.with_options(gpu=f"B200:{dp}").remote(dp, experts)
