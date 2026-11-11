"""Run the two-GPU ZMQ + NCCL + cuTile MoE demo on Modal.

pip install modal
modal setup
modal run modal_run.py
"""

from pathlib import Path

import modal

app = modal.App("vllm-zmq-moe")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("torch>=2.7", "pyzmq", "cuda-tile[tileiras]")
    .add_local_file(
        str(Path(__file__).with_name("moe_cutile_nccl_demo.py")),
        "/app/moe_cutile_nccl_demo.py",
    )
)


@app.function(image=image, gpu="B200:2", timeout=600)
def run_demo():
    import subprocess
    import sys

    subprocess.run(
        [sys.executable, "-u", "/app/moe_cutile_nccl_demo.py"],
        check=True,
    )


@app.local_entrypoint()
def main():
    run_demo.remote()
