"""A receive timeout and explicit cleanup; runs in one terminal.

No sender connects, so recv() raises zmq.Again after the configured timeout.
The finally block closes the socket before terminating its context.
Using linger=0 allows close to discard queued outgoing messages rather than wait.

Run: python 09_timeouts_shutdown.py
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
