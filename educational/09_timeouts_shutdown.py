"""Receive timeout and explicit cleanup.

ZMQ concepts
------------
RCVTIMEO limits each receive's wait in milliseconds; expiry raises zmq.Again.
A poll timeout instead limits waiting for readiness. Linger controls pending
outgoing messages at close: linger=0 discards them. It does not confirm delivery
or stop peers. Close sockets before ctx.term(). ctx.destroy(linger=0) closes all
context sockets and terminates it; do not use it with active sockets in other
threads, because socket closure is not thread-safe.

This example
------------
No sender connects to the PULL socket, so recv() times out after 100 ms and prints
No message arrived within 100 ms. The finally block closes the socket before
terminating its context. This PULL has no outgoing messages to discard, so the
run illustrates cleanup syntax rather than demonstrating discarded sends.

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
