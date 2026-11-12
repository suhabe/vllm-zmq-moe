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
