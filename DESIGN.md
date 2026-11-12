# Design Notes — Branch Queue

## Why one queue per service type?

A single ordered list of all clients would force `call_next(service_type)` to
scan the entire waiting room to find the first ticket matching that service.
That is a linear scan, so the cost grows with every client waiting for *other*
services — exactly the inefficiency the operations manager complained about.

`BranchQueue` instead keeps one FIFO `collections.deque` per service type
(`deposit`, `withdrawal`, `account_management`). Each agent dequeues from the
front of their own queue only. That makes `call_next` and `peek_next` O(1) and
independent of the other queues: the deposits agent never even looks at the
withdrawal queue. `list_waiting` and `stats` simply iterate the small, fixed set
of queues.

Ticket numbers come from a single global counter guarded by the same lock, so
numbers stay globally sequential across all service types even though the
tickets themselves live in separate queues.

## Concurrent `call_next` on the same service type

If two agents of the same service type call `call_next` at the same moment, a
naive implementation could let both read the same head ticket before either
removes it, serving the same client twice.

The fix is to make "select the head" and "remove the head" one atomic step:
acquire the queue's lock, check the queue, `popleft()` the front ticket, then
release the lock. The **removal must happen first, inside the critical
section**. The first agent to acquire the lock removes the ticket; the second
agent then sees either a different head or an empty queue (and raises
`EmptyQueueError` instead of double-serving). In production this same
check-and-dequeue would live in a database transaction or a single-threaded
event loop rather than a Python `threading.Lock`.
