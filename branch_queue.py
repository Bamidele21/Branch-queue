"""Branch Queue Manager CLI.

Models a bank branch where clients take a numbered ticket for one of three
service types: deposit, withdrawal, or account_management.

Design notes
------------
Why one queue per service type?
    A single shared list would force ``call_next(service_type)`` to scan the
    whole waiting room looking for the first matching ticket. That is O(n) per
    call and gets worse as the branch fills up with other services. Keeping a
    FIFO ``deque`` per service type means ``call_next`` is O(1): the agent
    simply pops from the front of their own queue, indifferent to how many
    people wait for the other services.

Concurrent ``call_next`` on the same service
    Two agents of the same service type could both observe the same front
    ticket before either removes it, calling the same client twice. The
    selection and removal must be one atomic operation: acquire the queue's
    lock, read the head, ``popleft()`` it, then release. The mutation that must
    happen first is the removal from the queue *inside* the critical section --
    the second agent then sees the updated head (or an empty queue).
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, List

SERVICE_TYPES = ("deposit", "withdrawal", "account_management")

SERVICE_LABELS = {
    "deposit": "Deposits",
    "withdrawal": "Withdrawals",
    "account_management": "Account Management",
}


class EmptyQueueError(Exception):
    """Raised when calling or peeking a service queue with no waiting clients."""


class InvalidServiceError(ValueError):
    """Raised when a service type is not one of the supported services."""


def normalize_service_type(service_type: str) -> str:
    """Normalize user input to one of the canonical service type strings."""
    if not isinstance(service_type, str):
        raise InvalidServiceError("Service type must be a string.")
    normalized = service_type.strip().lower().replace(" ", "_").replace("-", "_")
    if normalized not in SERVICE_TYPES:
        raise InvalidServiceError(
            f"Invalid service type '{service_type}'. "
            f"Choose one of: {', '.join(SERVICE_TYPES)}."
        )
    return normalized


@dataclass
class Ticket:
    number: int
    client_name: str
    service_type: str
    issued_at: datetime = field(default_factory=datetime.now)

    def __str__(self) -> str:
        stamp = self.issued_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"#{self.number:03d} {self.client_name} ({self.service_type}) @ {stamp}"


class BranchQueue:
    """Manages one FIFO queue per service type plus a global ticket counter."""

    def __init__(self) -> None:
        self._queues: Dict[str, Deque[Ticket]] = {
            service: deque() for service in SERVICE_TYPES
        }
        self._counter = 0
        self._lock = threading.Lock()

    def issue_ticket(self, client_name: str, service_type: str) -> Ticket:
        """Create and enqueue a ticket in the correct service queue."""
        if not isinstance(client_name, str) or not client_name.strip():
            raise ValueError("Client name cannot be empty.")
        service = normalize_service_type(service_type)

        with self._lock:
            self._counter += 1
            ticket = Ticket(
                number=self._counter,
                client_name=client_name.strip(),
                service_type=service,
            )
            self._queues[service].append(ticket)
        return ticket

    def call_next(self, service_type: str) -> Ticket:
        """Dequeue and return the next client for a service type."""
        service = normalize_service_type(service_type)
        with self._lock:
            queue = self._queues[service]
            if not queue:
                raise EmptyQueueError(
                    f"No clients waiting for {SERVICE_LABELS[service]}."
                )
            return queue.popleft()

    def peek_next(self, service_type: str) -> Ticket:
        """Return the next client for a service type without removing them."""
        service = normalize_service_type(service_type)
        with self._lock:
            queue = self._queues[service]
            if not queue:
                raise EmptyQueueError(
                    f"No clients waiting for {SERVICE_LABELS[service]}."
                )
            return queue[0]

    def list_waiting(self) -> Dict[str, List[Ticket]]:
        """Return each service type mapped to its ordered waiting tickets."""
        with self._lock:
            return {service: list(queue) for service, queue in self._queues.items()}

    def stats(self) -> Dict[str, int]:
        """Return waiting counts per service type plus a 'total' key."""
        with self._lock:
            counts = {service: len(queue) for service, queue in self._queues.items()}
        counts["total"] = sum(counts.values())
        return counts


def prompt_service_type() -> str:
    while True:
        raw = input(
            "Service type (deposit / withdrawal / account_management): "
        ).strip()
        try:
            return normalize_service_type(raw)
        except InvalidServiceError as exc:
            print(f"  ! {exc}")


def issue_ticket_flow(branch: BranchQueue) -> None:
    name = input("Client name: ").strip()
    if not name:
        print("  ! Client name cannot be empty.")
        return
    service = prompt_service_type()
    try:
        ticket = branch.issue_ticket(name, service)
    except ValueError as exc:
        print(f"  ! {exc}")
        return
    print(f"  -> Issued ticket {ticket}")


def call_next_flow(branch: BranchQueue) -> None:
    service = prompt_service_type()
    try:
        ticket = branch.call_next(service)
    except EmptyQueueError as exc:
        print(f"  ! {exc}")
        return
    print(f"  -> Now serving {ticket}")


def peek_next_flow(branch: BranchQueue) -> None:
    service = prompt_service_type()
    try:
        ticket = branch.peek_next(service)
    except EmptyQueueError as exc:
        print(f"  ! {exc}")
        return
    print(f"  -> Next for {SERVICE_LABELS[service]}: {ticket}")


def list_waiting_flow(branch: BranchQueue) -> None:
    waiting = branch.list_waiting()
    printed_any = False
    for service in SERVICE_TYPES:
        tickets = waiting[service]
        print(f"\n{SERVICE_LABELS[service]} ({len(tickets)} waiting)")
        if not tickets:
            print("  (empty)")
            continue
        printed_any = True
        for position, ticket in enumerate(tickets, start=1):
            print(f"  {position}. {ticket}")
    if not printed_any:
        print("\nNo clients are currently waiting.")


def stats_flow(branch: BranchQueue) -> None:
    counts = branch.stats()
    print("\nQueue stats")
    for service in SERVICE_TYPES:
        print(f"  {SERVICE_LABELS[service]:<20} {counts[service]}")
    print(f"  {'Total waiting':<20} {counts['total']}")


def print_menu() -> None:
    print(
        "\n=== Branch Queue Manager ===\n"
        "1. Issue ticket\n"
        "2. Call next client\n"
        "3. Peek next client\n"
        "4. List waiting clients\n"
        "5. Show stats\n"
        "6. Exit"
    )


def main() -> None:
    branch = BranchQueue()
    actions = {
        "1": lambda: issue_ticket_flow(branch),
        "2": lambda: call_next_flow(branch),
        "3": lambda: peek_next_flow(branch),
        "4": lambda: list_waiting_flow(branch),
        "5": lambda: stats_flow(branch),
    }

    print("Welcome to the Branch Queue Manager.")
    while True:
        print_menu()
        try:
            choice = input("Choose an option [1-6]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            return

        if choice == "6":
            print("Goodbye.")
            return

        action = actions.get(choice)
        if action is None:
            print("  ! Invalid option. Please enter a number from 1 to 6.")
            continue

        try:
            action()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            return


if __name__ == "__main__":
    main()
