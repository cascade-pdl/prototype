from dataclasses import dataclass


@dataclass
class RunStatus:
    """A snapshot of a spawned task's state, polled via Handle.state()"""

    running: bool
    exit_code: int | None = None
