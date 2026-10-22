from abc import ABC, abstractmethod

from cascade.protocol.run_status import RunStatus


class Handle(ABC):
    @abstractmethod
    async def state(self) -> RunStatus: ...

    @abstractmethod
    async def kill(self) -> RunStatus:
        """Stop the work and wait until it has stopped.

        Idempotent, and safe on work that already finished: it then just reports the
        final status. A killed instance's ``exit_code`` is whatever the substrate gives
        (a signal's negative code for a process, ``None`` for a cancelled coroutine);
        callers must not treat it as success.
        """

    @abstractmethod
    async def await_done(self) -> RunStatus | None: ...
