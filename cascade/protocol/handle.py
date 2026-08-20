from abc import ABC, abstractmethod

from cascade.protocol.run_status import RunStatus


class Handle(ABC):
    @abstractmethod
    async def state(self) -> RunStatus: ...

    # @abstractmethod
    # async def kill(self) -> RunStatus: ...

    @abstractmethod
    async def await_done(self) -> RunStatus | None: ...
