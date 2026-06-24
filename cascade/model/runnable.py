from abc import ABC

from cascade.model.types import IoDecl


class Runnable(ABC):
    name: str
    input: list[IoDecl]
    output: list[IoDecl]
