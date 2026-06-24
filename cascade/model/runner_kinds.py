from enum import Enum


class RunnerKind(str, Enum):
    """The fixed vocabulary of runner kinds

    loaded from pipeline.refs[].runner
    """

    docker = "docker"
    subprocess = "subprocess"
    echo = "echo"
