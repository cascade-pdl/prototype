"""One image, two jobs: ``gen`` writes five numbers, ``add-one`` adds one to its input.

Written against the cascade node SDK: it reads its inputs from the store and writes its
outputs itself, which is why the refs say ``wrap: false``. The job comes from the ref's
``command``, passed as this script's argument.
"""
import os
import sys

from cascade.node import session


def main(job: str) -> None:
    with session(os.environ) as n:
        if job == "gen":
            n.write("numbers", list(range(5)))
        elif job == "add-one":
            n.write("m", n.read("n") + 1)
        else:
            raise SystemExit(f"unknown job {job!r}")


if __name__ == "__main__":
    main(sys.argv[1])
