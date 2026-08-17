"""Mock detection ref: 3-8 detections per input integer.

Accepts either port, so one script serves both the scattered and the flat pipeline:

- ``number``  (int)   — one integer, the scattered case.
- ``numbers`` (int[]) — the whole array, the flat case.

``seed`` in args keeps a run reproducible.
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from nodeio import context


def detections_for(value: int, rng: random.Random) -> list[dict]:
    return [
        {"number": value, "score": round(rng.random(), 4)}
        for _ in range(rng.randint(3, 8))
    ]


def main() -> int:
    ctx = context()
    rng = random.Random(ctx.args.get("seed", 0) + hash(ctx.instance_id or "") % 10_000)

    if ctx.has("number"):
        values = [int(ctx.read("number"))]
    elif ctx.has("numbers"):
        values = [int(v) for v in ctx.read("numbers")]
    else:
        raise KeyError("expected an input on 'number' or 'numbers'")

    detections = [d for v in values for d in detections_for(v, rng)]
    ctx.log(f"{len(values)} input(s) -> {len(detections)} detection(s)")
    ctx.write("detections", detections)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())