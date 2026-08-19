"""Mock detection ref: 3-8 detections per input integer.

Written against ``cascade.node`` — the library form, for a ref that needs code. This one
does, because it derives its output from its input rather than merely relocating files.

Accepts either port, so one script serves both the scattered and the flat pipeline:
``number`` (int) is the scattered case, ``numbers`` (int[]) the whole array.
"""
import random

import os

import cascade.node as cn


def detections_for(value: int, rng: random.Random) -> list[dict]:
    return [
        {"number": value, "score": round(rng.random(), 4)}
        for _ in range(rng.randint(3, 8))
    ]


def main() -> int:
    with cn.from_env(os.environ) as n:
        rng = random.Random(n.args.get("seed", 0) + hash(n.instance_id or "") % 10_000)

        if n.has("number"):
            values = [int(n.read("number"))]
        elif n.has("numbers"):
            values = [int(v) for v in n.read("numbers")]
        else:
            raise cn.NodeError("expected an input on 'number' or 'numbers'")

        detections = [d for v in values for d in detections_for(v, rng)]
        n.log(f"{len(values)} input(s) -> {len(detections)} detection(s)")
        n.write("detections", detections)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
