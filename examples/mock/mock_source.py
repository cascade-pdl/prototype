"""Mock source ref: emits a range of integers.

Reads nothing, writes one port. ``count`` comes from the dag node's ``args``.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from nodeio import context


def main() -> int:
    ctx = context()
    count = int(ctx.args.get("count", 10))
    numbers = list(range(count))
    ctx.log(f"emitting {count} integers")
    ctx.write("numbers", numbers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())