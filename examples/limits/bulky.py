"""The slow step: squares its number after ``seconds`` (default 3) of pretend work.

Its ref says ``limit: bulky`` and the deployment says ``limits: {bulky: 2}``, so however
wide the fan, two of these run at once. Watch the start/done lines in the log."""
import os
import time

import cascade.node as cn


def main() -> int:
    with cn.from_env(os.environ) as n:
        seconds = float(n.args.get("seconds", 3))
        x = float(n.read("x"))
        n.log(f"start {x} at {time.strftime('%H:%M:%S')}")
        time.sleep(seconds)
        n.log(f"done  {x} at {time.strftime('%H:%M:%S')}")
        n.write("y", x * x)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
