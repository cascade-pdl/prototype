"""The `query` verb: inspect a run's state / results from the deployment's store."""

from __future__ import annotations

from .utils import load_deployment, store_from_deployment


def cmd_query(args) -> int:
    deployment, _ = load_deployment(args)
    store = store_from_deployment(deployment, args)

    key = f"runs/{args.run_id}/_run_state.json"
    if not store.has(key):
        print(f"no run state found for '{args.run_id}'")
        return 1
    state = store.get_json(key)
    if args.node:
        node = state["nodes"].get(args.node)
        if not node:
            print(f"no node '{args.node}' in run '{args.run_id}'")
            return 1
        for inst in node["instances"]:
            if args.instance and inst["instance_key"] != args.instance:
                continue
            print(f"{args.node}[{inst['instance_key']}]: {inst['status']} "
                  f"-> {inst.get('output_key')}")
        return 0
    print(f"run {state['run_id']}: {state['status']}")
    for nid, node in state["nodes"].items():
        n_inst = len(node["instances"])
        print(f"  {nid}: {node['status']} ({n_inst} instance(s))")
    return 0


def add_subcommands(sub):
    p = sub.add_parser("query", help="query a run's state / results")
    p.add_argument("run_id")
    p.add_argument("--runner-config", default=None,
                   help="deployment config YAML (for the store). If omitted, "
                        "./deployment.yaml is used when present.")
    p.add_argument("--store", default="./_cascade_store",
                   help="file-store root (only when the store is a local file store)")
    p.add_argument("--node", default=None)
    p.add_argument("--instance", default=None)
    p.set_defaults(func=cmd_query)
