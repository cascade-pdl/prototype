# cascade — authoring layer

Loads a pipeline, compiles it to a `Plan`, and validates it. The `Plan` is the
authoring → execution boundary: it carries everything the executor needs and
nothing it does not (no `Pipeline`, no passes, no `graphlib`).

## Layout

    cascade/
      graph.py            generic Graph[N,E] + serialization — neutral, dual-consumed
      model/              loaded pipeline data (decode + encode)
      plan/
        signature.py      TypeExpr, Signature          (artifact: derived I/O)
        type_env.py       TypeEnv + resolve_types       (artifact: runtime types, exec-only)
        plan.py           Plan                          (the crossing bundle; JSON round-trips)
        build.py          pipeline -> graphs            (pass; structural validation)
        elaborate.py      graphs -> signatures          (pass; shape + arity)
        validate.py       edges -> errors               (pass; type-name + arity)
        compile.py        pipeline -> Plan              (orchestrator + check())

Dependency direction is strictly downward: `engine -> plan -> model -> graph`.
The artifact modules (signature, type_env, plan) never import the passes, so an
executor importing `Plan` pulls in data definitions only.

## Use

    import yaml
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline, check

    pipe = Pipeline.decode(yaml.safe_load(open("examples/pipeline.yaml")))
    errors = check(pipe)          # [] if valid
    plan = compile_pipeline(pipe) # raises CompileError on any problem

    wire = json.dumps(plan.encode())          # ship to the executor
    plan = Plan.decode(json.loads(wire))      # rebuild the far side

## Not yet built (deliberate)

- Type *identity* validation is name + arity only — exact match, no subtyping.
  `TypeEnv.is_subtype` exists for the runtime payload validator, not for edges.
- The executor (`engine/executor.py`): consume a `Plan`, run `waves()` per dag,
  dispatch to runners. Nested/black-box execution; scatter fan-width is runtime.
- `encode` exists on the model types the Plan carries (DagNode, Dependency, the
  type structures); other model types decode only.
