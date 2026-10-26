"""Named limits belong to the pipeline: declared beside the refs, checked at compile,
carried in the Plan and in every slice of it that needs them."""
import pytest
from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.compile import check, compile_pipeline
from cascade.plan.plan import Plan
from cascade.plan.slice import slice_plan


PIPELINE = """
entrypoint: main
input: []
limits: { bulky: 2, api: 5 }
refs:
  - name: big
    runner: subprocess
    resources: { cpu: 1, memory: 256MiB }
    limit: %LABEL%
    config: { cmd: ["true"] }
    input: []
    output: [ { name: o, type: string } ]
  - name: call
    runner: subprocess
    resources: { cpu: 1, memory: 256MiB }
    limit: api
    config: { cmd: ["true"] }
    input: []
    output: [ { name: o, type: string } ]
dags:
  - name: inner
    input: []
    nodes: [ { name: b, runs: big } ]
    output: [ { node: b, field: o, as: o } ]
  - name: main
    input: []
    nodes: [ { name: i, runs: inner }, { name: c, runs: call } ]
    output: [ { node: i, field: o, as: o } ]
"""


def _pipeline(label: str = "bulky") -> Pipeline:
    return Pipeline.decode(safe_load(PIPELINE.replace("%LABEL%", label)))


def test_the_plan_carries_the_capacities():
    plan = compile_pipeline(_pipeline())
    assert plan.limits == {"bulky": 2, "api": 5}
    assert Plan.decode(plan.encode()).limits == plan.limits


def test_an_undeclared_label_is_a_compile_error_not_unlimited():
    """A cap that silently stops applying floods what it protected."""
    errors = check(_pipeline("bulkey"))
    assert errors == [
        "ref(s) ['big'] count against limit 'bulkey', which the pipeline's `limits:` "
        "does not declare"
    ]


def test_a_declared_label_no_ref_uses_is_allowed():
    raw = safe_load(PIPELINE.replace("%LABEL%", "bulky"))
    raw["limits"]["unused"] = 1
    assert check(Pipeline.decode(raw)) == []


@pytest.mark.parametrize("limits", [{"bulky": 0}, {"bulky": "3"}, {"bulky": True}, ["bulky"]])
def test_a_limit_is_a_positive_number_of_tasks(limits):
    raw = safe_load(PIPELINE.replace("%LABEL%", "bulky"))
    raw["limits"] = limits
    with pytest.raises(ValueError, match="limits"):
        Pipeline.decode(raw)


def test_a_slice_keeps_the_limits_its_refs_name():
    """A subdag run on its own is capped as it is inside the whole run."""
    plan = compile_pipeline(_pipeline())
    assert slice_plan(plan, "inner").limits == {"bulky": 2}
    assert slice_plan(plan, "main").limits == {"bulky": 2, "api": 5}
