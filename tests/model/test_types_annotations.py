"""Type annotations, and the structure-field validation hole they exposed.

An annotation refines a primitive without changing it: ``string<s3-uri>`` *is* a
``string``. The compatibility rule is deliberately the cheapest sound one — identical
or omitted — so a promise may be forgotten but never invented.
"""
import pytest
from yaml import safe_load

from cascade.model.types import check_annotation, get, known
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import check
from cascade.model.types import TypeExpr


# --- TypeExpr ----------------------------------------------------------------

@pytest.mark.parametrize(
    "declared",
    ["float", "Detection[]", "string<s3-uri>", "string<s3-uri>[]", "string<uri>[][]"],
)
def test_rendering_round_trips(declared):
    """The rendered form appears in error messages and in the plan artifact, so it has
    to be exact."""
    assert TypeExpr.parse(declared).render() == declared


def test_the_annotation_does_not_disturb_the_base():
    t = TypeExpr.parse("string<s3-uri>[]")
    assert (t.base, t.depth, t.annotation) == ("string", 1, "s3-uri")


def test_element_and_collection_preserve_the_annotation():
    """It qualifies the base, not the nesting — easy to drop, and silently wrong."""
    assert TypeExpr.parse("string<s3-uri>[]").element().render() == "string<s3-uri>"
    assert TypeExpr.parse("string<uri>").as_collection().render() == "string<uri>[]"


def test_an_empty_annotation_is_no_annotation():
    assert TypeExpr.parse("string<>").annotation is None


# --- the acceptance rule -----------------------------------------------------

def test_a_promise_may_be_forgotten():
    assert TypeExpr.parse("string").accepts(TypeExpr.parse("string<s3-uri>"))


def test_a_promise_may_not_be_invented():
    assert not TypeExpr.parse("string<s3-uri>").accepts(TypeExpr.parse("string"))


def test_different_annotations_do_not_mix():
    """No hierarchy: s3-uri is not 'more specific than' uri, it is simply different."""
    assert not TypeExpr.parse("string<uri>").accepts(TypeExpr.parse("string<s3-uri>"))


def test_base_and_depth_still_have_to_match():
    assert not TypeExpr.parse("string").accepts(TypeExpr.parse("int"))
    assert not TypeExpr.parse("string").accepts(TypeExpr.parse("string[]"))


# --- the registry ------------------------------------------------------------

def test_registered_annotations_declare_their_bases():
    assert get("s3-uri").bases == frozenset({"string"})
    assert get("nope") is None
    assert "s3-uri" in known()


def test_patterns_check_values_not_types():
    """Value validation is the SDK's job — the compiler has types and no data."""
    assert get("s3-uri").matches("s3://bucket/key.jpg")
    assert not get("s3-uri").matches("/local/path.jpg")
    assert get("path").matches("anything at all")  # deliberately unvalidated


# --- pipelines ---------------------------------------------------------------

PIPELINE = """
entrypoint: main
input: []
types:
  structures:
    - name: BBox
      fields:
        - {name: x, type: %NUM%}
    - name: Crop
      fields:
        - {name: image, type: "string<%ANN%>"}
        - {name: bbox, type: %BBOX%}
refs:
  - { name: r, runner: echo, config: {}, input: [], output: [ { name: crops, type: "Crop[]" } ] }
dags:
  - name: main
    input: []
    nodes: [ { name: n, runs: r } ]
    output: [ { node: n, field: crops, as: crops } ]
"""


def _check(num="float", ann="s3-uri", bbox="BBox"):
    y = PIPELINE.replace("%NUM%", num).replace("%ANN%", ann).replace("%BBOX%", bbox)
    return check(Pipeline.decode(safe_load(y)))


def test_composed_structures_with_an_annotated_field_compile():
    assert _check() == []


def test_an_undefined_field_type_is_caught():
    """The hole this closed: a typo in a *port* was caught, the same typo in a
    *structure field* was silently accepted — and structures compose, so a mistake
    there propagates furthest."""
    (error,) = _check(num="number")
    assert "structure 'BBox' field 'x'" in error
    assert "unknown type 'number'" in error


def test_a_typo_in_a_composed_structure_name_is_caught():
    (error,) = _check(bbox="BBoxxx")
    assert "unknown type 'BBoxxx'" in error


def test_an_unregistered_annotation_is_caught():
    """Otherwise a typo becomes a promise nobody keeps."""
    (error,) = _check(ann="typoo")
    assert "unknown annotation 'typoo'" in error
    assert "s3-uri" in error  # the message lists what is available


EDGE = """
entrypoint: main
input: []
types:
  structures: []
refs:
  - { name: lister,  runner: echo, config: {}, input: [], output: [ { name: uris, type: "string<s3-uri>[]" } ] }
  - { name: consume, runner: echo, config: {}, input: [ { name: u, type: "%WANT%" } ], output: [ { name: o, type: string } ] }
dags:
  - name: main
    input: []
    nodes:
      - { name: l, runs: lister }
      - { name: c, runs: consume, scatter: u, depends_on: [ { node: l, field: uris, as: u } ] }
    output: [ { node: c, field: o, as: out } ]
"""


@pytest.mark.parametrize(
    "want,ok",
    [("string", True), ("string<s3-uri>", True), ("string<uri>", False)],
)
def test_annotations_flow_across_edges_by_the_same_rule(want, ok):
    errors = check(Pipeline.decode(safe_load(EDGE.replace("%WANT%", want))))
    assert (errors == []) is ok


# --- refinement through extends ----------------------------------------------

EXTENDS = """
entrypoint: main
input: []
types:
  structures:
    - name: Base
      fields: [ {name: image, type: "%P%"} ]
    - name: Child
      extends: Base
      fields: [ {name: image, type: "%C%"} ]
refs:
  - { name: r, runner: echo, config: {}, input: [], output: [ { name: o, type: Child } ] }
dags:
  - name: main
    input: []
    nodes: [ { name: n, runs: r } ]
    output: [ { node: n, field: o, as: out } ]
"""


@pytest.mark.parametrize(
    "parent,child,ok",
    [
        ("string", "string<s3-uri>", True),   # a child may refine
        ("string<s3-uri>", "string<s3-uri>", True),
        ("string<s3-uri>", "string", False),  # ...and may not widen
    ],
)
def test_a_child_may_refine_an_inherited_field_but_not_widen_it(parent, child, ok):
    """Anything reading the parent's field must still get the parent's promise."""
    y = EXTENDS.replace("%P%", parent).replace("%C%", child)
    errors = check(Pipeline.decode(safe_load(y)))
    assert (errors == []) is ok
    if not ok:
        assert "does not refine" in errors[0]


# --- legality lives in the type system, not the compiler ---------------------

def test_check_annotation_accepts_a_legal_pairing():
    assert check_annotation("string", "s3-uri") is None
    assert check_annotation("string", None) is None


def test_check_annotation_rejects_an_unknown_name():
    problem = check_annotation("string", "typoo")
    assert "unknown annotation 'typoo'" in problem
    assert "s3-uri" in problem


def test_check_annotation_rejects_a_wrong_base():
    """A structure's shape is already its type, so an annotation on one means nothing."""
    problem = check_annotation("Detection", "s3-uri")
    assert "does not apply to 'Detection'" in problem


def test_the_registry_is_a_spelling_check_not_a_matching_rule():
    """Compatibility never consults it: accepts() is string equality on the
    annotation, which is why an unregistered one still matches itself."""
    a = TypeExpr.parse("string<not-registered>")
    assert a.accepts(TypeExpr.parse("string<not-registered>"))
    assert check_annotation("string", "not-registered") is not None