"""Build contexts: the content hash that names a built image, and compile recording it."""
import shutil

import pytest
from yaml import safe_load

import cascade
from cascade.model.pipeline import Pipeline
from cascade.plan import build_context
from cascade.plan.build_context import check_context, context_hash, uses_cascade
from cascade.plan.compile import CompileError, check, compile_pipeline


@pytest.fixture
def context(tmp_path):
    d = tmp_path / "refs" / "tool"
    d.mkdir(parents=True)
    (d / "Dockerfile").write_text("FROM alpine@sha256:abc\nCOPY run.sh /run.sh\n")
    (d / "run.sh").write_text("echo hi\n")
    return d


def test_the_hash_is_stable_and_independent_of_where_the_directory_is(context, tmp_path):
    copy = tmp_path / "elsewhere"
    shutil.copytree(context, copy)
    assert context_hash(context) == context_hash(context) == context_hash(copy)


@pytest.mark.parametrize("change", ["edit", "add", "rename", "chmod"])
def test_any_change_to_what_goes_into_the_image_changes_the_hash(context, change):
    before = context_hash(context)
    script = context / "run.sh"
    if change == "edit":
        script.write_text("echo bye\n")
    elif change == "add":
        (context / "data.txt").write_text("x")
    elif change == "rename":
        script.rename(context / "start.sh")
    else:
        script.chmod(0o755)
    assert context_hash(context) != before


def test_cascade_source_counts_only_when_the_dockerfile_uses_it(context, monkeypatch, tmp_path):
    source = tmp_path / "cascade.py"
    source.write_text("v1")
    monkeypatch.setattr(build_context, "cascade_source", lambda: [("cascade/x.py", source)])

    plain = context_hash(context)
    source.write_text("v2")
    assert context_hash(context) == plain  # no `--from=cascade`: cascade is no input

    (context / "Dockerfile").write_text("FROM python@sha256:abc\nCOPY --from=cascade . /src\n")
    assert uses_cascade(context)
    with_v2 = context_hash(context)
    source.write_text("v3")
    assert context_hash(context) != with_v2


def test_the_real_cascade_source_is_installable(context):
    names = [name for name, _ in build_context.cascade_source()]
    assert {"pyproject.toml", "README.md", "cascade/__init__.py"} <= set(names)
    assert not any("__pycache__" in n or n.endswith(".pyc") for n in names)


def test_editor_files_are_not_cascade_source(monkeypatch, tmp_path):
    (tmp_path / "pyproject.toml").write_text('name = "cascade-pdl"\n')
    (tmp_path / "README.md").write_text("")
    package = tmp_path / "cascade"
    package.mkdir()
    for name in ["__init__.py", "#build.py#", "build.py~"]:
        (package / name).write_text("")
    (package / ".#build.py").symlink_to(package / "__init__.py")  # a live lock link
    monkeypatch.setattr(cascade, "__file__", str(package / "__init__.py"))

    names = [name for name, _ in build_context.cascade_source()]
    assert names == ["pyproject.toml", "README.md", "cascade/__init__.py"]


def test_a_missing_directory_or_dockerfile_is_named(tmp_path, context):
    assert "does not exist" in check_context(tmp_path / "nope")[0]
    (context / "Dockerfile").unlink()
    assert "has no Dockerfile" in check_context(context)[0]


PIPELINE = """
entrypoint: tool
input: []
refs:
  - name: tool
    runner: docker
    resources: { cpu: 1, memory: 1GiB }
    config: { build: refs/tool/ }
    input: []
    output: [ { name: o, type: string } ]
dags: []
"""


def test_compile_records_the_hash_in_the_plan(context, tmp_path):
    plan = compile_pipeline(Pipeline.decode(safe_load(PIPELINE)), base=tmp_path)
    ref = plan.run_config["tool"].config
    assert ref.build == "refs/tool/"
    assert ref.build_hash == context_hash(context)
    assert type(plan).decode(plan.encode()).run_config["tool"].config.build_hash == ref.build_hash


def test_a_built_ref_cannot_compile_without_the_pipeline_directory(context):
    [error] = check(Pipeline.decode(safe_load(PIPELINE)))
    assert "relative to the pipeline's directory" in error


def test_a_missing_build_directory_is_a_compile_error(tmp_path):
    with pytest.raises(CompileError, match="ref 'tool': build directory .* does not exist"):
        compile_pipeline(Pipeline.decode(safe_load(PIPELINE)), base=tmp_path)
