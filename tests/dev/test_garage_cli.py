"""``cascade dev garage env``: the shell lines, from the keys ``up`` made."""
from cascade.cli.main import main


def test_env_prints_the_keys_and_drops_a_session_token(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    (tmp_path / "cascade" / "garage").mkdir(parents=True)
    (tmp_path / "cascade" / "garage" / "keys").write_text("GKabc s3cret\n")
    from cascade.dev import garage

    monkeypatch.setattr(garage, "DEV", garage.Instance())
    assert main(["dev", "garage", "env"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "export AWS_ACCESS_KEY_ID=GKabc",
        "export AWS_SECRET_ACCESS_KEY=s3cret",
        "unset AWS_SESSION_TOKEN",
    ]


def test_env_before_up_is_a_clean_error(tmp_path, monkeypatch, capsys):
    from cascade.dev import garage

    monkeypatch.setattr(garage, "DEV", garage.Instance(state=tmp_path / "none"))
    assert main(["dev", "garage", "env"]) == 2
    assert "cascade dev garage up" in capsys.readouterr().err
