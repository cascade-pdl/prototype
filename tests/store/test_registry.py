"""``cascade.store.registry`` — the {kind, config} envelope, defined once.

After the registry refactor the store-level (live ``Store``) and config-level
(inert ``StoreConfig``) ser/de share one envelope, the store-level pair
delegating to the config-level one. These tests guard that: if a future edit
reintroduces a second copy of the envelope, the equality assertion drifts and
fails here.
"""
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.registry import encode, decode, encode_config, decode_config


def _config(tmp):
    return FileConfig(root=str(tmp), scope=("wilder", "moth"))


def test_config_level_round_trip(tmp_path):
    cfg = _config(tmp_path)
    assert decode_config(encode_config(cfg)) == cfg


def test_store_level_round_trip(tmp_path):
    store = FileStore(_config(tmp_path))
    live = decode(encode(store))
    assert isinstance(live, FileStore)
    assert live.config == store.config


def test_both_levels_emit_identical_envelope(tmp_path):
    cfg = _config(tmp_path)
    store = FileStore(cfg)
    assert encode(store) == encode_config(cfg)


def test_decode_builds_a_live_store(tmp_path):
    cfg = _config(tmp_path)
    live = decode(encode_config(cfg))
    assert isinstance(live, FileStore)