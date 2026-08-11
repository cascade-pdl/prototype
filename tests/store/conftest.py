import pytest

from tests.support.memory_store import MemoryStore


@pytest.fixture
def memory_store() -> MemoryStore:
    """A fresh, empty in-memory store. Construct ``MemoryStore(...)`` directly
    when a test needs several (e.g. a cross-store copy) or a scoped one."""
    return MemoryStore()