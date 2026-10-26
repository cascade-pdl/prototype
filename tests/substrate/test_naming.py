"""Built image names: from the build directory and its content hash."""
from cascade.substrate.naming import HASH_LENGTH, built_image


def test_a_built_image_is_named_by_directory_and_hash():
    assert built_image("refs/numbers/", "0123456789abcdef" * 4) == "cascade/numbers:0123456789abcdef"
    assert HASH_LENGTH == 16


def test_the_name_is_made_valid_for_docker():
    assert built_image("refs/Flat Bug!", "ab" * 32).startswith("cascade/flat-bug:")
    assert built_image(".", "ab" * 32).startswith("cascade/image:")
