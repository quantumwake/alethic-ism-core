import random

from ismcore.utils.general_utils import calculate_uuid_based_from_string_with_sha256_seed


def test_load_template_relative_paths():
    random.seed(128)
    random_string = "".join([chr(random.randint(1, 256)) for x in range(500)])
    genarated_uuid = calculate_uuid_based_from_string_with_sha256_seed(random_string)
    assert len(genarated_uuid) == 36
