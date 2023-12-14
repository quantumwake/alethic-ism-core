from alethic_ism_core.core.utils.general_utils import load_template


def test_load_template_relative_paths():

    template_path = './test_templates/test_template_P1_user.json'
    template = load_template(template_path)

    assert template != None
