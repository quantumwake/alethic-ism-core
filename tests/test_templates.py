from ismcore.utils.general_utils import load_template, build_template_text


def test_load_template_relative_paths():

    template_path = './test_templates/test_template_P1_user.json'
    template = load_template(template_path)

    assert template != None

def test_template_fill():

    template_text = "hello {my_variable}, the sky is {color}, and the oceans is {thought}"

    status, built_text = build_template_text(template_text, query_state={
        "my_variable": "world",
        "color": "blue",
        "thought": "calming"
    })

    assert "hello world, the sky is blue, and the oceans is calming" == built_text
    assert status