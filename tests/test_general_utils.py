from ismcore.utils.general_utils import parse_response_json


def test_parse_json_text_simply_json_array():

    text = """hello world
    ```json[
        {"movie_title":"hello world 1"},
        {"movie_title":"hello world 2"}
    ]```
    """

    status, type, parsed_json = parse_response_json(text)

    assert isinstance(parsed_json, list)

    assert parsed_json[0]['movie_title'] == 'hello world 1'
    assert parsed_json[1]['movie_title'] == 'hello world 2'

def test_parse_json_text_simple_json_obj():
    text = """```json
    {
        "answer": 0.3125,
        "formula": "1.25 / 4.0"
    }
    ```
    """

    status, type, parsed_json = parse_response_json(text)
    assert isinstance(parsed_json, dict)

    assert parsed_json['answer'] == 0.3125
    assert parsed_json['formula'] == '1.25 / 4.0'


