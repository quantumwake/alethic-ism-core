from ismcore.utils.general_utils import parse_response_json, parse_response, parse_response_auto_detect_type


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


def test_parse_response_json_with_escaped_apostrophe():
    text = '{\n  "qsa_syn_vote": "Don\'t know",\n  "qsa_syn_confidence": 80,\n  "qsa_syn_rationale": "Treating ecologically important areas fits my priorities but the proposal uses chemical insecticide, which I strongly oppose, creating an unresolved tradeoff.",\n  "qsa_syn_infl_ecolog": "A Lot",\n  "qsa_syn_infl_social": "Not much",\n  "qsa_syn_infl_treat": "A Lot",\n  "qsa_syn_infl_cost": "Not much"\n}'

    data_parsed, data_type, raw_response = parse_response(text)

    assert data_type == 'json'
    assert isinstance(data_parsed, dict)
    assert data_parsed['qsa_syn_vote'] == "Don't know"
    assert data_parsed['qsa_syn_confidence'] == 80
    assert data_parsed['qsa_syn_rationale'] == "Treating ecologically important areas fits my priorities but the proposal uses chemical insecticide, which I strongly oppose, creating an unresolved tradeoff."
    assert data_parsed['qsa_syn_infl_ecolog'] == "A Lot"
    assert data_parsed['qsa_syn_infl_social'] == "Not much"
    assert data_parsed['qsa_syn_infl_treat'] == "A Lot"
    assert data_parsed['qsa_syn_infl_cost'] == "Not much"


def test_parse_response_malformed_json_stores_raw_result():
    """When JSON parsing fails, the raw result should be preserved in _raw_result."""
    text = '{ "answer": "some answer", "rationale": "this has an unclosed quote }'

    data_parsed, data_type, raw_response = parse_response(text)

    assert data_type == 'raw'
    assert isinstance(data_parsed, dict)
    assert '_raw_result' in data_parsed
    assert data_parsed['_raw_result'] == text.strip()


def test_parse_response_auto_detect_type_does_not_raise_on_malformed_json():
    """parse_response_auto_detect_type should never raise, even with unparseable input."""
    text = 'this is not json at all { broken }'

    status, dtype, result = parse_response_auto_detect_type(text)

    assert status is False
    assert result == text


def test_parse_response_valid_json_still_works():
    """Ensure valid JSON still parses correctly after the raw_result fallback changes."""
    text = '{"key": "value", "num": 42}'

    data_parsed, data_type, raw_response = parse_response(text)

    assert data_type == 'json'
    assert isinstance(data_parsed, dict)
    assert data_parsed['key'] == 'value'
    assert data_parsed['num'] == 42
    assert '_raw_result' not in data_parsed
