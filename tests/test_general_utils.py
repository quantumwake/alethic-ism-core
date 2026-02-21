from ismcore.utils.general_utils import parse_response_json, parse_response


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


