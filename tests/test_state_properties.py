import json
import pytest
from ismcore.model.processor_state import (
    State, StateConfig,
    StateProperties, ExecutionStrategy, RoutingMode,
    PersistenceMode, FlattenMode, InheritanceMode, OutputEnrichment,
    StateDataKeyDefinition,
)
from ismcore.messaging.base_message_consumer_processor import BaseMessageConsumerProcessor


def _state(properties=None, config=None):
    return State(
        id="test-state",
        config=config or StateConfig(name="test"),
        properties=properties,
    )


# ── typed_properties ──────────────────────────────────────────────────────

def test_typed_properties_none_returns_defaults():
    props = _state(properties=None).typed_properties
    assert props.execution is None
    assert props.routing is None
    assert props.persistence is None
    assert props.inheritance is None
    assert props.output is None
    assert props.dedup_enabled is False
    assert props.append_to_session is False


def test_typed_properties_partial():
    props = _state(properties={"execution": {"strategy": "batch"}}).typed_properties
    assert props.execution.strategy == "batch"
    assert props.routing is None
    assert props.persistence is None


def test_typed_properties_full():
    props = _state(properties={
        "execution": {"strategy": "stream"},
        "routing": {"mode": "after_save", "dispatch": "batch"},
        "persistence": {"mode": "individual_rows", "flatten": "json_string"},
        "inheritance": {"mode": "inverse", "require_primary_key": True},
        "output": {"enrichments": ["raw_output", "prompts"]},
        "dedup_enabled": True,
        "append_to_session": True,
    }).typed_properties

    assert props.execution.strategy == ExecutionStrategy.STREAM
    assert props.routing.mode == RoutingMode.AFTER_SAVE
    assert props.persistence.mode == PersistenceMode.INDIVIDUAL_ROWS
    assert props.persistence.flatten == FlattenMode.JSON_STRING
    assert props.inheritance.mode == InheritanceMode.INVERSE
    assert props.inheritance.require_primary_key is True
    assert OutputEnrichment.PROMPTS in props.output.enrichments
    assert props.dedup_enabled is True
    assert props.append_to_session is True


# ── _get_execution_strategy ───────────────────────────────────────────────

def test_execution_strategy_defaults_to_individual():
    assert BaseMessageConsumerProcessor._get_execution_strategy(_state()) == ExecutionStrategy.INDIVIDUAL


def test_execution_strategy_batch():
    s = _state(properties={"execution": {"strategy": "batch"}})
    assert BaseMessageConsumerProcessor._get_execution_strategy(s) == ExecutionStrategy.BATCH


def test_execution_strategy_invalid_raises():
    s = _state(properties={"execution": {"strategy": "invalid"}})
    with pytest.raises(ValueError, match="invalid execution strategy"):
        BaseMessageConsumerProcessor._get_execution_strategy(s)


# ── _should_flatten_on_save ───────────────────────────────────────────────

def test_flatten_default_true():
    assert _state()._should_flatten_on_save() is True


def test_flatten_json_string_false():
    assert _state(properties={"persistence": {"flatten": "json_string"}})._should_flatten_on_save() is False


def test_flatten_none_false():
    assert _state(properties={"persistence": {"flatten": "none"}})._should_flatten_on_save() is False


# ── _should_persist_individual_rows ───────────────────────────────────────

def test_individual_rows_default_false():
    assert _state()._should_persist_individual_rows() is False


def test_individual_rows_true():
    s = _state(properties={"persistence": {"mode": "individual_rows"}})
    assert s._should_persist_individual_rows() is True


def test_individual_rows_other_modes_false():
    assert _state(properties={"persistence": {"mode": "clob"}})._should_persist_individual_rows() is False
    assert _state(properties={"persistence": {"mode": "array_columns"}})._should_persist_individual_rows() is False
    assert _state(properties={"persistence": {"mode": "disabled"}})._should_persist_individual_rows() is False


def test_individual_rows_unknown_mode_does_not_raise():
    # a stale/renamed mode (e.g. an old 'json_column' still stored on a state) must
    # fall through to False rather than raising ValueError from PersistenceMode(...).
    s = _state(properties={"persistence": {"mode": "json_column"}})
    assert s._should_persist_individual_rows() is False


# ── pre_state_apply: flatten fan-out (dot notation + individual rows) ──────

def _CHOICES():
    return {"choices": [{"choice": "answer1"}, {"choice": "answer2"}]}


def test_pre_state_apply_fans_out_individual_rows():
    # INDIVIDUAL_ROWS + (default) DOT_NOTATION: array fans out into multiple rows,
    # dot-notation keys normalized to ddl-safe column names (choices_choice).
    s = _state(properties={"persistence": {"mode": "individual_rows"}})
    result = s.pre_state_apply(query_state=_CHOICES())
    assert result == [{"choices_choice": "answer1"}, {"choices_choice": "answer2"}]


def test_pre_state_apply_no_fanout_when_not_individual():
    # Default persistence mode (disabled): flatten still happens but the legacy
    # single-row behavior is preserved (only the first row is kept, never raises).
    s = _state(properties={"persistence": {"flatten": "dot_notation"}})
    result = s.pre_state_apply(query_state=_CHOICES())
    assert result == {"choices_choice": "answer1"}


def test_pre_state_apply_cleans_nested_dot_keys_single_row():
    # nested dict (no arrays) flattens to a single row; dot keys -> underscore columns.
    s = _state(properties={"persistence": {"mode": "individual_rows"}})
    result = s.pre_state_apply(query_state={"a": {"b": {"c": 1}}})
    assert result == {"a_b_c": 1}


def test_pre_state_apply_json_string_serializes_complex_to_text():
    # FlattenMode.JSON_STRING: complex values serialized to JSON strings (no fan-out),
    # even with individual_rows persistence. Result is a single dict, choices is a str.
    s = _state(properties={"persistence": {"mode": "individual_rows", "flatten": "json_string"}})
    result = s.pre_state_apply(query_state=_CHOICES())
    assert result == {"choices": '[{"choice": "answer1"}, {"choice": "answer2"}]'}
    assert isinstance(result["choices"], str)


def test_pre_state_apply_json_string_leaves_scalars():
    s = _state(properties={"persistence": {"flatten": "json_string"}})
    result = s.pre_state_apply(query_state={"answer": "42", "n": 7})
    assert result == {"answer": "42", "n": 7}


def test_pre_state_apply_none_keeps_native_complex():
    # FlattenMode.NONE: complex values pass through as native objects (no fan-out).
    s = _state(properties={"persistence": {"mode": "individual_rows", "flatten": "none"}})
    payload = _CHOICES()
    result = s.pre_state_apply(query_state=payload)
    assert result == payload
    assert isinstance(result["choices"], list)


def test_apply_query_state_json_string_infers_text_column():
    # JSON_STRING -> stringified value -> 'str' (text) column, not 'json'.
    s = _state(properties={"persistence": {"flatten": "json_string"}})
    s.apply_query_state(query_state=_CHOICES())
    assert s.columns["choices"].data_type == "str"


def test_apply_query_state_none_infers_json_column():
    # NONE -> native complex value -> 'json' column.
    s = _state(properties={"persistence": {"flatten": "none"}})
    s.apply_query_state(query_state=_CHOICES())
    assert s.columns["choices"].data_type == "json"


# ── PersistenceMode.CLOB / ARRAY_COLUMNS: FlattenMode is bypassed ──────────

def test_clob_collapses_to_single_blob_column():
    s = _state(properties={"persistence": {"mode": "clob"}})
    result = s.pre_state_apply(query_state={"a": 1, "choices": [{"choice": "x"}]})
    assert set(result.keys()) == {"_result_set"}
    assert isinstance(result["_result_set"], str)
    assert json.loads(result["_result_set"]) == {"a": 1, "choices": [{"choice": "x"}]}


def test_clob_ignores_flatten_mode_and_drops_nothing():
    # CLOB + dot_notation must NOT fan out or cut off values — whole state in one column.
    s = _state(properties={"persistence": {"mode": "clob", "flatten": "dot_notation"}})
    result = s.pre_state_apply(query_state=_CHOICES())
    assert set(result.keys()) == {"_result_set"}
    assert json.loads(result["_result_set"]) == _CHOICES()

# NOTE: array_columns is disabled for now (batch-level aggregation, not yet implemented;
# see docs/activities.md) — no behavioral test until it is re-enabled.


# ── apply_query_state: end-to-end fan-out into the state object ────────────

def test_apply_query_state_fanout_returns_list_and_adds_rows():
    s = _state(properties={"persistence": {"mode": "individual_rows"}})
    applied = s.apply_query_state(query_state=_CHOICES())

    # one input entry fanned out into two applied rows
    assert isinstance(applied, list)
    assert applied == [{"choices_choice": "answer1"}, {"choices_choice": "answer2"}]

    # both rows were registered against a single normalized column
    assert "choices_choice" in s.columns
    assert s.count == 2
    assert s.data["choices_choice"].values == ["answer1", "answer2"]


def test_apply_query_state_single_returns_dict():
    # a non-fanning-out query state keeps the original 1-in/1-out dict contract
    s = _state(properties={"persistence": {"mode": "individual_rows"}})
    applied = s.apply_query_state(query_state={"answer": "42"})

    assert isinstance(applied, dict)
    assert applied == {"answer": "42"}
    assert s.count == 1


# ── apply_result: batch inherited_batch_data ──────────────────────────────

@pytest.mark.asyncio
async def test_apply_result_batch_wraps_as_column():
    s = _state(properties={"execution": {"strategy": "batch"}})
    input_data = [{"a": 1}, {"a": 2}, {"a": 3}]

    outputs = await s.apply_result(result={"answer": "42"}, input_data=input_data, additional_query_state=None)

    assert len(outputs) == 1
    assert outputs[0]["answer"] == "42"
    assert outputs[0]["inherited_batch_data"] == input_data


@pytest.mark.asyncio
async def test_apply_result_batch_single_item_still_wraps():
    s = _state(properties={"execution": {"strategy": "batch"}})
    input_data = [{"a": 1}]

    outputs = await s.apply_result(result={"answer": "yes"}, input_data=input_data, additional_query_state=None)

    assert len(outputs) == 1
    assert outputs[0]["inherited_batch_data"] == [{"a": 1}]


# ── apply_result: inheritance modes ───────────────────────────────────────

@pytest.mark.asyncio
async def test_apply_result_inherit_all():
    s = _state(properties={"inheritance": {"mode": "all"}})

    outputs = await s.apply_result(
        result={"answer": "42"},
        input_data={"question": "what?", "context": "ctx"},
        additional_query_state=None,
    )

    assert len(outputs) == 1
    assert outputs[0]["question"] == "what?"
    assert outputs[0]["context"] == "ctx"
    assert outputs[0]["answer"] == "42"


@pytest.mark.asyncio
async def test_apply_result_selective_filters():
    s = _state(
        properties={"inheritance": {"mode": "selective"}},
        config=StateConfig(
            name="test",
            query_state_inheritance=[StateDataKeyDefinition(name="question")],
        ),
    )

    outputs = await s.apply_result(
        result={"answer": "42"},
        input_data={"question": "what?", "secret": "should not appear"},
        additional_query_state=None,
    )

    assert len(outputs) == 1
    assert outputs[0]["question"] == "what?"
    assert outputs[0]["answer"] == "42"
    assert "secret" not in outputs[0]
