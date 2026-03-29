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
