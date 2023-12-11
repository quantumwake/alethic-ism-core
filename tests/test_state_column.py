import argparse
from unittest import TestCase

from processor.processor_state import State, StateConfig, add_state_column_value, StateDataColumnDefinition, \
    StateConfigLM
from procli import column_action_add, column_action_rename


class StateCommandLineInterfaceTests(TestCase):

    def create_mock_state(self) -> State:
        state = State(
            config=StateConfigLM(
                name="Test me",
                version="test version 0.0",
                model_name="Hello World Model",
                provider_name="Hello World Provider",
                output_path="./tmp/tmp_state_test.pickle",
                user_template_path="./tmp/tmp_user_template.json"
            )
        )

        state.save_state(state.config.output_path)

        return state

    def create_mock_state_with_column(self):
        state = self.create_mock_state()
        namespace = argparse.Namespace(**{
            "search_path": "./tmp",
            "column_name": "response_model_name",
            "column_value_func": "state.config.model_name",
            "force_yes": True
        })
        files = column_action_add(args=namespace)
        return State.load_state(state.config.output_path)

    def test_state_column_add(self):
        state = self.create_mock_state_with_column()
        assert state.columns['response_model_name'].value == state.config.model_name

    def test_state_column_rename(self):
        state = self.create_mock_state_with_column()
        namespace = argparse.Namespace(**{
            "search_path": "./tmp",
            "column_name": "response_model_name",
            "new_column_name": "response_model_name_renamed",
            "force_yes": True
        })
        column_action_rename(args=namespace)
        state = State.load_state(state.config.output_path)

        assert state.columns['response_model_name_renamed'].value == state.config.model_name
        assert 'response_model_name' not in state.columns

