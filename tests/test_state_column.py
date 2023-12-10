import argparse
from unittest import TestCase

from processor.processor_state import State, StateConfig, add_state_column_value, StateDataColumnDefinition, \
    StateConfigLM
from procli import column_action_add


class StateCommandLineInterfaceTests(TestCase):

    def test_state_column_add(self):

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

        kwargs = {
            "search_path": "./tmp",
            "column_name": "response_model_name",
            "column_value_func": "state.config.model_name",
            "force_yes": True
        }

        namespace = argparse.Namespace(**kwargs)
        files = column_action_add(args=namespace)

        state = state.load_state(state.config.output_path)
        assert state.columns['response_model_name'].value == state.config.model_name

        # add_state_column_value(
        #     column=StateDataColumnDefinition(
        #         name="test me",
        #
        #     )
        # )
