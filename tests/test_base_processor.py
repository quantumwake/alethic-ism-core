from alethic_ism_core.core.base_processor import BaseProcessor


class MockProcessor(BaseProcessor):

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        assert input_query_state['animal'] in ['cat', 'dog', 'pig', 'sheep']


def create_mock_processor():
    pass
