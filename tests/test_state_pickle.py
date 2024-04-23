from alethic_ism_core.core.processor_state import State


def test_load_state_from_pickle():
    test_file = ('/Users/kasrarasaee'
                 '/Development/quantumwake/temp_processor_code/states'
                 '/animallm/prod/version0_7/p0_eval/'
                 '230d7ae5b60054d124a1105b9a76bdf0783efb790c2150a5bcad55a012709295.pickle')

    test_file_2 = ('/Users/kasrarasaee/Development/quantumwake'
                   '/temp_processor_code/states/animallm/prod'
                   '/version0_4/initial_input'
                   '/38abf1b92a115db0b5c3b9a2534d7203405264bd0951e0d2c355021bfeb066f1.pickle')

    # TODO needs a sample pickle state instead of hardcoded value

    loaded_state = State.load_state(test_file_2)

    assert loaded_state.config.model_name is not None
