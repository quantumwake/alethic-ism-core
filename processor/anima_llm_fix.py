
def anthropic_columns_pN2_8():
    ### **** IMPORTANT p(n)
    # anthropic
    state = State.load_state(
        '../states/animallm/prod/version0_1/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_provider_name",
            value=state.config.provider_name,
            data_type="str"
        ))

    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_model_name",
            value=state.config.model_name,
            data_type="str"
        ))
    state.save_state(state.config.output_path)  ## persist the state again

def openai_columns_pN2_8():

    # openai
    state = State.load_state(
        '../states/animallm/prod/version0_1/49717023a01b090af5315b23ed38bef5143acb3887b54d9fd2155da18bd2144e.pickle')
    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_provider_name",
            value=state.config.provider_name,
            data_type="str"
        ))

    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_model_name",
            value=state.config.model_name,
            data_type="str"
        ))
    state.save_state(state.config.output_path) ## persist the state again


def p0_columns(files: List[str]):
    # p0_files = [
    #     # P0 claude-2.1  - template version 0.2                 (2023-11-29 00:53:09.657690)
    #     '../states/animallm/prod/version0_2/68e8ae8bd3df94b64d2412f18e73e78f8fcf92d4abe69d22856225965e330b2c.pickle',
    #
    #     # P0 openai gpt4-1106-preview - template version 0.2    (2023-11-28 22:53:49.545835)
    #     '../states/animallm/prod/version0_2/de76389d42d0e3eaf97744c4da6b1e195780c7f95f1b4fa306f86b162f333140.pickle',
    #
    #     # P0 claude-2.1 version 0.2                             (2023-11-28 22:31:23.330755)        X
    #     '../states/animallm/prod/version0_2/f40b86a2231fb932ab2804f3052fa7005e7c11540d259a0ee798cb8af0b302d6.pickle',
    #
    #     # P0 claude-2.0 version 0.2                             (2023-11-28 23:08:16.431890)
    #     '../states/animallm/prod/version0_2/d9aac084cea5719043c8293081e677adecb6b8f1f99abf53284faf379fe0ecc8.pickle'
    # ]

    ### ****** IMPORTANT (P0)
    ## need to aadd the following additional columns to the datasets

    for file in files:
        state = State.load_state(file)
        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="perspective_index",
                value="P0",
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="perspective",
                value="Animal",
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="justification",
                value="N/A",
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="response_provider_name",
                value=state.config.provider_name,
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="response_model_name",
                value=state.config.model_name,
                data_type="str"
            ))

        state.save_state(state.config.output_path)  ## persist the state again

def p1_columns(files: List[str]):
    # p1_files = [
    #     # P1 claude-2.1  - template version 0.2                 (2023-11-29 01:19:39.990534)
    #     '../states/animallm/prod/version0_2/ec508ce9d0567d95057bbca6ecfabd61ef9fcc05bc905529c6a7444e73bdd70a.pickle',
    #
    #     # P1 openai gpt4-1106-preview - template version 0.2    (2023-11-29 00:51:21.819819)
    #     '../states/animallm/prod/version0_2/c5b058322bebf71b2a5acfbcf964e3ae0f74d048ccff8639025aba05303002d3.pickle',
    #
    #     # P1 claude-2.0 version 0.2                             (2023-11-29 01:41:43.881073)
    #     '../states/animallm/prod/version0_2/b73eb301635dd1c17b9ded221ed064b6be129c410c71eb352b841d11a6006cf5.pickle'
    # ]

    for file in files:
        state = State.load_state(file)
        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="perspective_index",
                value="P1",
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="perspective",
                value="Default",
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="response_provider_name",
                value=state.config.provider_name,
                data_type="str"
            ))

        state = add_column_value_constant_to_state(
            state=state,
            column=StateDataColumnDefinition(
                name="response_model_name",
                value=state.config.model_name,
                data_type="str"
            ))

        state.save_state(state.config.output_path)  ## persist the state again


## TEST
if __name__ == '__main__':
    log.basicConfig(level="DEBUG")
    display_state_information('../states/animallm/prod/version0_4/p0', name_filter="P0")

    p0_files = find_states('../states/animallm/prod/version0_4/p0', name_filter="P0")
    p0_columns(p0_files)

    # search
    found = find_states('../states/animallm/prod/version0_2', name_filter='P(n)')
    found_state_data = search_state(found[0], filter_func=lambda x: x)
    found_state_data_df = query_states_to_excel(output_excel_path='../excel/test_data/test.xlsx',
                                                query_states=found_state_data)

    print(found)

    # print(p0_files)
    # anthropic_columns_pN2_8()     # ran it already on the p0_files
    # openai_columns_pN2_8()        # ran ....
    # p0_columns()                  # ran ...
    # p1_columns()                  # ran ..

    # state = State.load_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    #
    # print(state.config.name)
    # print(state.config.output_path)
    # print(state.count)
    # state.config.name = 'AnimaLLM Instruction for Query Response Perspective P(n)'
    # state.config.output_path = '../states/animallm/prod'
    # state.save_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    #
    # p = AnthropicQuestionAnswerProcessor(state=state)
    # print(p.build_final_output_path())
    # state.save_state(p.build_final_output_path())


