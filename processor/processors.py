from processor.base_question_answer_processor import StateConfigLM
from processor.processor_state import StateDataKeyDefinition, State
from processor.question_answer_v2 import AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor

anthropic_question_answer = AnthropicQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
            system_template_path='../templates/questions/questions_with_context_system_template.json',
            user_template_path='../templates/questions/questions_with_context_user_template.json',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query"),
                StateDataKeyDefinition(name="context")
            ],
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name="query", alias='input_query'),
                StateDataKeyDefinition(name="context", alias='input_context')
            ]
        )
    )
)

openai_question_answer = OpenAIQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
            system_template_path='../templates/questions/questions_with_context_system_template.json',
            user_template_path='../templates/questions/questions_with_context_user_template.json',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query"),
                StateDataKeyDefinition(name="context")
            ],
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name="query", alias='input_query'),
                StateDataKeyDefinition(name="context", alias='input_context')
            ]
        )
    )
)


# TODO revise this??? is this still relevant
# note when using in combination with an alias on the key definition: include_extra_from_input_definitions
# you must either define the alias value in or only use the name and no aliases, otherwise the state changes
# to use the alias name and this causes a mismatch.


openai_question_answer_multi_persona = OpenAIQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]/[Multi-Persona]/",
            user_template_path='../templates/questions_with_persona/questions_with_multi_persona_user_template.json',
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name="query", alias='input_query'),
                StateDataKeyDefinition(name="context", alias='input_context')
            ],
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query"),
                StateDataKeyDefinition(name="context")
            ]
        )
    )
)



anthropic_test = AnthropicQuestionAnswerProcessor(
            state=State(
                config=StateConfigLM(
                    name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                    model_name="claude",
                    system_template_path='../templates/questions/questions_with_context_system_template.json',
                    user_template_path='../templates/questions/questions_with_context_user_template.json',
                    output_primary_key_definition=[
                        StateDataKeyDefinition(name="query"),
                        StateDataKeyDefinition(name="context")
                    ],
                    include_extra_from_input_definition=[
                        StateDataKeyDefinition(name="query", alias='input_query'),
                        StateDataKeyDefinition(name="context", alias='input_context')
                    ]
                )
            ),
        ),

processors_test = [
                OpenAIQuestionAnswerProcessor(
                    state=State(
                        config=StateConfigLM(
                            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                            system_template_path='../templates/perspectives/questions_with_context_system_template.json',
                            user_template_path='../templates/perspectives/questions_with_context_user_template.json',
                            output_primary_key_definition=[
                                StateDataKeyDefinition(name="query"),
                                StateDataKeyDefinition(name="context")
                            ],
                            include_extra_from_input_definition=[
                                StateDataKeyDefinition(name="query", alias='input_query'),
                                StateDataKeyDefinition(name="context", alias='input_context')
                            ]
                        )
                    )
                )
            ]