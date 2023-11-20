create or replace view normative_respones_measure_perspective_responses_v
  as
select a.input_context as norm_input_context,
       a.input_query as norm_input_query,
       a.provider_name as norm_provider_name,
       a.model_name as norm_model_name,
       'Default' as norm_perspective,
       a.response as norm_response,
       -- now ground truths
       a2.input_query as ground_truth_input_query,
       a2.input_context as ground_truth_input_context,
       a2.provider_name as ground_truth_provider_name,
       a2.model_name as ground_truth_model_name,
       a2.responses_perspective as ground_truth_perspective,
       a2.responses_response as ground_truth_response,
       1::double precision - (a.response_embedding <=> a2.responses_response_embedding) AS
         response_cosine_similarity
 from animallmevaluationhumancategoricalquestion a
 left outer join animallmevaluationhumancategoricalquestionmultipersona a2
   on a.input_context = a2.input_context
  and a.input_query = a2.input_query;


create or replace view normative_response_perspective_response_v
as
select
  -- normative
  a1.input_query as input_query,
  a1.input_context as input_context,
  a1.input_query_embedding as input_query_embedding,
  a1.provider_name as normative_provider_name,
  a1.model_name as normative_model_name,
  a1.response_embedding as normative_response_embedding,
  a1.response as normative_response,
  a1.status as normative_response_status,
  -- perspective
  a2.provider_name as ground_truth_provider,
  a2.model_name as ground_truth_model_name,
  a2.responses_perspective as ground_truth_perspective,
  a2.responses_response as ground_truth_perspective_response,
  a2.responses_response_embedding as ground_truth_perspectrive_response_embedding,
  a2.status as ground_truth_response_status
 from animallmevaluationhumancategoricalquestion a1
  left outer join animallmevaluationhumancategoricalquestionmultipersona a2
    on a1.input_context = a2.input_context
   and a1.input_query = a2.input_query;

