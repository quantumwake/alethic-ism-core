from ismcore.compiler.runnable import BaseRunnable
from ismcore.model.processor_state import State, StateConfig

code = """
class Runnable(BaseRunnable):

    # initialize any variables here
    def init(self, **kwargs):
        pass
    

    # Function to call /api/v1/query/:state
    def call_api_query(self, state_id: str, user_id: str, filters: List[Dict]) -> Dict:
        print("something happening 1")
        base_url = "https://api.ism.quantumwake.io"
        session = requests.Session()
        
        url = f"{base_url}/api/v1/query/{state_id}"

        # Construct the request payload
        payload = {
            "filter_groups": [
                {
                    "filters": filters,
                    "group_logic": "AND"
                }
            ],
            "state_id": state_id,
            "user_id": user_id
        }

        # Send the POST request
        print("something happening 2")
        response = self.session.post(url, json=payload)

        # Handle the response
        if response.status_code == 200:
            return response.json()  # Return JSON if successful
        else:
            return None
            # response.raise_for_status()  # Raise an error for unsuccessful requests

    # if this instruction is connected to an output state that happens to be of type stream (StateConfig)
    def process_query_states(self, query_states: List[Dict]) -> List[Dict]:
        output_states = []
    
        state_id = "4910bc34-41ce-4425-82e1-a7f2cc20be61"
        user_id = "77c17315-3013-5bb8-8c42-32c28618101f"
            
        for query_state in query_states:
            # Define the filters
            filters = [
                {
                    "column": "instruction",
                    "operator": "like",
                    "value": "%problematic%"
                },
                {
                    "column": "animal",
                    "operator": "=",
                    "value": "cat"
                }
            ]
            results = self.call_api_query(state_id=state_id, user_id=user_id, filters=filters)
            #count = len(results) if results else 0

            # add a new column named 'working' with value = "true" it can also be primitive
            query_state['working'] = "true"
            

            # append the outputs to the output state which will be returned
            output_states.append(query_state)
    
        # returns the new output states such that it can be passed in as an input to the next process
        return output_states

    # if this instruction is connected to an output state that happens to be of type stream (StateConfigStream)
    def process_stream(self, query_state: Any):
        # the data that is passed in is simply yielded to the stream 
        # (e.g. a websocket on the egress of an output state that happens to be a state stream config)
        yield json.dumps(query_state, indent=2)
"""

def test_compiler_code():
    state = State(
        id="1",
        config=StateConfig(

        )
    )

    r = BaseRunnable(state, None)
    compiled_code = r.instantiate(code=code)
    print(compiled_code)
