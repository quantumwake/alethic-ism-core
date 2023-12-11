import os
from typing import List, Dict

import pandas as pd

def query_states_to_excel(output_excel_path: str,
                          query_states: List[Dict]):
    # Convert query states to a dataframe and then save it as an excel file
    df = pd.DataFrame(query_states)
    path = os.path.dirname(output_excel_path)
    os.makedirs(path, exist_ok=True)  # create dir recursively
    df.to_excel(output_excel_path, index=False)
    return df
