import numpy as np
import pandas as pd
import os
import json

class ColumnContextRowQuestionToStateConverter:

    def __init__(self):
        pass

    def load_data_from_excel(self, filename: str):
        # Check if the file exists
        if not os.path.exists(filename):
            raise Exception(f'Excel file {filename} not found.')

        # Read the Excel file into a DataFrame, skipping the first row (which contains comments)
        df = pd.read_excel(filename, header=1)

        # Extract the data as a 2D numpy array
        data = df.values

        # The column names (contexts) are now correctly set as headers in the DataFrame
        context_headers = df.columns
        mappings = []

        # Iterate over each row to create mappings of context to query
        for row in data:
            for col_idx, query in enumerate(row):
                context = context_headers[col_idx]

                if query is not np.nan:
                    mappings.append({
                        'context': context,
                        'query': query
                    })

        return mappings

    def convert(self, intput_filename: str, output_filename: str):

        if os.path.exists(output_filename):
            raise Exception(f'output filename {output_filename} already exists, '
                            f'you can safely remove this file if you intend to recreate the output '
                            f'state from the input file (e.g. a @sg defined input structure')

        mapping = self.load_data_from_excel(input_filename)

        state = {
            "header": {
                "type": "manual",
                "description": f"converted using {self} on "
                               f"input file {input_filename} to "
                               f"output filename {output_filename}"
            },
            "data": mapping
        }

        with open(output_filename, 'w') as fio:
            json.dump(state, fio)

        return state


# main function
if __name__ == '__main__':
    input_filename = '../../dataset/examples/processor/vetted_questions/sg/questions_4gs.xlsx'
    output_filename = '../../dataset/examples/processor/vetted_questions/sg/questions_4gs.json'

    converter = ColumnContextRowQuestionToStateConverter()
    converter.convert(input_filename, output_filename)