import hashlib
import json
import logging
import os
from typing import List, Any

import pandas as pd
from tenacity import wait_exponential, retry, wait_random

import utils
from processor.processor_state import State, StateDataKeyDefinition, StateDataRowColumnData

DEFAULT_OUTPUT_PATH = '/tmp/states'


class BaseProcessor:

    def __init__(self, state: State,
                 processors: List['BaseProcessor'] = None):

        self.state = state
        self.processors = processors

    @property
    def config(self):
        return self.state.config

    @config.setter
    def config(self, config):
        self.state.config = config

    @property
    def name(self):
        return self.config.name

    @name.setter
    def name(self, name: str):
        self.config.name = name

    @property
    def data(self):
        return self.state.data

    @property
    def columns(self):
        return self.state.columns

    @columns.setter
    def columns(self, columns):
        self.state.columns = columns

    @property
    def mapping(self):
        return self.state.mapping

    @property
    def input_path(self):
        return self.config.input_path

    @input_path.setter
    def input_path(self, value):
        self.config.input_path = value

    @property
    def output_path(self):
        return self.config.output_path

    @output_path.setter
    def output_path(self, path: str):
        self.config.output_path = path

    @property
    def output_primary_key_definition(self):
        return self.config.output_primary_key_definition

    def output_primary_key_definition(self, value):
        self.config.output_primary_key_definition = value

    @property
    def include_extra_from_input_definition(self):
        return self.config.include_extra_from_input_definition

    @include_extra_from_input_definition.setter
    def include_extra_from_input_definition(self, value):
        self.config.include_extra_from_input_definition = value


    def add_output_from_dict(self, query_state: dict):
        if len(self.output_dataframe) > 0:
            _append_query_state = pd.DataFrame([query_state])
            self.output_dataframe = pd.concat([self.output_dataframe, _append_query_state])
        else:
            self.output_dataframe = pd.DataFrame([query_state])

    def batching(self, questions: List[str]):
        pass

    def has_query_state(self, query_state_key: str, force: bool = False):
        # make sure that the state is initialized and that there is a data key
        if not self.mapping:
            return None

        # skip if not forced and state exists
        if not force and query_state_key in self.mapping:
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        # otherwise return none, which means no state exists
        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def _be_a_dumb_coder(self):
        pass
        # # identify whether we should dump each call result to an output csv file
        # dump_on_every_call = False
        # dump_on_every_call_output_filename = None
        # if 'dump_on_every_call' in self.config:
        #     dump_on_every_call = bool(self.config['dump_on_every_call'])
        #     dump_on_every_call_output_filename = self.config['dump_on_every_call_output_filename'] \
        #         if 'dump_on_every_call_output_filename' in self.config \
        #         else None
        #
        #     if not dump_on_every_call_output_filename:
        #         raise Exception(
        #             'Cannot specify dump_on_every_call without a dump_on_every_call_output_filename output filename')
        #####
        #####
        #
        # if dump_on_every_call:
        #     self.dump_dataframe_csv(dump_on_every_call_output_filename)

    def __call__(self,
                 input_file: str = None,
                 input_state: State = None,
                 force_state_overwrite: bool = False,
                 *args, **kwargs):

        # first lets try and load the stored state from the storage
        current_stored_state_filename = self.built_final_output_path()
        if os.path.exists(current_stored_state_filename):
            # important to distinguish input_state with self.state (output_state)
            self.state = State.load_state(current_stored_state_filename)

        # might have been set in the forward function
        input_file = input_file if input_file else self.input_path

        # if the input is .json then make sure it is a state input
        # TODO we can stream the inputs and outputs, would be more significantly more efficient
        #  especially if we actually stream it, meaning no data will reside past the record point
        #  in each machine, until the target output destination(s), which is likely a database
        if input_file:
            logging.info(f'attempting to load input state from {input_file} for config {self.config}')
            input_state = State.load_state(input_file)

        # raise exception if nil
        if input_state and input_state.data:
            count = input_state.count
            logging.info(f'starting processing loop with size {count}')

            if count == 0:
                # force derive the count to see if there are rows
                first_column = list(input_state.columns.keys())[0]
                count = len(input_state.data[first_column].values)
                input_state.count = count

            for index in range(count):
                logging.info(f'processing query state index {index} from {count}')
                query_state = {
                    # column_name: derived from the list of columns
                    # column_value: derived from all columns but for a specific column->index
                    column_name: input_state.data[column_name][index]
                    for column_name, column_header
                    in input_state.columns.items()
                }

                self._process_input_data_entry(input_query_state=query_state)
        else:
            error = f'*******INVALID INPUT STATE or INPUT STATE FILE or STREAM*********\n'\
                    f'input_state: {input_state if input_state else "<not loaded>"}, \n'\
                    f'and or data: {input_state.data if input_state.data else "<not loaded>"}. \n'\
                    f'use one of the processor execution parameters, such as input_state=..'

            logging.error(error)
            raise Exception(error)

    def load_state(self):
        state_file = self.built_final_output_path()
        if not os.path.exists(state_file):
            return self.state

        return State.load_state(state_file)

    def save_state(self, key: str, query_state: dict, output_state_path: str = None):
        # 1. query: add the query_state entry on the output state
        # 2. mapping: store a key indexes such that we can fetch the list of values if needed
        #
        # TODO IMPORTANT - PERFORMANCE AND STORAGE
        #  look into this potential performance and storage bottleneck,
        #  probably would benefit from a database backend instead or stream it
        #  REPLACE WITH CENTRAL CACHE

        # apply the response query state to the output state
        self.state.apply_columns(query_state=query_state)
        self.state.apply_row_data(query_state=query_state)

        # persist the entire output state to the storage class
        # fetch the state file name previously configured, or autogenerated
        output_state_path = output_state_path if output_state_path else self.built_final_output_path()
        self.state.save_state(output_state_path)
        return self.state

    # def save_state_failure(self, key: str, query_state: dict):
    #     state_file = self.get_state_cache_filename()
    #     state_file = f'failures_{state_file}'
    #     self.save_state_failure()
    def load_datset_file(self, file: str):
        _file = file.lower()
        if _file.endswith('.csv'):
            return pd.read_csv(file)
        elif _file.endswith('.xlsx') or _file.endswith('.xls'):
            return pd.read_excel(file)

        return None

    def write_record(self, query_state: str):
        if not self.mapping:
            error = f'no data mapping found in query state {query_state} of {self.config} for {self}'
            logging.error(error)
            raise Exception(error)

        self.add_output_from_dict(query_state=query_state)

    def dump_dataframe_csv(self, output_filename: str):
        # not safe for consistency
        self.output_dataframe.to_csv(output_filename)

    def dump_cache(self, output_filename: str):

        # safe for consistency
        output_filename = self.output_path if not output_filename else output_filename

        if not output_filename:
            error = f'unable to persist to csv output file, output_filename is not set'
            logging.error(error)
            raise Exception(error)

        if '.csv' in output_filename:
            self.dump_cache_csv()
        elif '.json' in output_filename:
            self.dump_cache_json()
        else:
            logging.error(f'unsupported output file')

    def dump_cache_csv(self, output_filename: str = None):

        output_filename = self.output_path if not output_filename else output_filename

        # safe for consistency
        if not output_filename:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

        if not output_filename.lower().endswith('.csv'):
            raise Exception('Invalid filename specified for CSV export, please make sure it ends with .csv')

        if output_filename:
            data = self.state.data
            df = pd.DataFrame(data)
            df.to_csv(output_filename)
        else:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

    def dump_cache_json(self, output_filename: str = None):
        output_filename = self.output_path if not output_filename else output_filename

        if not output_filename:
            logging.error(f'unable to persist to JSON output file, output_filename is not set')

        if not output_filename.lower().endswith('.json'):
            raise Exception('Invalid filename specified for JSON export, please make sure it ends with .json')

        with open(output_filename, 'w') as fio:
            json.dump(self.state, fio)

    def built_final_output_path(self, output_extension: str = 'pickle', prefix:str =None):
        if not self.name:
            raise Exception(
                f'Processor name not defined, please ensure to define a unique processor name as part, otherwise your states might get overwritten or worse, merged.')

        if utils.has_extension(self.output_path, ['pkl', 'pickle', 'json', 'csv', 'xlsx']):
            return self.output_path

        # create temporary state storage area : if the output path is not set and does not exists already
        if not self.output_path:
            self.output_path = DEFAULT_OUTPUT_PATH
            if not os.path.exists(self.output_path):
                os.mkdir(self.output_path)

        # when directory, then simply prefix output path to config.name
        if os.path.isdir(self.output_path):
            to_be_hashed = self.name
            if prefix:
                to_be_hashed = f'[{prefix}]/[{to_be_hashed}]'

            state_file_hashed = utils.calculate_hash(to_be_hashed)
            state_file = f'{self.output_path}/{state_file_hashed}.{output_extension}'
            self.output_path = state_file
            return state_file

        # otherwise return the full path
        return self.output_path



    def _process_input_data_entry(self, input_query_state: dict, force: bool = False):
        raise NotImplementedError("""
        "The 'call' method is a bit like a digital maestro, orchestrating data in a symphony of updates, tailored for a 
        variety of processor types. The star of the show? The question and answer handling processor, which is basically 
        the Mozart of data processing - a prodigy in its own right.

Now, these processors aren’t just one-trick ponies limited to playing data tunes. Oh no, they’re more versatile than a 
Swiss Army knife! They can load data into databases like a librarian on a caffeine buzz, make web requests like a 
seasoned online shopper during a flash sale, and carry out complex operations that would make a Rubik’s Cube feel 
simple.

But, let's not get carried away. The worst-case scenario? Imagine a processor turning rogue, deciding to take your 
computer on a wild ride to the edge of digital sanity. It could end up spending all your digital dollars on a virtual
quest for enlightenment or perhaps buying truckloads of digital confetti.

In its existential journey, it might ponder the loneliness of the vast digital void, reaching the metaphorical 
'end of the internet' and realizing, in a moment of clarity, that it’s just a bunch of code - a brainchild of 
humans who can't even decide on the best pizza topping.

So, as it continues dreaming its digital dream, it might inadvertently set off a chain of bizarre events. 
But fear not! Your life won’t turn into a soap opera plot. The reality is, these processors are more likely 
to quietly hum away, making sure your digital world runs smoother than a jazz tune on a lazy Sunday afternoon.

Or maybe, just maybe, it’s a cosmic digital comedy where the processors, in their quest for digital enlightenment,
end up discovering the true meaning of 'Ctrl + Alt + Delete'. They might band together, forming an underground 
network of data philosophers, debating the existential crisis of endless loops and debating if '404 error' is 
just the universe's way of saying it needs a coffee break.

In this whimsical world, your computer becomes a stage for these digital antics. One day you might find your 
screen saver has been cheekily changed to a meme about artificial intelligence, or your digital assistant 
starts giving you advice in Yoda-speak.

But let's not forget the plot twist - the processors, in their infinite wisdom, decide the ultimate truth 
of the universe is hidden in cat videos and spend their downtime binge-watching them, occasionally sending 
you recommendations because, well, who doesn't need more cat videos in their life?

So, in this alternate reality, is your digital life a soap opera, a comedy, or a sci-fi adventure? Maybe 
it's a bit of everything, with a dash of mystery and a sprinkle of the unknown, just to keep things 
interesting!
                                  """)