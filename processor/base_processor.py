import hashlib
import json
import logging
import os
from typing import List, Any

import pandas as pd
from tenacity import wait_exponential, retry, wait_random

import utils


class BaseProcessor:

    def __init__(self, name: str, config: dict,
                 processors: List[Any] = None):

        #
        self.processors = processors

        self.state = {
            'header': {
                'name': name
            },
            'config': config,
            'data': list(),
            'mapping': dict(),
        }


    @property
    def header(self):
        return self.state['header']

    @header.setter
    def header(self, value):
        self.state['header'] = value

    @property
    def config(self):
        return self.state['config']

    @config.setter
    def config(self, value):
        self.state['config'] = value

    @property
    def name(self):
        return self.header['name']

    @name.setter
    def name(self, name: str):
        self.header['name'] = name

    @property
    def input_state_filename(self):
        return self.config['input_file'] \
            if 'input_file' in self.config \
            else self.config['input_state_filename']

    @input_state_filename.setter
    def input_state_filename(self, value):
        self.config['input_file'] = value

    @property
    def state_cache_path(self):
        return self.config['state_cache_path'] \
            if 'state_cache_path' in self.config \
            else '/tmp/states'

    @state_cache_path.setter
    def state_cache_path(self, path: str):
        self.config['state_cache_path'] = path

    @property
    def state_query_key_definition(self):
        return self.config['state_query_key_definition']

    def add_output_from_dict(self, query_state: dict):
        if len(self.output_dataframe) > 0:
            _append_query_state = pd.DataFrame([query_state])
            self.output_dataframe = pd.concat([self.output_dataframe, _append_query_state])
        else:
            self.output_dataframe = pd.DataFrame([query_state])

    def load_template(self, template_file: str):
        if not template_file or not os.path.exists(template_file):
            return None

        with open(template_file, 'r') as fio:
            template_dict = json.load(fio)

        # if template content exists then set it, it can be overwritten by a file
        if 'template_content' in template_dict:
            template_content = template_dict['template_content']
        else:
            template_content = None

        # if a template file exists then try and load it
        if 'template_content_file' in template_dict:

            # throw an exception if both the template_file and template keys are set
            if template_content:
                raise Exception(f'Cannot define a template_content and a template_content_file in the same '
                                f'template configuration {template_file}. For example, if you define the key '
                                f'"template_content" with some text content, you cannot also define a '
                                f'"template_content_file" which points to a different content from a file"')

            # otherwise load the template
            template_file = template_dict['template_content_file']
            with open(template_file, 'r') as fio_tc:
                template_content = fio_tc.read().strip()
                template_dict['template_content'] = template_content

        if not template_content:
            raise Exception(f'no template defined in template file {template_file} with configuration {template_dict}')

        return template_dict

    def get_hash_from_string(self, input_string: str):
        # Create a SHA-256 hash object
        hash_object = hashlib.sha256()

        # Update the hash object with the bytes of the string
        hash_object.update(input_string.encode())

        # Get the hexadecimal representation of the hash
        return hash_object.hexdigest()

    def extract_values_from_query_state_by_key_config_name(self, config_name: str, query_state: dict):
        # if the key config map does not exist then attempt
        # to use the 'query' key as the key value mapping
        if config_name not in self.config:
            if "query" not in query_state:
                raise Exception(f'query does not exist in query state {query_state}')

            return query_state['query']

        # basically a list of parameter names (or parameters by ( name = alias )
        # pairing to pick off data from the query state object
        key_definition = self.config[config_name]

        # iterate each parameter name and look it up in the state object
        results = {}
        for name in key_definition:

            alias = name
            # check to make sure that this is not a dict
            if isinstance(name, dict):
                # if dictionary then we must strip out the name and alias name
                # this is used to create a target dictionary from the source dictionary
                # but using a new alias, as defined by the config values
                #    'config_name':[{ 'query': 'value'}, ...]
                keyval = dict(name)
                if len(keyval) != 1:
                    raise Exception(
                        f'Invalid key value pair set for config_name {config_name}, values {keyval} is invalid, only a single key value pair is allowed when defining alias')

                # since there should only be 1 value, pop it and read it in
                keyval = keyval.popitem()
                name = keyval[0]  # the new name is no longer the dict but the key name in the dict
                alias = keyval[1]  # the alias is not the name but pulled from the dict key value

            # if it does not exist, throw an exception to warn the user that the state input is incorrect.
            if name not in query_state:
                raise Exception(f'Invalid state input for parameter: {name}, '
                                f'query_state: {query_state}, '
                                f'key definition: {key_definition}')

            # add a new key state value pair,
            # constructing a key defined by values from the query_state
            # given that the key exists and is defined in the config_name mapping
            results[alias] = query_state[name]

        return results

    def get_output_filename(self):
        return self.config['output_file'] \
            if 'output_file' in self.config \
            else None

    def batching(self, questions: List[str]):
        pass

    def has_query_state(self, query_state_key: str, force: bool = False):
        # make sure that the state is initialized and that there is a data key
        if not self.state or not self.state['mapping']:
            return None

        mapping = self.state['mapping']
        if not force and query_state_key in mapping:  # skip if not forced and state exists
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        # otherwise return none, which means no state exists
        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def __call__(self, *args, **kwargs):
        input_file = self.input_state_filename

        # identify whether we should dump each call result to an output csv file
        dump_on_every_call = False
        dump_on_every_call_output_filename = None
        if 'dump_on_every_call' in self.config:
            dump_on_every_call = bool(self.config['dump_on_every_call'])
            dump_on_every_call_output_filename = self.config['dump_on_every_call_output_filename'] \
                if 'dump_on_every_call_output_filename' in self.config \
                else None

            if not dump_on_every_call_output_filename:
                raise Exception(
                    'Cannot specify dump_on_every_call without a dump_on_every_call_output_filename output filename')

        # if the input is .json then make sure it is a state input
        if input_file.lower().endswith('.json'):
            input_state = utils.load_state(input_file)
            input_dataset = input_state['data']

            count = len(input_dataset)
            logging.info(f'starting processing loop with size {count}')
            for data_idx, data in enumerate(input_dataset):
                logging.info(f'processing dataset entry {data_idx} / {count}')
                self.call(values=data)

                if dump_on_every_call:
                    self.dump_dataframe_csv(dump_on_every_call_output_filename)

        else:
            raise NotImplementedError('Format type not implemented, states must be in json format with header and '
                                      'data keys, **please use a state template** to construct your input state or '
                                      'use a previous output state as an input state to your processor')

    def load_state(self):
        state_file = self.build_state_cache_filename()
        if not os.path.exists(state_file):
            return self.state

        with open(state_file, 'r') as fio:
            return json.load(fio)

    def save_state(self, key: str, query_state: dict, state_file: str = None):
        # fetch the state file name previously configured, or autogenerated
        state_file = state_file if state_file else self.build_state_cache_filename()

        # update the data states for the specific query / question / input
        data = self.state['data']
        data.append(query_state)

        # store a hashed value, not the best method I admit,
        # but it will do for now we are not dealing with massive data, yet
        # TODO IMPORTANT - PERFORMANCE AND STORAGE
        # TODO look into this potential performance and storage bottleneck,
        # TODO probably would benefit from a database backend instead
        # key = self.get_query_state_key(query_state)
        mapping = self.state['mapping']
        mapping[key] = query_state

        # write the state as json
        with open(state_file, 'w') as fio:
            json.dump(self.state, fio)

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
        data = self.state['mapping'] if 'mapping' in self.state else None

        if not data:
            error = f'no data mapping found in query state {query_state} of {self.config} for {self}'
            logging.error(error)
            raise Exception(error)

        self.add_output_from_dict(query_state=query_state)

    def dump_dataframe_csv(self, output_filename: str):
        # not safe for consistency
        self.output_dataframe.to_csv(output_filename)

    def dump_cache(self, output_filename: str):

        # safe for consistency
        output_filename = self.get_output_filename() if not output_filename else output_filename

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

        output_filename = self.get_output_filename() if not output_filename else output_filename

        # safe for consistency
        if not output_filename:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

        if not output_filename.lower().endswith('.csv'):
            raise Exception('Invalid filename specified for CSV export, please make sure it ends with .csv')

        if output_filename:
            data = self.state['data']
            df = pd.DataFrame(data)
            df.to_csv(output_filename)
        else:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

    def dump_cache_json(self, output_filename: str = None):
        output_filename = self.get_output_filename() if not output_filename else output_filename

        if not output_filename:
            logging.error(f'unable to persist to JSON output file, output_filename is not set')

        if not output_filename.lower().endswith('.json'):
            raise Exception('Invalid filename specified for JSON export, please make sure it ends with .json')

        with open(output_filename, 'w') as fio:
            json.dump(self.state, fio)

    def build_state_cache_filename(self):
        if not self.name:
            raise Exception(f'Processor name not defined, please ensure to define a unique processor name as part, otherwise your states might get overwritten or worse, merged.')

        state_file_hashed = self.get_hash_from_string(self.name)
        state_file = f'{self.state_cache_path}/{state_file_hashed}.json'
        return state_file

    def get_query_state_key(self, query_state: dict):
        state_values = self.extract_values_from_query_state_by_key_config_name(config_name='state_query_key_definition',
                                                                               query_state=query_state)

        keys = [(name, value) for name, value in state_values.items()]

        return self.get_hash_from_string(str(keys)), keys

    def call(self, values: dict, force: bool = False):
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

