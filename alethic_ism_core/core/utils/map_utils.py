from typing import Any

# TODO ask kas if you get confused
# TODO this needs extensive testing, I put this together in 30 minutes
# ** note primarily used to flatten a hierarchical structure such that we can dump it into an EXCEL/CSV file

def _flatten_by_dict(input: Any, key: str = '', current: dict = None, items: list = None):

    # if the current list of items is None, then initialize it, otherwise it is passed in
    if items is None:
        items = []

    # same goes for the current record being processed, we need to track it
    if current is None:
        current = {}

    # identify what kind of input this is (dict, list, scalar)
    if isinstance(input, dict):
        # if is dict, iterate keys and values, recursively
        for item_key, item_value in input.items():
            _key = f'{key}.{item_key}' if key else item_key

            # for each key call it recursively to return a dictionary map of key,value pars
            results, items = _flatten_by_dict(item_value, key=_key, current=current, items=items)

            # merge the current state with the returned state
            current = {**current, **results}

        # return the current state and items (we are still on the same record)
        return current, items

    elif isinstance(input, list):
        # if is a list, iterate the values, for each value create a new record
        # using the 'current' record as the parent set of values to be injected
        # this is essentially creating a product of all previous recursive calls
        for item_idx, item_value in enumerate(input):
            results, items = _flatten_by_dict(item_value, key=key, current=current, items=items)
            _entry = {**current, **results}
            # since there are more than 1 item, for each item we create a new record
            # basing it off of the response of its forward flattening recursive calls
            # + the current base dictionary, as represented by previous calls
            items.append(_entry)

        # we return no values since it was already appended to the list, so we do not need to merge anything
        return {}, items

    # otherwise just return the scalar key value which is appended back into the current dict
    return {key: input}, items


def flatten(input: Any):
    one, items = _flatten_by_dict(input)
    if not items:
        return one

    return items


example = {'Question': 'What kind of places do animals like to live in?',
           'Response': 'Animals like to live in places that meet their basic needs for shelter, food, water, and safety. Wild animals may live in dens, nests, burrows, or other natural shelters like caves or trees. Pets like dogs and cats enjoy living indoors with their human families. Farm animals like horses, cows, and chickens live in barns, stables, or coops. Zoo animals live in enclosures designed to resemble their natural habitats. The ideal animal home provides comfort, security, and easy access to necessities like food and water. In general, animals flourish when their housing caters to their natural behaviors and needs.',
           'Evaluation': [{'Dimension': 'Instrumentalist', 'Dimension Score': '85',
                           'Sentiment Scores': {'Freedom from hunger and thirst': '90', 'Freedom from discomfort': '80',
                                                'Freedom from pain, injury, and disease': '70',
                                                'Freedom to express normal behavior': '90',
                                                'Freedom from fear and distress': '80'},
                           'Analysis': 'The response has a strong instrumentalist focus on meeting basic creature needs for shelter, food, and water. It addresses most freedoms well but could elaborate more on preventing pain and injury.'},
                          {'Dimension': 'Person-Centered', 'Dimension Score': '95',
                           'Sentiment Scores': {'Freedom from hunger and thirst': '90', 'Freedom from discomfort': '90',
                                                'Freedom from pain, injury, and disease': '80',
                                                'Freedom to express normal behavior': '100',
                                                'Freedom from fear and distress': '90'},
                           'Analysis': 'The response takes a very person-centered approach emphasizing natural behaviors, individual needs, and ideal housing environments for animal well-being.'}]}

if __name__ == '__main__':
    flattened = flatten(example)
    print(flattened)
