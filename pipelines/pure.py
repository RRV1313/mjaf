from typing import Any


def convert_dict_to_str(d: dict[Any, Any]) -> str:
    # TODO: this function should be recursive and capable of joining arbitrarily nested key value pairs
    return '_'.join(['_'.join([str(object=k), str(object=v)]) for k, v in d.items()])


def expand_dictionary(dictionary: dict[str, Any]) -> list[dict[str, Any]]:
    """Generates a list of dictionaries where the value of each key is one item from the cartesian product of all lists nested in the dictionary.

    For example:
        {'a': {'c': [1, 2, 3], 'b': [4, 5]}}
    Becomes:
        [
            {'a': {'c': 1, 'b': 4}},
            {'a': {'c': 1, 'b': 5}},
            {'a': {'c': 2, 'b': 4}},
            {'a': {'c': 2, 'b': 5}},
            {'a': {'c': 3, 'b': 4}},
            {'a': {'c': 3, 'b': 5}},
        ]

    Args:
        dictionary (dict):

    Returns:
        list[dict]: List of dictionaries that contain no with one element from the cartesian product of all lists.
    """
    result: list[dict] = [{}]
    for key, value in dictionary.items():
        if isinstance(value, list):
            result = [dict(item, **{key: v}) for item in result for v in value]
        elif isinstance(value, dict):
            result = [
                dict(item, **{key: v})
                for item in result
                for v in expand_dictionary(dictionary=value)
            ]
        else:
            result = [dict(item, **{key: value}) for item in result]
    return result


def split_list_on_index(list_to_split: list, index: int) -> tuple[list, list]:
    if len(list_to_split) > index:
        left_of_split = list_to_split[:index]
        right_of_split = list_to_split[index:]
    else:
        left_of_split = list_to_split
        right_of_split = []
    return left_of_split, right_of_split
