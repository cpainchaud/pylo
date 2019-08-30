import json


def nice_json(json_obj):
    return json.dumps(json_obj, indent=2, sort_keys=True)


def string_list_to_text(string_list, separator=None):
    """

    :type string_list: List[str]
    :type separator: str
    """
    msg = ""
    first = True
    for stringItem in string_list:
        if not first:
            if separator is None:
                msg += ","
            else:
                msg += separator
        first = False
        msg += stringItem

    return msg


