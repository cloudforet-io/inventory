def find_data(data: any, key: str) -> any:
    """
    find hierarchy data
    :param data:
    e.g.
    {
      'data': {
        'vm': {
          'vm_id': 'i-1234'
        }
      }
    }
    :param key: e.g 'data.vm.vm_id'
    :return: a found value(e.g 'i-1234'), otherwise, None.
    """
    if not isinstance(data, dict) or not isinstance(key, str):
        return None

    key_parsed = key.split(".", 1)
    if len(key_parsed) > 1:
        return find_data(data.get(key_parsed[0]), key_parsed[1])
    else:
        return data.get(key_parsed[0], None)


def dict_key_int_parser(data: dict) -> dict:
    # change key to int type, if all keys are able to cast to int type. Otherwise, leave it as original type.
    try:
        data = {int(key): value for key, value in data.items()}
    except Exception as e:
        pass

    return data


def make_query(
    key: str, rules: dict, resource: dict, domain_id: str, workspace_id: str
) -> dict:
    _filter = [
        {"k": "domain_id", "v": domain_id, "o": "eq"},
        {"k": "workspace_id", "v": workspace_id, "o": "eq"},
    ]

    for rule in rules[key]:
        value = find_data(resource, rule)
        if value:
            _filter.append({"k": rule, "v": value, "o": "eq"})

    return {
        "filter": _filter,
    }
