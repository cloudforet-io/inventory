def add_domain_id(query, domain_id: str):
    q = {'k': 'domain_id', 'v': domain_id, 'o': 'eq'}
    query.append(q)
    return query


def add_k_v_eq(query, k, v):
    q = {'k': k, 'v': v, 'o': 'eq'}
    query.append(q)
    return query


def find_data(dic: dict, key: str) -> str:
    """
    find hierarchy data
    :param dic:
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
    if not isinstance(dic, dict) or not isinstance(key, str):
        return None

    key_parsed = key.split('.', 1)
    if len(key_parsed) > 1:
        return find_data(dic.get(key_parsed[0], None), key_parsed[1])
    else:
        return dic.get(key_parsed[0], None)


def dict_key_int_parser(dic: dict):
    # change key to int type, if all keys are able to cast to int type. Otherwise, leave it as original type.
    try:
        dic = {int(k): v for k, v in dic.items()}
    except Exception as e:
        pass
    return dic


def make_query(key, rules, resource, domain_id):
    query = []
    query = add_domain_id(query, domain_id)
    for rule in rules[key]:
        v = find_data(resource, rule)
        if v:
            query = add_k_v_eq(query, rule, v)

    query = {'filter': query}
    return query
