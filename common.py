
def env_str_to_dict(env_value):
    env_dict = {}
    if env_value is None:
        return env_dict

    for variablevalues in env_value.split(" "):
        if "=" in variablevalues:
            pair = variablevalues.split("=")
            env_dict[pair[0]] = pair[1]
        else:
            env_dict[variablevalues] = None
    return env_dict