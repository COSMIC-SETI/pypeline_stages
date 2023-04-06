import os
import shutil

def env_str_to_dict(env_value):
    env_dict = {}
    if env_value is None:
        return env_dict

    for variablevalues in env_value.split(" "):
        if "=" in variablevalues:
            pair = variablevalues.split("=")
            env_dict[pair[0]] = pair[1]
    return env_dict

def directory_exists(dirpath):
    return os.path.exists(dirpath) and os.path.isdir(dirpath)

def makedirs(dirpath, user=None, group=None, mode=None, exist_ok=False):
    if directory_exists(dirpath):
        if not exist_ok:
            raise FileExistsError(dirpath)
        return

    sub_directories_to_make = []
    existing_root = dirpath
    while True:
        existing_root, subdir = os.path.split(existing_root)
        sub_directories_to_make.insert(0, subdir)
        if directory_exists(existing_root):
            break

    try:
        os.makedirs(dirpath, exist_ok=False)
    except FileExistsError as err:
        # directories are made since last looking
        if not exist_ok:
            raise err
        return

    for subdir in sub_directories_to_make:
        existing_root = os.path.join(existing_root, subdir)

        if user is not None or group is not None:
            shutil.chown(existing_root, user=user, group=group)
        if mode is not None:
            os.chmod(existing_root, mode=mode)