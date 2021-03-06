import os
import inspect
import hashlib
import datetime

from .lazy_loader import LazyLoader as LL
yaml = LL('yaml', globals(), 'yaml')
json = LL('json', globals(), 'json')

# types
from typing import Tuple, Set, Dict, Union, Type, Any


__all__ = [
    'recursive_update',
    'load_config_file', 
    'save_config',
    'unpickle', 
    'pickle', 
    'hash_path', 
    'function_call_stack']


def recursive_update(default  :Dict[str, Any], custom  :Dict[str, Any]) -> Dict[str, Any]:
    """Recursively update nested dict `default` with the values of nested dict `custom`.
    Values present in `custom` but not present in `default` will be added to `default`.

    NOTE: this function operates inplace in `default`.

    Arguments:
        default: The dict whose values will be updated by the matching values of `custom`.

        custom: The dict whose values will update the matching values of `default`.

    Returns:
        Updated version of `default` containing the values of `custom.`
    """
    for k in custom:
        if isinstance(custom[k], dict) and isinstance(default.get(k), dict):
            default[k] = recursive_update(default[k], custom[k])
        else:
            default[k] = custom[k]

    return default


def load_config_file(path: str) -> Dict[str, Any]:
    """Load a YAML file into a dict.
    Extensions accepted are `{.yml, .yaml}`.

    Arguments:
        path: The relative path to the YAML file to load.

    Returns:
        A dict version of the YAML file.
    """
    file_ext = path.split('.')[-1]
    if file_ext in {'yml', 'yaml'}:
        with open(path, 'rb') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
    elif file_ext == 'json':
        config = json.loads(path)
    else:
        raise NotImplementedError('unrecognized file extension .{:s} for file {:s}'.format(file_ext, path))
    return config


def typecheck_config(config: Dict[str, Any]) -> None:
    # TODO: allowed types in constants?
    basic_types: Tuple[type] = (
        type(None), 
        bool, 
        int, 
        float, 
        str, 
        datetime.datetime, 
        bytes, 
        complex)
    itr_types: Tuple[type] = (
        list, 
        tuple, 
        set,
        dict)

    def has_type_in(o: Any, types: Tuple[Type[Any]]) -> bool:
        return isinstance(o, types)
    
    def recursive_typecheck(struct: Union[Dict[str, Any], Any]) -> bool:
        if has_type_in(struct, itr_types):
            if isinstance(struct, dict):
                return all(map(recursive_typecheck, struct.values()))
            return all(map(recursive_typecheck, struct))
        else:
            return has_type_in(struct, basic_types)
    
    # TODO: more helpful error message with precise info about which nested members violate allowed types
    if not recursive_typecheck(config):
        raise TypeError(f'config {config} contains disallowed type(s)')


def save_config(path: str, config: Dict[str, Any]) -> None:
    try:
        typecheck_config(config)
    except TypeError as e:
        raise RuntimeError( [e, RuntimeError('Cannot cache runtime parameter values due to type checking violation.')] )

    # cache
    with open(path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)


def unpickle(path  :str) -> Any:
    """Function that loads a pickle file generated by [`pickle(path)`](./#pickle).

    Arguments:
        path: File path to a valid pickle file.

    Returns:
        The unpickled object.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    import pickle as pkl
    with open(path, 'rb') as in_file:
        obj = pkl.load(in_file)
    return obj


def pickle(path  :str, value  :Any) -> None:
    """Function that pickles an object to the location specified by `path`.

    Arguments:
        path: The file path for the pickle file that will be created.
        value: The object to pickle.
    """
    import pickle as pkl
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as handle:
        pkl.dump(value, handle)


def hash_path(path: str) -> str:
    """Creates a UTF-8 SHA1 hash of an input string.

    Arguments:
        path: The string value to hash.

    Return:
        UTF-8 SHA1 hash of `path`.
    """
    hash = hashlib.sha1(path.encode('utf-8')).hexdigest()
    return hash


def function_call_stack() -> Set[str]:
    """Returns the function call stack context.
    source: https://stackoverflow.com/a/2654130
    """
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    funcs = set(map(lambda x: x.function, calframe))
    return funcs
