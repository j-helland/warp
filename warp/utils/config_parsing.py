# std
import datetime
from copy import deepcopy
from collections import deque
import yaml

# from .lazy_loader import LazyLoader as LL
# yaml = LL('yaml', globals(), 'yaml')
# json = LL('json', globals(), 'json')

# types
from typing import Dict, Any, Union, Tuple


__all__ = [
    'load_config_file', 
    'save_config']


BASIC_TYPES: Tuple[type, ...] = (
    type(None), 
    bool, 
    int, 
    float, 
    str, 
    datetime.datetime, 
    bytes, 
    complex)
ITERABLE_TYPES: Tuple[type, ...] = (
    list, 
    tuple, 
    set,
    dict)


class HyperParameter:
    verbose = False
    @classmethod
    def set_verbosity(cls, value):
        cls.verbose = value
    
    def __init__(self, values=None, spec_type=None, spec=None):
        
        # Default version is to provide a list of actual values
        if values and type(values) is not list:
            raise TypeError(f'hyperparameter values must be a list not {type(values)}')
        
        if values:
            if not isinstance(values[0],dict) and not isinstance(values[0],list):
                values = sorted(set(values))
                if self.verbose: print('Found literal (unique) hparam values: ',values)
            elif len(values)==1 and isinstance(values[0],dict):
                raise TypeError(f'known bug/unsupported, hparam len(values)==1 but elm is a dict')
            else:
                # values = sorted(values)
                if self.verbose: print('Found literal hparam values: ',values)

        # Can support other value shorthands/generators
        if values is None:
            # A simple count or range(n) type
            if spec_type == 'int':
                values = [i for i in range(spec)]
            else:
                raise TypeError(f'no generator for hyperparameter spec.type: {spec_type}')
            # Could add another range type with low, high, stepsize... etc

            if self.verbose: print('Found constructable hparam values: ',values)
                    
        self.values = values


def set_value(dictionary, keychain, value):
    if len(keychain) == 1:
        dictionary[keychain[0]] = value
        return  
    set_value(dictionary[keychain[0]],keychain[1:],value)
    return dictionary


class BFTreeExpander:
    roots = {}
    # hparam_keys = set()
    # hparam_keychains = set()
    hparam_keychains = {}

    @classmethod
    def reset_roots(cls):
        cls.roots = {}
    @classmethod
    def get_roots(cls):
        return [v.root for k,v in cls.roots.items()]
    @classmethod
    def reset_keys(cls):
        # cls.hparam_keys = set()
        # cls.hparam_keychains = set()
        cls.hparam_keychains = {}
    # @classmethod
    # def get_hparam_key_list(cls):
    #     return list(cls.hparam_keys)
    @classmethod
    def get_hparam_keychains(cls):
        return list(cls.hparam_keychains.keys())
        # return cls.hparam_keychains
    
    def __init__(self, root):

        self.root = root
        self.queue = deque()
        self.id = id(self)
        self.roots[self.id] = self
    
    # recursive traverser
    def expand(self, node = None, keychain = []):
        if node is None: node = self.root

        if isinstance(node, HyperParameter): 
            # self.hparam_keys.add(keychain[-1])
            # self.hparam_keychains.add(".".join(keychain[1:])) # drop root key
            self.hparam_keychains[".".join(keychain[1:])] = None
            
            if len(node.values) == 1:
                set_value(self.root,keychain,node.values[0])
                return False
            else:
                for val in node.values: 
                                
                    new_root = set_value(deepcopy(self.root),keychain,val)
                    new_tree = BFTreeExpander(new_root) 

                return True # "expansion was performed"

        if isinstance(node, dict):
            for key,val in node.items():
                if val is not None:
                    new_keychain = keychain.copy()
                    new_keychain.append(key)
                    self.queue.append((val, new_keychain))

        while len(self.queue) > 0:
            next_node, next_keychain = self.queue.popleft()
            expanded = self.expand(next_node, next_keychain)
            
            if expanded:
                # since we had to expand this tree further, 
                # we can now remove it from the working set
                # pop w/ default None, instead of del, as this can get called repeatedly on way up
                self.roots.pop(self.id, None) 
                return True # bubble up
            
        return False # no expansion performed


def expand_config(orig_config):
    
    old_roots = [{'root': orig_config}]
    
    while True:
        old_ct = len(old_roots)
        new_roots = []
        
        for input_root in old_roots:

            BFTreeExpander.reset_roots()
            
            bfte = BFTreeExpander(input_root)
            bfte.expand()
            
            new_roots.extend(bfte.get_roots())

        if old_ct == len(new_roots):
            break

        old_roots = new_roots.copy()
        
    roots, keychains = [tree['root'] for tree in new_roots], BFTreeExpander.get_hparam_keychains() 
    BFTreeExpander.reset_roots()
    BFTreeExpander.reset_keys()
    
    return roots, keychains 


############ PyYAML Custom obj constructors/representers ###############     
def hparam_constructor(loader, node):
    fields = loader.construct_mapping(node, deep=True)
    hparam = HyperParameter(**fields)
    yield hparam

def tuple_to_list_constructor(loader, node):
        return list(loader.construct_sequence(node, deep=True))

def hparam_representer(dumper, node):
        return dumper.represent_mapping(u'!HYPERPARAMETER', [("values",node.values)], flow_style=False ) 


# def load_config_file(path: str) -> Dict[str, Any]:
def load_config_file(path: str) -> Tuple[list, list]:
    """Load a YAML file into a dict.
    Extensions accepted are `{.yml, .yaml}`.

    Arguments:
        path: The relative path to the YAML file to load.

    Returns:
        A dict version of the YAML file.
    """
    yaml.add_constructor('!HYPERPARAMETER', hparam_constructor, yaml.FullLoader)
    yaml.add_representer(HyperParameter, hparam_representer)
    # HyperParameter.set_verbosity(args.verbose)

    file_ext = path.split('.')[-1]
    if file_ext in {'yml', 'yaml'}:
        with open(path, 'rb') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
    else:
        raise NotImplementedError('unrecognized file extension .{:s} for file {:s}'.format(file_ext, path))

    # expanded_set, keychains = expand_config(config)
    return expand_config(config)
    # return config


def typecheck_config(config: Dict[str, Any]) -> None:
    invalid_types = set()
    def recursive_typecheck(struct: Union[Dict[str, Any], Any]) -> bool:
        # Recurse through iterables
        if isinstance(struct, ITERABLE_TYPES):
            if isinstance(struct, dict):
                return all(map(recursive_typecheck, struct.values()))
            return all(map(recursive_typecheck, struct))

        # Check against allowed types. Aggregate any found violations.
        else:
            if not isinstance(struct, BASIC_TYPES):
                invalid_types.add(type(struct))
                return False
            return True
    
    if not recursive_typecheck(config):
        raise TypeError(f'config {config} contains invalid type(s) {invalid_types}')


def save_config(path: str, config: Dict[str, Any]) -> None:
    try:
        typecheck_config(config)
    except TypeError as e:
        raise RuntimeError( [e, RuntimeError('Cannot cache runtime parameter values due to invalid type(s).')] )

    # cache
    with open(path, 'w') as file:
        yaml.dump(config, file, default_flow_style=False)
