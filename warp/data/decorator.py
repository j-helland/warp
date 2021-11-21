# std
import functools
from abc import abstractmethod

# extern

# warp

# types
from typing import Tuple


class Decorator:
    """
    TODO
    """
    __cache__ = dict()

    def __init__(self, func):
        functools.update_wrapper(self, func)
        if isinstance(func, Decorator):
            func.__set_name__(self, 'nested')
            self.func = func.func
        else:
            self.func = func
        self.owner = None
    
    def __call__(self, *args, **kwargs):
        return self.func(self.owner, *args, **kwargs)
    
    # __set_name__ allows a decorator that can access the class attributes before any run() computations
    def __set_name__(self, owner, name):
        self.owner = owner

        key, attrs = self.get_attrs()

        # get the exterior module name
        module = self.owner.__module__
        if module not in self.__cache__:
            self.__cache__[module] = {key: attrs}
        else:
            self.__cache__[module][key] = attrs

        # handle the nested decorator case
        if not isinstance(owner, Decorator):
            self.owner.__attrs__[module] = self.__cache__[module]
            del self.__cache__[module]

    @abstractmethod
    def get_attrs(self) -> Tuple[str, dict]:
        pass
