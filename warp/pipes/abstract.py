import warp
import warp.utils as utils

# types
from typing import Optional, Union, Set, Any, Type

SelfType = object


__all__ = ['AbstractPipeObject']


class AbstractPipeObject:
    """
    TODO
    """
    __slots__ = (
        'value', 
        'value_type', 
        'return_instance_func_set')

    def __init__(
        self,
        value:                     Any                 =None, 
        value_type:                Type[Any]           =type(None), 
        return_instance_func_set:  Optional[Set[str]]  =None
    ):
        self.value: Any = value
        self.value_type: Type[Any] = value_type

        if return_instance_func_set is not None:
            self.return_instance_func_set = return_instance_func_set
        else:
            self.return_instance_func_set = set()
        self.return_instance_func_set.add(warp.Pipe._WARP_get_class_instances.__name__)

    def __get__(self, instance: Any, _) -> Union[SelfType, Any]:
        # TODO: is this a janky af hack?
        # if the number of functions in the call stack matching registered instance-return functions is nonzero, return self instance
        if (isinstance(instance, warp.Pipe) or isinstance(instance, warp.data.decorator.Decorator)) \
           and len(self.return_instance_func_set.intersection(utils.function_call_stack())) > 0:
            return self
        return self.value
