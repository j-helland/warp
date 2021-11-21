import os
import inspect

import warp
import warp.constants as constants
from warp.pipes.abstract import AbstractPipeObject
import warp.utils as utils

# types
from typing import List, Dict, Union, Optional, Type, Any
from types import FunctionType

SelfType = object


__all__ = [
    'Parameter',
    'ParameterFile',
    'Product']


class Parameter(AbstractPipeObject):
    """
    A wrapper for pipeline parameters that can be loaded from and saved to YAML files.
    This class is meant to be used to create class-level attributes of your [`Pipe`](../pipes/#pipe) subclass (see example below).
    When used in this way, `Parameter` objects can be treated syntactically like the value that they contain.

    Example:
        !!!example
            ```python
            class A(Pipe):
                p = Parameter('pname', default=0)

                def run(self) -> None:
                    # `p` can be used like a regular class attribute. 
                    print(self.p)
                    self.p += 1
                    print(self.p)
            ```
    """
    __slots__ = ('name', 'kwargs')

    def __init__(self, name  :str, **kwargs  :Dict[str, Any]):
        """
        Arguments:
            name: Identifier for the parameter. 
                If loading the value from a YAML config file, this needs to match the corresponding field name. 
            
            default: (`Optional[Any]`) The default value that will be used for the parameter if no config file is specified.
                If not specified, the value of the parameter will be `name` unless overridden by a config file.

                The type of this value must be serializable to a YAML file -- it cannot be a function or a class.
                An error will be raised if the type is not allowed.
        """
        self.name = name

        # validate kwargs
        self.kwargs = kwargs
        if self.kwargs.get('return_instance_func_set', False):
            raise NameError('parameter name `return_instance_func_set` is reserved, please use a different name')

        # get parameter type and make sure it's valid
        param_type = self.kwargs['type'] = self._get_type(self.kwargs)
        if self.kwargs.get('default', False):
            assert type(kwargs['default']) == param_type, \
                'default parameter value must be of specified type {}'.format(param_type)
            value = self.kwargs['default']
        else:
            value = self.name

        super().__init__(value=value, value_type=param_type)

    @staticmethod
    def _get_type(kwargs  :Dict[str, Any]) -> Type[Any]:
        # source: https://github.com/Netflix/metaflow/blob/master/metaflow/parameters.py#L197
        default_type = str

        default = kwargs.get('default')
        if default is not None:
            assert not callable(default), 'default argument cannot be a function'
            assert not inspect.isclass(default), 'default argument cannot be a class'
            default_type = type(default)

        return kwargs.get('type', default_type)

    def __str__(self) -> str:
        return self.value.__str__()


class ParameterFile(AbstractPipeObject):
    """
    A wrapper for declaring a config file to load [`Parameter`](./#parameter) values from.
    This config file should contain at least one field that corresponds to a parameter of the pipe in which this config is used.

    This should be used as a class-level attribute of your [`Pipe`](../pipes/#pipe) subclass.

    Example:
        ```python
        class A(Pipe):
            # Must have a field `p_field` e.g. `p_field: 0`
            config = ParameterFile('../configs/A.yml')

            p = Parameter('p_field')

            def run(self) -> None:
                # The value from the config file. 
                # In this case, the output will be `p value 0`.
                print('p value: ', p)
        ```
    """
    __slots__ = '__multi_use__'

    def __init__(
        self, 
        path:       str, 
        multi_use:  bool  =False,
    ):
        """
        Arguments:
            path: File path to a valid YAML file with fields matching [`Parameter`](./#parameter) names.

            multi_use: Flag to allow a config file to be used by more than just this pipe.
                In such cases, each pipe declaring this config file must use this flag.
        """
        self.__multi_use__ = multi_use

        if not os.path.isfile(path):
            raise EnvironmentError(f'`{path}` does not exist`')
        super().__init__(value=path, value_type=str)

    # def __set__(self, value):
    #     raise RuntimeError('Cannot change the value of a `ParameterFile` object.')


class Product(AbstractPipeObject):
    """
    A wrapper for the inputs/outputs of [`Pipe`](../pipes/#pipe) subclasses.
    The saving/loading behavior is altered by passing various flags to [`__init__`](./#warp.pipes.attributes.Product.__init__).

    By default, products passed into [`@produces`](../../data/data/#produces) will be [pickled](../../utils/utils/#warp.utils.utils.pickle) to the WARP cache location 
    associated with the active session at runtime. 
    Similarly, products passed into [`@dependencies`](../../data/data/#dependencies) will be [unpickled](../../utils/utils/#warp.utils.utils.unpickle)

    Custom saving/loading functions can be attached to a `Product` if pickling is not desired.

    !!!note
        The topology of the [`Graph`](../../graph/graph/#graph) is implicitly defined by passing `Product` instances to the
        [`@dependencies`](../../data/data/#dependencies) and [`@produces`](../../data/data/#produces) decorators in the
        [`Pipe`](../pipes/#pipe) declaration.

    !!!tip
        `Product` provides the custom syntax `<<<`, which is shorthand for assigning a value to the `Product.value` attribute.

        ???example
            ```python
            p = Product('p')
            p << 0
            p.value = 0  # equivalent
            ```

    ???info
        `Product` contains the following class-level attributes that change the behavior of 
        [`Workspace.build`](../../workspace/workspace/#warp.workspace.Workspace.build).

        - `__source__`: The name of the pipe that produced this product.
        - `__static__`: Specifies whether or not to cache the product statically.
        - `__external__`: Specifies whether or not to cache the product externally.
        - `__save__`: Specifies whether to cache the product or to leave it in working memory.
    """
    __source__       = None
    __static__       = False
    __external__     = False
    __save__         = True 

    def __init__(
        self, 
        path:      str, 
        *,
        static:    bool                                 =False,
        external:  bool                                 =False,
        save:      Optional[Union[bool, FunctionType]]  =None,
        load:      Optional[FunctionType]               =None,
    ):
        """
        Arguments:
            path: The relative path to save the product value to after the `run()` function of a [`Pipe`](../pipes/#pipe) subclass finishes.

                By default, this path will be appended to the path corresponding to the current WARP session directory in the WARP cache.
                Both `static` and `external` modify this behavior.

                - If `static=True`, `path` will instead be appended to the `static_products/` subdirectory of the WARP cache.
                - If `external=True`, then the path will remain unaltered regardless of the `static` flag.
            
            static: Flag to mark this product as static, meaning that this product will be visible to other sessions.
                Unless `external=True` is specified, this will cause `path` to be appended to the `static_products/` subdirectory of the WARP cache.

            external: Flag to mark this product as external, meaning that its `path` will left unmodified.

                ???info
                    This flag is used by the implementation of [`Source`](../pipes/#Source) to allow the ingestion of external data artifacts into the pipeline.

            save: 
                - `None`: Cache the `Product` value by calling [`pickle`](../../utils/utils/#warp.utils.utils.pickle).

                - `FunctionType`: A user-defined callable implementing custom saving behavior.
                    The function must have the signature `(path: str, value: Any) -> None`.
                    The `path` argument to this function will always be the fully resolved file location.

                    Passing a custom callable to `save` requires also passing a custom callable to 
                    [`load`](./#warp.pipes.attributes.Product.load).

                    ???example
                        The default `save` function is implemented in WARP as
                        ```python
                        def save(path: str, value: Any) -> None:
                            warp.utils.pickle(path, value)
                        ```

                    !!!warning
                        This callable must create a file specified by `path`, otherwise WARP will detect this pipe as 
                        unbuilt and trigger rebuilds.

                - `False`: Do not cache the product -- rather, leave it in working memory.

                    !!!warning
                        During [`backfill`](../../workspace/workspace/#warp.workspace.Workspace.backfill) calls, products 
                        that are not cached in this manner will trigger rebuilds of the upstream pipes that produced them 
                        if said upstream pipes are direct ancestors of the target pipe.
            
            load:
                - `None`: Call [`unpickle`](../../utils/utils/#warp.utils.utils.unpickle) on the specified `path`.

                - `FunctionType`: A user-defined callable implementing custom loading behavior.
                    The callable must have the signature `(path: str) -> Any`.
                    The `path` argument to this function will always be the fully resolved file location.

                    Passing a custom callable to `load` requires also passing a custom callable to
                    [`save`](./#warp.pipes.attributes.Product.save).

                    ???example
                        The default `load` function is implemented in WARP as
                        ```python
                        def load(path: str) -> Any:
                            return warp.utils.unpickle(path)
                        ```
        """
        self._path = path
        self.__external__ = external
        self.__static__ = static

        if save == False:
            self.__save__ = save
        else:
            if (save is not None and load is None) or (save is None and load is not None):
                raise ValueError('A custom `save` method requires a custom `load` method.')

            if save is not None:
                if not callable(save):
                    raise ValueError('`save` must be callable')

                argnames = set(inspect.getfullargspec(save)[0])
                if len(argnames - {'path', 'value'}) != 0:
                    raise NameError(
                        'Custom `save` function must have arguments `path` and `value` but got argspec '
                        f'`{argnames}` instead.')
                assert isinstance(save, FunctionType)
                setattr(self, '_save', save)
                # self._save: FunctionType = save

                argnames = set(inspect.getfullargspec(load)[0])
                if len(argnames - {'path'}) != 0:
                    raise NameError(
                            'Custom `load` function must have arguments `path` but got argspec '
                            f'`{argnames}` instead.')
                assert isinstance(load, FunctionType)  # `load` cannot be NoneType at this point.
                setattr(self, '_load', load)
                # self._load: FunctionType = load  

        super().__init__(
            value     =self.path, 
            value_type=str)

    # TODO: figure out how to make products behave like parameters
    # I had to override the AbstractPipeObject behaviour because I couldn't figure out a clean way to integrate products with @produces otherwise.
    # This difficulty is mainly because save/load functions need to be attached and referred to in downstream pipes.
    def __get__(self, *args: List[Any]) -> SelfType:
        return self

    # Since `=` will now overwrite the entire class (because of `__get__` override) rather than `self.value`, I defined `<<` as an assignment operator for convenience.
    # You can also do `obj.value = other_value`, but this is kind of annoying.
    # `<<`
    def __lshift__(self, other: Any) -> None:
        self.value = other

    def __str__(self) -> str:
        return str(self.value)

    @property
    def path(self) -> str:
        """The correctly resolved path to the `Product` based on the state of the `__external__` and `__static__` attributes.

        !!!warning
            The correct WARP cache location for the current session must still be resolved, which can be done via 
            `path.format(warp.globals.product_dir())`.
        """
        if self.__external__:
            return self._path
        elif self.__static__:
            return os.path.join(
                constants.HOME_DIR_DEFAULT, 
                constants.STATIC_PRODUCTS_DIR_NAME, 
                self._path)
        else:
            return os.path.join('{}', self._path)

    def save(self) -> None:
        self._save(
            path =self.path.format(warp.globals.product_dir()), 
            value=self.value)

    def load(
        self, 
        path:  Optional[str] =None, 
        value: Optional[Any] =None,
    ) -> Optional[SelfType]:
        if path is None and value is None:
            self.load(path=self.path.format(warp.globals.product_dir()))
            return 

        if path is None:
            self.value = value
        else:
            self.value = self._load(path=path.format(warp.globals.product_dir()))
        return self

    # default implementation for product saving is to pickle value
    @staticmethod
    def _save(path: str, value: Any) -> None:
        utils.pickle(path, value)

    # default load implementation is just unpickling
    @staticmethod
    def _load(path: str) -> Any:
        return utils.unpickle(path)
