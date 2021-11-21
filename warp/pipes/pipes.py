import functools
from abc import abstractmethod

import warp
import warp.constants as constants

from warp.pipes.attributes import Parameter, ParameterFile, Product

# types
from typing import Optional, List, Tuple, Dict, Any, Iterator
StrDict = Dict[str, Any]
SelfType = object


__all__ = [
    'Pipe',
    'Source']


class Pipe:
    """
    `Pipe` must be subclassed by user-defined pipes in order to be recognized by WARP.
    Subclasses can have any name but are restricted to one declaration per file (with the exception of [`Source`](./#source)).

    ???info
        The [`@dependencies`](../../data/data/#dependencies) and [`@produces`](../../data/data/#produces) decorators mutate 
        the `__attrs__` class-level attribute of `Pipe` which is then used by [`Workspace`](../../workspace/workspace/#workspace), 
        which is essentially a roundabout way of making `__attrs__` a global variable.
        `__attrs__` itself is a `dict` mapping `Pipe` subclass names to [`Product`](../../pipes/attributes/#product) instances that
        were passed to the [`@dependencies`](../../data/data/#dependencies) and [`@produces`](../../data/data/#produces) decorators.

        More specifically, [`Workspace`](../../workspace/workspace/#workspace) accesses this quasi-global variable `__attrs__` 
        using class-level methods `__dependencies__` and `__products__` of `Pipe`. 

    Examples:
        !!!example
            The following is a minimal example of a `Pipe` declaration, which is placed in its own file.
            ```python
            # A.py
            from warp import Pipe

            class A(Pipe):
                def run(self) -> None:
                    print('A.run()')
            ```
    """
    __required_keys__ = (
        '__dependencies__', 
        '__products__') 
    __attrs__: StrDict = dict()

    @classmethod
    def __dependencies__(cls, k: str) -> StrDict:
        ret: StrDict = dict()
        if k not in cls.__attrs__:
            return ret
        if '__dependencies__' not in cls.__attrs__[k]:
            return ret
        return cls.__attrs__[k]['__dependencies__']

    @classmethod
    def __products__(cls, k: str) -> StrDict:
        ret: StrDict = dict()
        if k not in cls.__attrs__:
            return ret
        if '__products__' not in cls.__attrs__[k]:
            return ret
        return cls.__attrs__[k]['__products__']

    @classmethod
    def load_products(cls) -> None:
        """For the current `Pipe` subclass, create class-level attributes containing the value of the [`Product`](../pipes/attributes/#product) 
        instances passed to [`@dependencies`](../../data/data/#dependencies).

        This is what allows the user to access the outputs of upstream pipes as if they were attributes of the current `Pipe` context.

        !!!note
            This function is called in [`Workspace.build`](../../workspace/workspace/#build) so that the requested 
            [`@dependencies`](../../data/data/#dependencies) are available at runtime.

        !!!note
            The default behavior of this function is to load cached [`Product`](../attributes/#product) values since they
            are cleared from memory unless otherwise specified.
            This happens by calling the [`Product.load`](../attributes/#warp.pipes.attributes.Product.load) method.
            This behavior occurs if the `Product.__save__` attribute is `True`.

            If `save=False` was passed to a [`Product`](../attributes/#product) constructor, then `Product.__save__ = False`
            and the value of the [`Product`](../attributes/#product) in memory is used.
        """
        pipe_name = cls.__module__

        # load dependency products
        for name, product in cls.__dependencies__(pipe_name).items():
            # Load in requested products. We only give a copy of the contained value to avoid corrupting products.
            source = product.__source__
            if source and warp.Graph.is_source_pipe(source):
                setattr(cls, name, product.path)

            elif name in cls.__products__(source):
                p = cls.__products__(source)[name]

                if p.__save__:
                    p.load()
                    setattr(cls, name, p.value)
                    p.value = None  # unload
                
                else:
                    setattr(cls, name, p.value)

            else:
                raise AttributeError(
                    f'Dependency product `{name}, {product.path}` of `{pipe_name}` could not be found among '
                    f'products. HINT: use @produces in the pipe where `{name}` '
                    'was created.')

    def save_products(self) -> None:
        """For the current `Pipe` subclass, cache the values of each [`Product`](../attributes/#product) that was passed to
        the [`@produces`](../../data/data/#produces) decorator.

        !!!note
            The default behavior is to call the [`Product.save`](../attributes/#warp.pipes.attributes.Product.save) method and then set `product.value = None`.
            However, if `Product.__save__ = False`, then `save_products` does nothing.
        """
        pipe_name = self.__module__

        # handle cached products
        for product in self.__products__(pipe_name).values():
            if product.__save__:
                product.save()
                product.value = None  # clear the product, will be loaded later if necessary

    @classmethod
    def clear_products(cls, name: str) -> None:
        """Iterate over all [`Product`](../attributes/#product) instances passed to the [`@produces`](../../data/data/#produces)
        decorator and set their `value` attributes to `None`.

        Arguments:
            name: The full name of the `Pipe` subclass to clear.
        """
        for p in filter(
            lambda _p: _p.__save__, 
            cls.__products__(name).values()
        ):
            p.value = None

    @classmethod
    def clear_dependencies(cls, name: str) -> None:
        """Iterate over all [`Product`](../attributes/#product) instances passed to the [`@dependencies`](../../data/data/#dependencies)
        decorator and call `delattr` on them.

        Arguments:
            name: The full name of the `Pipe` subclass to clear.
        """
        for name in cls.__dependencies__(name):
            if hasattr(cls, name):
                delattr(cls, name)

    def clear_all(self, name: str) -> None:
        """Call both [`clear_products`](./warp.pipes.pipes.Pipe.clear_products) and 
        [`clear_dependencies`](./warp.pipes.pipes.Pipe.clear_dependencies).
        In addition, find all attributes of the pipe specified by the `name` argument that are 
        [`Product`](../attributes/#product) instances and set their `value` to `None`.

        Arguments:
            name: The full name of the `Pipe` subclass to clear.
        """
        for p in self._WARP_get_products():
            p[1].value = None
        self.clear_dependencies(name)
        self.clear_products(name)

    def _WARP_get_class_instances(self, cls: type) -> Iterator[Tuple[str, Any]]:
        for var in dir(self):
            if var.startswith('_'):
                continue
            attr = getattr(self, var)
            if isinstance(attr, cls):
                yield var, attr

    @functools.lru_cache(maxsize=1)
    def _WARP_get_parameters(self) -> List[Tuple[str, Parameter]]:
        return list(self._WARP_get_class_instances(cls=Parameter))

    @functools.lru_cache(maxsize=1)
    def _WARP_get_parameter_files(self) -> List[Tuple[str, ParameterFile]]:
        return list(self._WARP_get_class_instances(cls=ParameterFile))

    @functools.lru_cache(maxsize=1)
    def _WARP_get_products(self) -> List[Tuple[str, Product]]:
        return list(self._WARP_get_class_instances(cls=Product))

    def _WARP_load_parameters(self, param_dict: StrDict, missing_ok: bool =False) -> None:
        if len(param_dict) == 0:
            return
        params = list(self._WARP_get_parameters())
        # update parameter values
        for attr_name, parameter in params:
            if parameter.name in param_dict:
                parameter.value = param_dict[parameter.name]
            elif not missing_ok:
                raise AttributeError(f'`{attr_name}::{parameter.name}` is not a parameter of current pipe')

    @abstractmethod
    def run(self) -> None:
        """
        `@abstractmethod`

        This function must be declared by `Pipe` subclasses.
        Called by the [`__call__`](./#warp.pipes.pipes.Pipe.__call__) function.
        """
        pass

    def __call__(self) -> None:
        """Calls the [`run`](./#warp.pipes.pipes.Pipe.run) function.
        """
        self.run()


class Source:
    """
    An class for conveniently declaring source [`Product`](../attributes/#product) instances that can be referenced by
    other [`Pipe`](./#pipe) declarations.
    
    Instances of `Source` contain an attribute `Main` which is a [`Pipe`](./#pipe) subclass, allowing `Source` instances 
    to masquerade as python modules.

    Example:
        !!!example
            This example shows how `Source` can be used to easily declare a source [`Product`](../attributes/#product).
            Although you cannot declare multiple [`Pipe`](./#pipe) subclasses in the same file, you can declare `Source` 
            instances in the same file as your pipe declaration.
            ```python
            # A.py
            from warp import Source, Pipe, dependencies

            source = Source()(p0='path/to/product')

            class A(Pipe):
                @dependencies(p0=source.Main.p0)
                def run(self) -> None:
                    print(self.p0)
            ```
    """
    __slots__ = (
        '__name__', 
        '__file__',
        constants.PIPE_DECLARATION_NAME) 

    def __init__(self, name: Optional[str] =None):
        """`Source` pipes will appear in the graph with the naming convention `__source__{name}`.
        """
        self.__name__ = f'__source__{name or ""}'
        self.__file__ = f'__source__{name or ""}'  # gets hashed, so needs to not collide with any other file system location

    def __call__(self, *products: str, **kwproducts: str) -> SelfType:
        """Dynamically create a [`Pipe`](./#pipe) subclass declaration with name `Main` and add it as an attribute of this
        instance.
        Arguments will be wrapped 

        Arguments:
            products: Unnamed source products. 
                These will become attributes of `Source.Main` under the naming convention `product[INDEX]`, 
                where `[INDEX]` is a unique integer.

                !!!example
                    `s = Source()('path/to/file')` will create `s.Main.product0`.

            kwproducts: Named source products. 
                These will become attributes of `Source.Main` under the name specified by the keyword.

                !!!example
                    `s = Source()(user_product='path/to/file')` will create `s.Main.user_product`.
        """
        class Main(Pipe):
            __slots__ = ()

        for i, p in enumerate(products):
            setattr(Main, f'product{i}', Product(p, static=True))  # `Product` instances will be automatically found by `Pipe.products`
        for k, p in kwproducts.items():
            setattr(Main, k, Product(p, external=True))

        setattr(self, constants.PIPE_DECLARATION_NAME, Main)       # attribute name needs to be PIPE_DECLARATION_NAME to be considered a valid pipe

        return self
