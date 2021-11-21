# std
from copy import deepcopy

# extern

# warp
import warp
from warp.data.decorator import Decorator

# types
from typing import Dict


__all__ = ['dependencies']


def dependencies(**kwargs: Dict[str, warp.Product]):
    """Link [`Product`](../../pipes/attributes/#Product) instances as dependencies of a [`Pipe`](../../pipes/pipes/#Pipe) subclass.
    This decorator should be decorate the `run` function therein.

    Example:
        ```python
        class B(Pipe):
            @dependencies(product=A.product)
            def run(self) -> None:
                # At runtime, `A.product` will be available to this pipe as a class-level attribute under the kwarg name.
                self.product
        ```

    Arguments:
        kwargs: You must specify dependencies with `str` type keys and [`Product`](../../pipes/attributes/#Product) instance values.
    """
    # preprocessing of products
    for k, v in kwargs.items():
        if not isinstance(v, warp.Product):
            kwargs[k] = warp.Product(v, external=True)

    class Dependencies(Decorator):
        def get_attrs(self):
            return '__dependencies__', deepcopy(kwargs)  # full copy to avoid filling with data at runtime

    return Dependencies
