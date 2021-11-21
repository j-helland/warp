# std

# extern

# warp
import warp
from warp.data.decorator import Decorator

# types
from typing import Dict


__all__ = ['produces']


def produces(**kwproducts  :Dict[str, warp.Product]):
    """Declare the products of a pipe. 
    Products fed to this decorator will be automatically pickled after the wrapped function finishes unless the `save` and `load` arguments of [`Product`](../../pipes/attributes/#Product) are specified.

    Because we want non-static products to be cached in a session-specific location, we unfortunately
    need to refer to a global variable that specifies this session `warp.globals.product_dir()`.

    Example:
        ```python
        class A(Pipe):
            product = warp.Product('product')

            @produces(product=product)
            def run(self) -> None:
                ...
        ```

    Arguments:
        kwproducts: Values must be [`Product`](../../pipes/attributes/#Product) instances and keys must match the name of the product.
    """
    class Produces(Decorator):
        def get_attrs(self):
            return '__products__', kwproducts
    
    return Produces
