import warp
from warp import Pipe, Parameter, Product
from warp.utils import GlobalImport

from . import A


# Here, we demonstrate how to use custom save/load functions to specify non-pickle based serialization.
# In this case, our product is a directory.
def save(path, value):
    # GlobalImport makes sure that `os` is available
    os.makedirs(path, exist_ok=True)


# Custom save functions also require custom loading functions.
# Since the product is a directory, nothing needs to happen.
def load(path):
    pass


class C(Pipe):
    message = Parameter('message', default='test_C')

    ### products
    # product = Product('data/C', save=save, load=load)
    product = Product('data/C.txt', static=True)

    @warp.dependencies(productA2=A.A.productA2)
    @warp.produces(product=product)
    def run(self):
        with GlobalImport(globals()):
            import os

        print({n: p.value for n, p in A.A()._WARP_get_parameters()})

        print('`run` in {:s}'.format(__file__))
        print(f'Parameter `message` has value `{self.message}`')
        # If a product cached by @save is a dependency for this pipe, then it is automatically loaded.
        print('This product was created by upstream pipe `A`: ', self.productA2)
        self.productA2 = 'ERROR'
        print(f'The value of `self.productA2` has been locally changed to `{self.productA2}`. This value does not carry to other pipes.')

        self.product << 'C_product'
