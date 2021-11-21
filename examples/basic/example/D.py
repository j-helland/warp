import warp
from warp import Pipe, Product

from . import A, C

class D(Pipe):
    ### products
    productD = Product('data/C/D.txt')

    # This will throw an error because `productA2` was produced upstream in a pipe using `@reservoir`.
    # productA2 = Product('adfasfadsfasdfasf')

    @warp.dependencies(
        productA2=A.A.productA2,
        product  =C.C.product)
    @warp.produces(productD=productD)
    def run(self):
        print(f'The value of `self.productA2` is `{self.productA2}`')
        self.productD = 'D output'
