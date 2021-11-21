import warp
from warp import Pipe, Parameter, Product

from . import A


class Main(Pipe):
    message = Parameter('test_B')

    ### products
    # Since this product is never declared via `@produces`, it is ignored by WARP.
    product = Product('data/B.txt', static=True)

    @warp.dependencies(
        productA1=A.A.productA1, 
        productA2=A.A.productA2)
    def run(self):
        print('main() in {:s}'.format(__file__))
        with open(self.product, 'a') as f:
            f.write(self.message)
