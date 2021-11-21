import warp
from warp import Pipe, Source, Parameter, ParameterFile, Product
import warp.utils as utils


source = Source()(
    p0='config/A1.yml', 
    p1='config/A2.yml')


# By using the @static decorator, we can force all contained products to be cached statically.
class A(Pipe):
    ### parameters
    # Multiple config files can be specified to handle parameters with more granularity.
    config_file1 = ParameterFile('config/A1.yml')
    config_file2 = ParameterFile('config/A2.yml')

    # The config files contain `message1` and `message2` entries, which will be automatically loaded.
    message1_attr = Parameter('message1', default='MESSAGE1_DEFAULT')
    message2_attr = Parameter('message2', default='MESSAGE2_DEFAULT')

    ### products
    # Note: the `static=True` flag can be passed to force WARP to not create a copy for each distinct session.
    productA1 = Product('data/A1.txt')
    productA2 = Product('data/A2.txt')

    # @warp.dependencies(
    #     source1=source.Main.p0, 
    #     source2=source.Main.p1)
    @warp.produces(
        productA1=productA1, 
        productA2=productA2)
    def run(self) -> None:
        # Imports here will be available for all methods of `Main`.
        with utils.GlobalImport(globals()):
            import os

        # print('Source product: ', self.source1)
        print(f'message1 = {self.message1_attr}, message2 = {self.message2_attr}')

        # Note that the parameter `self.message` behaves like the value it contains.
        # `Product`s behave similarly; the value that will be saved can be assigned directly 
        # to the `Product` variable.
        self.productA1 << 'PRODUCTA1'
        # `<<` is the same as `.value =`
        self.productA2.value = self.message2_attr

        # `@produces` automatically saves products via pickling after `run` completes
