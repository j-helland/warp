# std
from importlib import import_module
import inspect
import functools

# extern
import networkx as nx

# warp
from warp.utils import pickle, unpickle
from warp.pipes import Pipe, Source
import warp.constants as constants

# types
from typing import Union, List, Set, Dict, Optional, Any
from types import ModuleType

SelfType = object


__all__ = ['Graph']


class Graph:
    """
    A thin wrapper around a multi-digraph which provides some syntax for adding [`Pipe`](../../pipes/pipes/#pipe) subclasses.
    
    - Nodes refer to [`Pipe`](../../pipes/pipes/#pipe) subclasses.
    - Edges represent [`Product`](../../pipes/attributes/#product) instances.

    An instantiated `Graph` is required in order to create a [`Workspace`](../../workspace/workspace/#Workspace).

    Special syntax is provided to reduce the verbosity of adding [`Pipe`](../../pipes/pipes/#pipe) subclasses to the graph.
    Note that both of the following operators can be composed.

    - The `>>` operator is a basic [`add`](./#warp.graph.graph.Graph.add) operation. 
        This operator that you should use for adding whenever possible.

        !!!note
            An error will raise if the [`Pipe`](../../pipes/pipes/#pipe) subclass(es) producing the [`@dependencies`](../../data/data/#dependencies) of the [`Pipe`](../../pipes/pipes/#pipe) subclass to be added do not already exist in the graph as nodes.

    - The `@` operator is an [`add`](./#warp.graph.graph.Graph.add) operation with the additional semantics that any [`@dependencies`](../../data/data/#dependencies) that are not already [`@produces`](../../data/data/#produces) outputs of an existing node in the graph will be added as [`Source`](../../pipes/pipes/#Source) products.

        !!!note
            [`Source`](../../pipes/pipes/#Source) pipes will appear in the `Graph` under the naming convention 
            `__source__{pipe name}{i}`, where `{pipe name}` is the automatically derived name of the 
            [`Pipe`](../../pipes/pipes/#pipe) subclass being added and `{i}` is a unique index for resolving 
            cases in which multiple sources are specified.

        !!!warning
            This operator should be used with caution as it can silently create unexpected `Graph` topologies.

    Examples:
        !!!example
            In the following example, we take [`Pipe`](../../pipes/pipes/#pipe) subclasses `A`, `B`, `C`, and `D` and add them to a `Graph` object.
            ```python
            g = Graph()
            g @ A + B + C + D
            ```
            The last line is equivalent to
            ```python
            g @ A
            g + B
            g + C
            g + D
            ```
            which is in turn equivalent to
            ```python
            g.add(A, make_dependencies_sources=True)
            g.add(B)
            g.add(C)
            g.add(D)
            ```

            !!!note
                Since the topology of the `Graph` is defined implicitly by the [`@dependencies`](../../data/data/#dependencies) 
                and [`@produces`](../../data/data/#produces) decorators within a [`Pipe`](../../pipes/pipes/#pipe) 
                subclass declaration, the syntax `A >> B >> C >> D` does not mean that the graph will be a linked list from `A` to `B`.
                In this sense, the topology of the `Graph` is not reflected by the `>>` operator. 
    
    ---

    """ 
    __slots__ = ('G', 'config_files', 'session_id')

    def __init__(self):
        """
        """
        self.G = nx.MultiDiGraph()
        self.config_files = set()

    # `+`
    def __add__(self, pipe):
        return self.add(pipe)

    # `@`
    def __matmul__(self, pipe):
        return self.add(pipe, make_dependencies_sources=True)

    ##########################################
    # public methods
    ##########################################
    def add(
        self,
        pipe  :ModuleType,
        *,
        make_dependencies_sources  :bool            =False,
        override_existing_attrs    :bool            =False,
        docstring                  :Optional[str]   =None,
    ) -> SelfType:
        """
        Create a node in the graph for a [`Pipe`](../../pipes/pipes/#pipe) subclass. 
        Moreover, create edges between existing nodes whose [`Product`](../../pipes/attributes/#product) instances 
        are [`@dependencies`](../../data/data/#dependencies) of the new node.

        By default, an error will be raised if [`@dependencies`](../../data/data/#dependencies) exist that cannot be found in the graph.
        Using the `make_dependencies_sources=True` flag will cause such [`@dependencies`](../../data/data/#dependencies) 
        to be added as the outputs of a [`Source`](../../pipes/pipes/#Source) node.

        Arguments:
            pipe: Must be a subclass of the [`Pipe`](../../pipes/pipes/#pipe) class.

            make_dependencies_sources: Instead of raising an error when the [`@dependencies`](../../data/data/#dependencies) 
                of `pipe` are not found, create a [`Source`](../../pipes/pipes/#Source) node whose outputs are the missing 
                [`@dependencies`](../../data/data/#dependencies).

            override_existing_attrs: Flag to allow overriding of existing class-level attributes of `pipe`.
                This flag is used internally to allow reloading of the graph, since declaring 
                [`@dependencies`](../../data/data/#dependencies) will create corresponding class-level attributes, the overriding of which is disallowed by default.

                !!!important
                    This flag is for internal use only -- the user should never need this.
            
            docstring: An optional descriptor of the `pipe`'s functionality.

        Return:
            self
        """
        pipe_module = self.get_pipe_module(pipe)
        pipe_instance = pipe_module()

        # VALIDATE: make sure name collision doesn't happen
        if pipe.__name__ in self.G:
            raise BrokenPipeError(f'Tried to add duplicate pipe `{pipe.__name__}`.')

        # VALIDATE: make sure that default config files are only used once
        config_files = pipe_instance._WARP_get_parameter_files()
        for cfg in config_files:
            if cfg[1].value in self.config_files:
                raise BrokenPipeError(
                    f'The config file `{cfg[0]} = ParameterFile(\'{cfg[1].value}\')` has already been used. HINT: break up your config file into separate semantic chunks OR '
                    'use the `multi_use=True` flag of `ParameterFile` in upstream pipes that use this config.')
            elif not cfg[1].__multi_use__:
                self.config_files.add(cfg[1].value)  # reference config files by their paths

        products = self._list_local_pipe_products(pipe_instance)
        for p in products:
            if not isinstance(p, str):
                p = p.path
            self._prevent_nested_products(p)

        # get dependencies -- these must have been declared via @dependencies
        try:
            keys, parent_products = zip(*map(
                lambda p: (p[0], p[1].path), 
                (pipe_module.__dependencies__(pipe.__name__) or {}).items()))
        except ValueError:
            keys = parent_products = tuple()

        # VALIDATE: there should be no existing attributes sharing the same name as dependencies
        if not override_existing_attrs:
            for k in keys:
                if hasattr(pipe_module, k):
                    raise AttributeError(
                        f'pipe `{pipe.__name__}` depends on product of name `{k}` but already has a product attribute of the same name. '
                        'HINT: change the name of the existing product attribute.')

        # derive the parent pipe based on the dependency
        parents = self.get_parents_from_products(parent_products)
        if make_dependencies_sources:  # dirty-add pipe (happens via `@` operator): don't verify that dependencies exist already
            try:
                # lump all missing dependencies for this pipe into one source node
                missing = zip(*filter(
                    lambda p: p[1][1] is None, 
                    enumerate(zip(parent_products, parents))))
                idx, missing = next(missing), next(zip(*next(missing)))

                # get unique name for source
                name = pipe.__name__.split('.')[-1] + '{}'
                n = 0
                while Source().__name__ + name.format(n) in self.G:
                    n += 1
                name = name.format(n)

                # recursive pipe addition
                source = Source(name)(*missing)
                self.add(source)
                for i in idx:
                    parents[i] = source.__name__
            except StopIteration: 
                pass
        else:
            # VALIDATE: every dependency must exist already in the graph
            for p, prod in zip(parents, parent_products):
                if p is None:
                    raise BrokenPipeError(f'no pipe producing `{prod}` exists in the pipeline.')

        # Annotate dependencies with their source pipe.
        # This will be referenced later in WorkSpace build.
        for k, p in zip(keys, parents):
            pipe_module.__dependencies__(pipe.__name__)[k].__source__ = p

        ## add pipe to the graph
        self.G.add_node(pipe.__name__,  
            pipe           =pipe,
            make_dependencies_sources=make_dependencies_sources,  # We need to keep track of how the pipe was added for reloading purposes.
            products       =products,
            parent_products=parent_products,
            docstring      =docstring)
        self._link_parents(
            pipe_name      =pipe.__name__, 
            parents        =parents,
            parent_products=parent_products)  

        return self

    def save(self, path: str) -> None:
        # See, what's real fuckin' nice about python dict-based data structures is that insertion order is preserved.
        # This makes this step way easier.
        # In order to load the graph, we also need to know whether or not each pipe depends on source products. 
        pipe_info = [
            dict(name=p[0], depends_on_source_products=p[1]['make_dependencies_sources']) 
            for p in self.G.nodes(data=True)]
        pickle(path, pipe_info)

    def load(self, path: str):
        # We must clear the current graph and set of config files.
        self.__init__()

        pipe_info = unpickle(path)
        pipe_modules = map(import_module, map(lambda p: p['name'], pipe_info))
        itr = zip(pipe_modules, map(lambda p: p['depends_on_source_products'], pipe_info))

        # NOTE: Assume that loaded pipe_info has correct adding order.
        for p, make_dependencies_sources in itr:
            self.add(p, make_dependencies_sources=make_dependencies_sources)

    def get_parents_from_products(self, products :List[str]) -> List[str]:
        """return a list of pipes that have products matching `products` argument
        """
        if products is None:
            return []

        nodes = self.G.nodes(data=True)
        parents = []
        for p in products:
            for n in nodes:
                if p in n[1]['products']:
                    parents.append(n[0])
                    break
            else:
                parents.append(None)
        return parents

    def get_pipe_dependencies(self, pipe_name: str) -> Dict[str, Any]:
        pipe = self.get_pipe_module(pipe_name)
        return pipe.__dependencies__(pipe_name)

    def paths(self, source :str, dest :str) -> List[str]:
        """Identify all paths from source to destination
        """
        paths_gen = nx.all_simple_paths(self.G, source, dest)
        return list(paths_gen)

    def get_lineage(self, pipe_name: str) -> List[str]:
        ancestors = set(nx.ancestors(self.G, pipe_name))
        ordered_graph = dict(self.G.nodes.data()).keys()
        return [node for node in ordered_graph if node in ancestors]

    @staticmethod
    def is_source_pipe(pipe_name  :str) -> bool:
        """
        """
        assert isinstance(pipe_name, str)
        return pipe_name.startswith(Source().__name__)

    def get_pipe_name_from_abbreviation(self, name :str) -> str:
        """Fuzzy matching of a string to the existing pipe names in the graph.
        """
        if self.is_source_pipe(name):
            return name

        matches = list(filter(
            lambda x: (name in x) and (not self.is_source_pipe(x)), 
            self.G.nodes))
        for m in matches:
            if name == m:
                return m
        else:
            if len(matches) == 0:
                raise ModuleNotFoundError('could not find any pipes matching `{}`'.format(name))
            elif len(matches) > 1:
                raise ValueError('found multiple pipes matching `{}`: {}'.format(name, matches))
        return matches[0]

    def get_pipe_module(self, pipe  :Union[str, type]) -> type:
        """Search module for class declarations subclassing `Pipe`.
        There should only be one of these per file.
        """
        # named access
        if isinstance(pipe, str):
            pipe = self.G.nodes(data='pipe')[pipe]

        # Direct access via keyword. Declaration this way allows for multiple `Pipe` subclasses (not that you'd want that).
        # Faster than looping over entire dir(pipe).
        if hasattr(pipe, constants.PIPE_DECLARATION_NAME):
            return getattr(pipe, constants.PIPE_DECLARATION_NAME)

        # If a pipe is not declared via the `Main` keyword, then search for a `Pipe` subclass.
        # Throw an error if more than one is found.
        pipes = set()
        allowed_modules: Set[str] = {pipe.__name__, pipe.__class__.__module__}
        for var in dir(pipe):
            attr = getattr(pipe, var)
            if inspect.isclass(attr) and issubclass(attr, Pipe) and (attr.__module__ in allowed_modules):
                pipes.add(attr)
        
        if len(pipes) == 0:
            raise ModuleNotFoundError(f'No subclasses of `Pipe` found in `{pipe.__name__}`')
        elif len(pipes) > 1:
            raise BrokenPipeError(f'multiple pipes declared in `{pipe.__name__}`: `{pipes}`')

        return list(pipes)[0]

    def get_pipe_instance(self, pipe_name :str) -> Pipe:
        """
        """
        return self.get_pipe_module(pipe_name)()

    ##########################################
    # private methods
    ##########################################
    def _list_local_pipe_products(self, pipe :Optional[Pipe]) -> List[str]:
        if pipe is None:
            return []
        return list(map(
            lambda p: p[1].path, 
            pipe._WARP_get_products()))

    def _link_parents(
        self, 
        pipe_name        :str, 
        parents          :List[str], 
        parent_products  :Optional[List[str]]
    ) -> None:
        if parent_products is None:
            return
        for parent, product in zip(parents, parent_products):
            self.G.add_edge(parent, pipe_name, label=product)

    def _prevent_nested_products(self, path :str) -> None:
        ''' To prevent nested nodes (physically), we need to ensure that a node is not a subdirectory of another node'''
        for node in list(self.G.nodes):
            if node.startswith(path) or path.startswith(node):
                raise BrokenPipeError(node + ' and ' + path + ' are nested')

    def _is_product(self, product_name :str) -> bool:
        """check to see if `pipe_name` is in the list of pipe products for the entire graph"""
        products = functools.reduce(
            lambda x, y: x + y, map(
                lambda x: x[1]['products'], 
                self.G.nodes(data=True)))
        return product_name in products
