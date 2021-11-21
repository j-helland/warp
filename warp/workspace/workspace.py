# stdlib
import os
import shutil
import time
import datetime
import subprocess
import inspect
import functools
from copy import deepcopy

# extern
import networkx as nx

# lazyily loaded modules
from warp.utils import LazyLoader as LL
pd      = LL('pd',      globals(),  'pandas')
yaml    = LL('yaml',    globals(),  'yaml')
syntax  = LL('syntax',  globals(),  'rich.syntax')

# WARP modules
import warp
from warp.graph import Graph
import warp.utils as utils
import warp.constants as consts
import warp.globals
from .home import Home

# logging
from warp.globals import log, log_and_raise

# types
from typing import Optional, Union, List, Dict, Set, Any
ConfigType = Union[str, List[str], Dict[str, Any]]


__all__ = ['Workspace']


def print(*args: str, init: str ='[[WARP]]', **kwargs: Any):
    from rich import print as rich_print 
    rich_print(init, *args, **kwargs)


class Workspace:
    """
    The `Workspace` object is the main interface through which the user can interact with their pipeline -- it serves as a manager for the other core classes that WARP provides, namely the [`Graph`](../../graph/graph/#graph).
    It provides a set of tools for generating desired outputs, inspecting historical outputs, and for visualizing the pipeline itself.
    Notably, `Workspace` is the object that enables pipeline reproducibility by keeping track of data provenance, parameter values used at runtime by each pipe, as well as some other metadata.

    It is also worth noting that `Workspace` can be integrated easily into other scripts that extend functionality e.g. a script that launches multiple sessions to be run in parallel.

    Creating a `Workspace` instance will typically be the last step in initializing your WARP pipeline.
    It requires an instantiated [`Graph`](../../graph/graph/#graph) object passed to `__init__`.
    Once the `Workspace` instance has been created, the next most common step will be to call [`build`](./#build) or [`backfill`](./#backfill) to generate pipeline output.
    """
    __slots__ = (
        'PG', 
        'graph_key',
        'home', 
        'link_static_products') 

    accepted_config_ext = {'yaml', 'yml', 'json'}

    def __init__(
        self, 
        *,
        graph                 :Optional[str]  =None,
        session_id            :Optional[str]  =None,
        link_static_products  :bool           =False,
    ):
        """
        Arguments:
            graph: Constructed pipeline graph.
            session_id: The unique id for the session to launch. 
                If the id does not already exist, this creates a new session with that id.
            link_static_products: Link static products produced by other sessions to this session and resolve backfill operations accordingly.
        """
        # create a cache directory
        self.home = Home(consts.HOME_DIR_DEFAULT, session_id=session_id)
        # NOTE: `home.session_id` is a property that actually creates the directory.
        log.info(f'Loaded session {self.home.session_id}')

        graph_path = os.path.join(self.home.session_dir, 'graph.pkl')
        if isinstance(graph, str):
            if not len(warp.globals.GRAPH_BUILDERS):
                log_and_raise(KeyError(f'No graph builders have been registered. Hint: use `warp.globals.{warp.globals.register_graph.__name__}`'))

            elif graph not in warp.globals.GRAPH_BUILDERS:
                log_and_raise(NameError(f'No graph builder named `{graph}` registered.'))

            self.graph_key = graph
            self.PG = warp.globals.GRAPH_BUILDERS[self.graph_key]()
            log.info(f'Built graph `{self.graph_key}`')

            self.PG.save(graph_path)

        else:
            if not os.path.isfile(graph_path):
                log_and_raise(FileNotFoundError(f'No saved graphs exist for session {self.home.session_id}'))

            self.PG = Graph()
            self.PG.load(graph_path)

        self.link_static_products = link_static_products
        if self.link_static_products:
            log.warn('Linking the static products of other sessions to this one can cause unexpected behaviour -- use with caution.')
    
    ############################################################
    # public methods
    ############################################################
    def backfill(
        self, 
        warp_pipe_name: str, 
        *,
        configs:                  Optional[Dict[str, ConfigType]]  =None,
        build_target_pipe:        bool                             =True,         
        rebuild_all:              bool                             =False,
        rebuild_static_products:  bool                             =False,
    ) -> None:
        """
        Call [`build`](./#build) on all out-of-sync upstream pipes to `warp_pipe_name`, including `warp_pipe_name` itself as the terminal build.

        !!!note
            If there are no out-of-sync upstream pipes, `backfill` reduces to simply calling `build` on `warp_pipe_name`.

        Arguments:
            warp_pipe_name: A partially matching string of a pipe name contained in the graph.
                This string will be fuzzily matched.

            configs: A dict mapping a (fuzzy) pipe name to parameter values that should be used.
                The parameter values can be passed by config file paths or directly using a proper dict structure.

                - If the dict value is `List[str]`, then it is assumed that the list entries are paths to config files.
                - If the dict value is `Dict[str, Any]`, then the parameter whose name matches the dict key will be updated according to the value.
                    If the value is another dict, then this will be merged with the existing dict, giving precedence to the passed values.

                !!!note
                    All pipes specified by the key values will be rebuilt regardless of their timestamps.

            rebuild_all: A flag that forces warp to rebuild all pipes in the lineage of `warp_pipe_name`.

            rebuild_static_products: Flag forcing warp to re-generate static products. 
                Only useful if `Workspace` was initialized using the `link_static_products=True` flag.

        Returns:
        
        Example:
            The following example will force a rebuild of the `exmaple.A` and `example.C` pipes.
            In the `A` case, existing parameter values will be overridden by the values found in `config/A`.yml`.
            In the `C` case, only the `message` parameter is overridden with the specified value.
            ```python
            ws.backfill('D', 
                 configs={
                     'A': 'config/A1.yml', 
                     'C': dict(
                         message='OVERRIDE_C'),
                 },
            )
            ```
        """
        # allow for fuzzy matching of pipe names in dict
        configs = configs or dict()
        configs = {self.PG.get_pipe_name_from_abbreviation(p): c for p, c in configs.items()}

        # if all relevant ancestor builds are up to date, then just run build on target pipe
        pipe_name = self.PG.get_pipe_name_from_abbreviation(warp_pipe_name)
        if rebuild_all:
            # Another soln would have been `self.clear_cache()`, but this would clear the entire graph rather than just the provenance 
            # scope of this pipe. TODO: allow clear_cache to target specific pipes.
            gap_pipes = self.PG.get_lineage(pipe_name)
        else:
            gap_pipes = self._gap_pipes(pipe_name, 
                always_build           =set(configs), 
                rebuild_static_products=rebuild_static_products)
            # Pair up pipe names with their instantiations
            gap_pipes = [(x, self.PG.get_pipe_instance(x)) for x in gap_pipes]

        if len(gap_pipes):
            log.info('Build trajectory: ' + ' -> '.join(map(lambda x: f'{x[0]:s}', gap_pipes)))
        else:
            out_str = f'All relevant ancestors are up to date'
            if build_target_pipe:
                out_str += ', running `build(\'{pipe_name:s}\')`.'
                self.build(warp_pipe_name=pipe_name, warp_backfill_call=False)
            else:
                out_str += ', nothing to do.'
            log.info(out_str)

        for name, _ in gap_pipes:
            try:
                subprocess.check_call([
                    'python', warp.workspace.build_pipe.__file__,
                    '--session-id', self.home.session_id,
                    '--target', name,
                ])
            except subprocess.CalledProcessError as e:
                log.error(f'\nHalting execution of build trajectory due to exception:')
                log_and_raise(e)

        # CLEANUP: set value attribute of every product to `None` regardless of `@dependencies`, `@produces`, and `__save__` attribute.
        # We ignore `@dependencies` and `@produces` here because the user might pollute working memory by setting the values of `Product`
        # instances that aren't flagged as inputs or outputs.
        for pipe_name, pipe in gap_pipes:
            pipe.clear_all(pipe_name)

    def build(
        self, 
        warp_pipe_name      :str, 
        *,
        config              :Optional[ConfigType]  =None,
        warp_backfill_call  :bool                  =False, 
        # force_build         :bool                  =False,
    ) -> None:
        """Build a pipe according to its user-defined pipe class. 
        If any upstream pipes are out-of-sync, an exception will be raised prompting you to use [`backfill`](./#backfill) instead.
        Update pipe parameters from loaded config files.
        If a keyword argument is specified, this always takes precedence over config file values.

        Arguments:
            warp_pipe_name: A partially matching string of a pipe name contained in the graph.
                This string will be fuzzily matched.
            config: 
            warp_backfill_call: Internal flag indicating whether or not build is being called through self.backfill
        """
        pipe_name = self.PG.get_pipe_name_from_abbreviation(warp_pipe_name)
        print('\n\n', init='')
        log.info(f'Building pipe {pipe_name:s}')
        print('[bold]-------------------------------------------[/bold]', init='')

        # load any cached products from parent pipes
        pipe_module = self.PG.get_pipe_module(pipe_name)
        pipe_module.load_products()
        pipe = pipe_module()

        # Update pipe parameters from config files and kwargs.
        # Only try to update if the pipe actually has parameters.
        if len(self.parameters(pipe_name)):
            config_base = self.configs[pipe_name]
            pipe._WARP_load_parameters(config_base, missing_ok=True)

            # loading kwargs after config forces precedence of user-input parameter values
            if config is not None:
                if isinstance(config, str):
                    config = utils.load_config_file(config)
                elif isinstance(config, list):
                    cfg = [utils.load_config_file(c) for c in config]
                    config = dict()
                    for c in cfg:
                        config.update(c)

                config = utils.recursive_update(deepcopy(config_base), config)
            else:
                config = config_base
            
            pipe._WARP_load_parameters(config, missing_ok=True)

        elif (config is not None) and len(config):
            log.warn('pipe {} has no parameters but you entered the following: {}'.format(pipe_name, config))

        # build the pipe
        time_start = time.time()
        try:
            pipe()
        except Exception as e:
            print('[bold]-------------------------------------------[/bold]', init='')
            print(f'[[[bold][red]FAILURE[/red][/bold]]] with exception:')
            log_and_raise(e)
        time_elapsed = time.time() - time_start

        # handle @produced products
        pipe.save_products()

        # check that promised products were produced
        products = map(
            lambda p: p.path.format(warp.globals.product_dir()), 
            filter(
                lambda p: p.__save__,
                pipe_module.__products__(pipe_name).values()))
        for p in products:
            if not os.path.isfile(p) and not os.path.isdir(p):
                raise RuntimeError(f'Promised product `{p}` of pipe `{pipe_name}` was not produced.')

        print('[bold]-------------------------------------------[/bold]', init='')
        log.info(f'Elapsed time: {time_elapsed:.4f}(s)')

        # cache pipe metadata
        self._log_pipe_metadata(pipe_name, time_elapsed)

        ### CLEANUP
        # If this is a backfill operation, we only want to clear out products with `__save__ == True`. This is because some products
        # need to be preserved in working memory according to user specification of the `Product(..., save=False)` flag.
        if warp_backfill_call:
            # clear any products specified by `@dependencies` 
            pipe_module.clear_dependencies(pipe_name)
            # clear any products specified by `@produces`
            pipe_module.clear_products(pipe_name)
        # If this is not a backfill call, then clear everything out. 
        else: 
            pipe.clear_all(pipe_name)

    def status(self, pipe_name :str) -> Optional[Dict[str, str]]:
        """Report whether a data pipe exists and when it was built.

        Arguments:
            pipe_name: Name of the target pipe (will be fuzzily matched).
        """
        from rich.console import Console
        from rich.table import Table

        console = Console()
        table = Table(show_header=False, show_lines=True)

        pipe_name = self.PG.get_pipe_name_from_abbreviation(pipe_name)
        table.title = f'Session {self.home.session_id} | pipe [b]{pipe_name:s}[/b]'
        table.add_column('', style='bold', justify='right')
        table.add_column('', justify='left')
        age = self._get_pipe_age(pipe_name)

        # display pipe lineage
        # highlight pipes that need to be (re)built in red.
        # lineage = Ancestry.lineage(self.PG.G, pipe_name) + [pipe_name]
        lineage = self.PG.get_lineage(pipe_name) + [pipe_name]
        bad_pipes = set(self._gap_pipes(pipe_name))
        lineage = ' [bold]->[/bold] '.join([f'[bold {"red" if p in bad_pipes else "green":s}]{p:s}[/bold {"red" if p in bad_pipes else "green":s}]' for p in lineage])
        table.add_row('Lineage', lineage)

        if age == float('inf'):
            console.print(table)
            fmt_str = f'Pipe [bold]{pipe_name:s}[/bold] has not been built'
            return {'': fmt_str}
        else:
            pipe_hash = self._get_pipe_cache_dir(pipe_name)
            table.add_row('Pipe cache directory', pipe_hash)

            meta_file = os.path.join(self._get_pipe_cache_dir(pipe_name), consts.METADATA_FILE_NAME)
            df_meta = pd.read_csv(meta_file)
            table.add_row('Commit hash', df_meta["git_commit_hash"].values[0])

            timestamp = datetime.datetime.fromtimestamp(age)  # convert to readable timestamp
            table.add_row('Last build', str(timestamp))

            time_elapsed = df_meta['time_elapsed'].values[0]
            table.add_row('Elapsed time', str(time_elapsed))

            param_file = os.path.join(self._get_pipe_cache_dir(pipe_name), consts.PARAMETER_FILE_NAME)
            parameters = utils.load_config_file(param_file)
            parameters = str(parameters).replace('{', '')\
                                        .replace('}', '')\
                                        .replace(',', '\n')\
                                        .replace(' ', '')\
                                        .replace(':', ' : ')
            table.add_row('Parameters', parameters)

            products = self.products(pipe_name)
            if len(products) == 0:
                products = [None]
            table.add_row('Products', str(products).replace('[', '')\
                                                   .replace(']', '')\
                                                   .replace(',', '\n')\
                                                   .replace(' ', ''))
            console.print(table)

    def clear_cache(
        self, 
        session_id  :Optional[str]  =None, 
        # pipe_names  :Optional[str]  =None,
        clear_all   :bool           =False,
    ) -> None:
        """Delete cached metadata about pipes for currently loaded session -- parameter values, commit hash, timestamps etc.

        Arguments:
            session_id: The name of the session to clear.
            clear_all: If True, deletes full warp cache, including all session directories therein. 
                If False, deletes cache for currently loaded session only.
        
        TODO: allow the clearing of specific pipes within a session.
        """
        path = self.home.path
        if session_id is not None:
            if clear_all:
                log.warn('session_id not None does nothing with `clear_all=True`')
            else:
                assert self.home.is_valid_session_id(session_id)
                path = os.path.join(path, str(session_id))
        elif not clear_all:
            session_id = self.home.session_id
            path = self.home('')
        else:
            session_id = str(time.time())

        if os.path.isdir(path):
            log.info(f'Deleting cache {path}')
            log.info('Resetting...')
            shutil.rmtree(path)
        else:
            log.warn(f'path `{path}` does not exist')

        if session_id == self.home.session_id or clear_all:
            log.info(f'Starting new session with id {session_id}')
            self.create_session(session_id)

    def resume(self) -> None:
        """Load the most recent session.
        """
        meta_path = os.path.join(self.home.path, consts.WARP_METADATA_FILE_NAME)
        if not os.path.isfile(meta_path):
            log.info('No previous sessions are available, retaining current session.') 
            return

        log.info('Loading most recent session...')
        with open(meta_path, 'r') as meta:
            session_id = meta.read()
        if session_id == self.home.session_id:
            log.info('The current session is the most recent.')
        else:
            self.load_session(session_id)

    def load_session(self, session_id  :str) -> None:
        """Load an existing session. 

        Arguments:
            session_id: The name of the session to load. 
                !!!note
                    There is no fuzzy matching -- `session_id` must match an existing session_id exactly.
        """
        log.info('Attempting to load session id {}'.format(session_id))
        if not self.home.is_valid_session_id(session_id):
            log_and_raise(FileNotFoundError(f'no session with id {session_id} found, try `create_session({session_id})` instead'))
        if session_id == self.home.session_id:
            log.info(f'Session {session_id} is already the current session.')
        else:
            self.home = Home(self.home.path, session_id=session_id)
            log.info('Load succeeded')
        
        # final step: load the runtime parameter values from last time
        self._load_parameters_from_this_session()

    def create_session(self, session_id  :str) -> None:
        """Create a new session with the id `session_id` and load into it.

        Arguments:
            session_id: The name of the new session to create. 
                Must not collide with an existing session.
        """
        if self.home.is_valid_session_id(session_id):
            log_and_raise(FileExistsError(f'session with id {session_id} already exists'))

        self.home = Home(self.home.path, session_id=session_id)
        # home.session_dir must be accessed to actually create the directory
        log.info(f'Session {self.home.session_dir} created and loaded.')

    def parameters(self, pipe_name :str) -> list:
        """List the parameter values that are currently loaded for a pipe.

        Arguments:
            pipe_name: Name of the pipe.
                !!!note
                    `pipe_name` is fuzzily matched against existing pipes in the graph.
        """
        pipe_name = self.PG.get_pipe_name_from_abbreviation(pipe_name)
        main = self.PG.get_pipe_instance(pipe_name)
        return main._WARP_get_parameters()

    def products(self, pipe_name  :str) -> List[str]:
        """List the products that a pipe will produce.

        Arguments:
            pipe_name: Name of the pipe. 
                !!!note
                    `pipe_name` is fuzzily matched against existing pipes in the graph.
        """
        pipe_name = self.PG.get_pipe_name_from_abbreviation(pipe_name)
        main = self.PG.get_pipe_instance(pipe_name)
        return list(map(
            lambda p: p[1].path, 
            main._WARP_get_products()))

    def show(
        self, 
        path    :Optional[str] =None, 
        verbose :bool          =False
    ) -> None:
        """Visualize the pipe graph using graphviz.
        Optionally save the visualization to a file path.
        See [`warp.visualization`][warp.visualization].

        Arguments:
            path: Path to save visualization of the pipeline.
            verbose: If True, display the full node/edge labels.
        """
        from warp.visualization import matplotlib_show
        matplotlib_show(self.PG.G, 
            path   =path, 
            verbose=verbose)

    def gui(self) -> None:
        """Launch the GUI mode of WARP.
        See [`warp.visualization.gui`][warp.visualization.gui].
        """
        from warp.visualization.gui import create_server
        create_server(graph=self.PG.G, workspace=self)

    def view_pipe(
        self, 
        pipe_name   :str, 
        return_str  :bool  =False, 
        full_file   :bool  =False
    ) -> Optional[str]:
        """Display the source code for a pipe.

        Arguments:
            pipe_name: Name of the pipe in the graph.
                !!!note
                    Will be fuzzily matched.
            
            return_str: Flag to return the source code of the pipe as a string.

            full_file: Flag to show the full contents of the source file rather than just the pipe class declaration therein.
        """
        pipe_name = self.PG.get_pipe_name_from_abbreviation(pipe_name)
        main = self.PG.get_pipe_module(pipe_name)

        if full_file:
            with open(inspect.getsourcefile(main), 'r') as f:
                src_str = f.read()
        else:
            src_str = inspect.getsource(main)

        if return_str:
            return src_str

        # display with syntax highlighting
        src = syntax.Syntax(src_str, 'python', theme='monokai', line_numbers=True)
        print(src, init='')

    # def script(self, pipe_name  :str, out_path  :str):
    #     """Compute the lineage necessary to compute the output of a pipe. Dump the source code for each pipe in the lineage into the 
    #     specified file.

    #     TODO: Implement parser so that the output script can actuall run. 
    #     This will require replacing products with their values, parameters with their runtime values, etc.
    #     """
    #     raise NotImplementedError

    #     import re

    #     pipe_name = self.PG.get_pipe_name_from_abbreviation(pipe_name)
    #     lineage = Ancestry.lineage(self.PG.G, pipe_name)[1:] + [pipe_name]
    #     sources = [self.view_pipe(p, return_str=True) for p in lineage]

    #     with open(out_path, 'w') as out_file:
    #         for s in sources:
    #             out_file.write(s)
    #             out_file.write('\n\n\n')

    @property
    def pipes(self):
        """List the pipes present in the pipeline graph.
        """
        return self.PG.G.nodes

    @property
    def sessions(self) -> List[str]:
        """List the available sessions.
        """
        return list(filter(
            lambda d: os.path.isdir(os.path.join(self.home.path, d)) and (d != consts.STATIC_PRODUCTS_DIR_NAME), 
            os.listdir(self.home.path)))

    @property
    def session_timestamps(self) -> List[float]:
        """List the timestamp of each existing session.
        """
        return [
            float(open(os.path.join(self.home.path, s, consts.WARP_PIPE_TIMESTAMP_FILE_NAME), 'r').read())
            for s in self.sessions]

    # TODO: python >=3.8 functools.cached_property
    @property
    @functools.lru_cache(maxsize=1)
    def methods(self) -> List[str]:
        """List the available methods for the user.
        """
        return list(filter(
            lambda x: (x[0] != '_') and (x[:2] != '__' and x[-2:] != '__'),
            dir(self)))

    # TODO: python >=3.8 functools.cached_property
    @property
    @functools.lru_cache(maxsize=1)
    def configs(self) -> Dict[str, Any]:
        """Search for declared config files and load them.
        Results are cached via `@functools.lru_cache(maxsize=1)`.
        """
        configs = dict()
        for pipe_name in self.pipes:
            if self.PG.is_source_pipe(pipe_name):
                continue
            # Get all files in config directory.
            # If no config directory was specified by the user, default to looking in the directory where the pipe
            # was defined.
            param_files = self.PG.get_pipe_instance(pipe_name)._WARP_get_parameter_files()
            if len(param_files) == 0:
                configs[pipe_name] = dict()
            else:
                # load parameter values from each config file
                cfgs = [utils.load_config_file(pf[1].value) for pf in param_files]

                # check for duplicate parameter specs
                params = set()
                for pf, cfg in zip(param_files, cfgs):
                    _params = set(cfg)
                    duplicates = params.intersection(_params)
                    if len(duplicates):
                        raise ValueError(f'duplicate config spec for parameter(s) `{duplicates}` of pipe `{pipe_name}`')
                    params = params.union(_params)

                # cache config file values for loading later
                for cfg in cfgs:
                    if pipe_name in configs:
                        configs[pipe_name] = {**configs[pipe_name], **cfg}.copy()
                    else:
                        configs[pipe_name] = cfg
        return configs

    ############################################################
    # private methods
    ############################################################
    def _load_parameters_from_this_session(self) -> None:
        # load previous runtime parameters for all pipes
        for pipe_name in self.pipes:
            param_file = os.path.join(self._get_pipe_cache_dir(pipe_name), consts.PARAMETER_FILE_NAME)
            # param file might not exist if a pipe was never run in this session
            if not os.path.isfile(param_file):
                continue
            parameters = utils.load_config_file(param_file)

            pipe = self.PG.get_pipe_instance(pipe_name)

            if len(self.parameters(pipe_name)):
                pipe._WARP_load_parameters(parameters)
                log.info(f'Successfully loaded cached runtime parameter values for `{pipe_name}` from `{param_file}`')

    def _is_pipe_built(self, pipe_name  :str) -> bool:
        """If a product of a pipe does not exist, we know the pipe was not built.
        """
        # add correct output directory depending on whether or not the product was marked as __static__
        prods = map(
                lambda s: s.format(warp.globals.product_dir()), 
                self.products(pipe_name))
        # make sure at least one of the output products exists
        return not any(map(
            lambda p: not os.path.isfile(p)
                      and not os.path.isdir(p),
            prods))

    def _get_pipe_age(self, pipe_name  :str) -> float:
        meta_file = os.path.join(self._get_pipe_cache_dir(pipe_name), consts.METADATA_FILE_NAME)
        if os.path.isfile(meta_file):
            df_meta = pd.read_csv(meta_file)
            age = df_meta['last_build_time'].values[0]
        else:
            age = float('inf')
        return age

    def _discuss_ancestry_integrity(self, pipe_name  :str) -> None:
        """Throw error if there was an ancestral pipe that was built more recently than the target pipe.
        """
        # lineage = Ancestry.lineage(self.PG.G, node_name=pipe_name)
        lineage = self.PG.get_lineage(pipe_name)
        ages = {p: self._get_pipe_age(p) for p in lineage}

        # get pipes in the lineage that have not been built
        unbuilt = list(filter(lambda x: ages[x] == float('inf'), ages))
        if len(unbuilt):
            e = RuntimeError('Unbuilt ancestor pipe(s).') 
            log.exception(e)
            for p in unbuilt:  # properly order the ancestors while removing duplicates due to multigraph
                log.error(f'\t{p:s}')
            log.error(f'Hint: run `backfill(\'{pipe_name:s}\')`')
            raise e
        
        # get pipes in the lineage that have been built more recently than their descendents        
        unordered = [p for i, p in enumerate(lineage) 
                     if any(map(
                         lambda x: ages[lineage[i]] > ages[x], 
                         lineage[i+1:]))]
        if len(unordered):
            e = RuntimeError(f'Ancestral pipe(s) built more recently than descendents.')
            log.exception(e)
            for p in unordered:
                log.error(f'\t{p:s}')
            log.error('Hint: run `backfill(\'{pipe_name:s}\')`')
            raise e

    def _parameter_intersection_warning(self, pipe_name  :str, parameters  :Dict[str, Any]) -> None:
        """Display warnings when a parameter will override a config file value for pipe_name pipe.
        """
        pint = set(self.configs[pipe_name].keys())
        pint = pint.intersection(set(parameters.keys()))
        for p in pint:
            log.warn(f'overriding parameter `{p}` config value `{self.configs[pipe_name][p]}` with user-specified value `{parameters[p]}`')
            # warnings.warn('overriding parameter `{}` config value `{}` with user-specified value `{}`'.format(
            #     p, self.configs[pipe_name][p], parameters[p]))

    # def _impute_pipes(self, pipes, target) -> list:
    #     # append to pipes any additional pipes that sit on a path connecting a
    #     #   pipe in pipes to target
    #     # TODO upgrade from History to Ancestry to do this cleaner
    #     pipes = set(pipes)
    #     # while appending:
    #     for p in pipes.copy():
    #         graph_paths = self.PG.paths(source=p, dest=target)
    #         graph_path_pipes = set([])
    #         for p in graph_paths:
    #             graph_path_pipes.update(set(p))
    #         pipes.update(graph_path_pipes)
    #     if len(pipes) > 0:
    #         pipes.add(target)
    #     ordered = [p for p in self.pipes if p in pipes]
    #     return ordered

    def _gap_pipes(
        self, 
        target:                  str, 
        always_build:            Optional[Set[str]]  =None, 
        rebuild_static_products: bool           =False,
    ) -> List[str]:
        """Consider the target's history. Every member of the lineage that
        either (a) has not been built yet or (b) is younger than its ancestors
        is part of the 'historical gap'. Return the list of such pipes.
        """
        # # TODO: Might be worth having more sophisticated source code versioning. For example, a new class attribute that isn't used
        # #       maybe shouldn't trigger a rebuild. This would require a parser, which will be necessary for the generating scripts
        # #       feature.
        # def is_pipe_code_changed(p):
        #     import difflib

        #     cache_dir = self._get_pipe_cache_dir(p)
        #     diff = []
        #     if os.path.isfile(os.path.join(cache_dir, consts.SOURCE_FILE_NAME)):
        #         with open(os.path.join(cache_dir, consts.SOURCE_FILE_NAME), 'r') as f:
        #             source_prev = f.read().split('\n')
        #         source = self.view_pipe(p, return_str=True, full_file=True).split('\n')

        #         diff = filter(lambda li: li[0] != ' ', difflib.ndiff(source, source_prev))
        #     try:
        #         next(diff)
        #         return True
        #     except StopIteration:
        #         return False

        # get lineage of target pipe
        # lineage_full = Ancestry.lineage(self.PG.G, target) + [target]
        lineage_full = self.PG.get_lineage(target) + [target]

        if self.link_static_products:
            # Remove already existing static product ancestors from lineage
            parents_to_drop = set()
            parents_to_keep = set()
            for p in lineage_full:
                for d in self.PG.get_pipe_dependencies(p).values():
                    # No need to rebuild if all the products are static and already exist
                    if (not rebuild_static_products) and (d.__static__ and (os.path.isfile(d.path) or os.path.isdir(d.path))):
                        parents_to_drop.add(d.__source__)
                    # We need to track parents_to_keep because of the multigraph -- a parent can be a dependency for both static and non-static products.
                    else:
                        parents_to_keep.add(d.__source__)
            lineage = list(filter(lambda p: p not in (parents_to_drop - parents_to_keep), lineage_full))
            subgraph = self.PG.G.subgraph(lineage)
            # Retrieve connected component containing target node.
            subgraph = subgraph.subgraph(nx.shortest_path(subgraph.reverse(copy=False), target))
            # Restrict lineage to this connected component.
            lineage = [p for p in lineage if p in subgraph]
        else:
            lineage = lineage_full

        # find parents that didn't save their products
        parents_rebuild = [p.__source__ 
            for p in self.PG.get_pipe_dependencies(target).values()
            if not p.__save__]

        ages = {p: self._get_pipe_age(p) for p in lineage}

        # list comprehension arouses me
        bad_pipes = set([
            p for i, p in enumerate(lineage) 
                 # inf age means never built
              if (ages[p] == float('inf'))  
                 # build chronology violation
                 or any(map(
                     # NOTE: We have to make sure not only that downstream pipes in lineage are chronologically
                     #       in sync, we also have to make sure that they're actually path-connected. This is to 
                     #       avoid problems with branching diamond-dependency type topologies in which non-connected
                     #       pipes will show up in the lineage and will be detected as bad otherwise.
                     lambda x: (ages[p] < ages[x]) and 
                               nx.has_path(self.PG.G, x, p),
                     lineage[:i]))
                 # pipes that don't save their products need to be rebuilt if they are direct parents of target
                 or (p in parents_rebuild)
                 or (not self._is_pipe_built(p))])
                #  # pipe needs to be rebuilt if its source code has been changed
                #  or is_pipe_code_changed(p)]

        # bad_pipes |= (always_build or set())
        # bad_pipes = bad_pipes.union(always_build or set())
        # get pipes in the proper order
        bad_pipes = [p 
            for p in lineage_full 
            if p in bad_pipes | (always_build or set())]  
        # bad_pipes = [(x, self.PG.get_pipe_instance(x)) for x in bad_pipes]

        return bad_pipes

    def _get_pipe_cache_dir(self, pipe_name  :str) -> str:
        if self.PG.is_source_pipe(pipe_name):  # source nodes don't have pipes TODO: should they?
            pipe_hash = utils.hash_path(pipe_name)
        else:
            pipe_hash = utils.hash_path(self.pipes[pipe_name]['pipe'].__file__)
        return self.home(pipe_hash)

    def _log_pipe_metadata(self, pipe_name  :str, time_elapsed  :float) -> None:
        # set up metadata cache directory for the pipe
        cache_dir = self._get_pipe_cache_dir(pipe_name)
        os.makedirs(cache_dir, exist_ok=True)

        # create a timestamp
        timestamp = time.time()

        # retrieve git commit hash of repo
        try:
            commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD'])
            commit_hash = commit_hash.decode('utf-8').strip()
        except Exception as e:
            log.exception(e)
            log.warn('No git versioning detected')
            commit_hash = None

        # cache log file
        df = pd.DataFrame(
            data    =[[pipe_name, timestamp, time_elapsed, commit_hash]],
            columns =['pipe_name', 'last_build_time', 'time_elapsed', 'git_commit_hash'])
        df.to_csv(os.path.join(cache_dir, consts.METADATA_FILE_NAME))

        # cache current parameter values for pipe
        parameters = {v.name: v.value for k, v in self.parameters(pipe_name)}
        utils.save_config(
            path=os.path.join(cache_dir, consts.PARAMETER_FILE_NAME),
            config=parameters)

        # log the name of the current session for the sake of resuming later
        with open(os.path.join(self.home.path, consts.WARP_METADATA_FILE_NAME), 'w') as meta:
            meta.write(self.home.session_id)

        # log the source code of the pipe
        with open(os.path.join(cache_dir, consts.SOURCE_FILE_NAME), 'w') as source:
            source.write(self.view_pipe(pipe_name, return_str=True, full_file=True))
