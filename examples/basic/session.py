from warp import Graph, Workspace
from warp.globals import register_graph
from warp.constants import WARP_LOGO
from warp import log

from example import A, B, C, D


@register_graph('build-graph-example')
def build_graph() -> Graph:
    return Graph() @ A + B + C + D
    

### Header
print(WARP_LOGO)

### Info
ws = Workspace(graph='build-graph-example')
log.info('Your workspace is now loaded as `ws`.')
log.info(f'Cache directory: {ws.home.path}/{ws.home.session_id}')
log.info('Do `ws.methods` to see available commands. Do `help(ws.[METHOD NAME])` for further information.')
print()
