import os
import typing as T

from warp import Source


__all__ = ['matplotlib_show']


def matplotlib_show(graph   :T.Any,  # :nx.MultiDiGraph 
                    *,
                    path    :T.Optional[str] =None, 
                    verbose :bool            =False
                    ) -> None:
        """
        Visualize the pipe graph using graphviz.

        :param path     : Optional path to save image of workspace graph.
        :param verbose  : If True, display the full node/edge labels.
        """
        # allow use of WARP without installing graphviz
        try:
            import networkx as nx
            import pydot
            import matplotlib.pyplot as plt 
            import matplotlib.image as mpimg
            from io import BytesIO
        except ImportError as e:
            print(f'[bold red]ERROR:[/bold red] {repr(e)}')
            raise ImportError('show() requires pydot installed, try `pip install pydot` or install python-graphviz if you want this functionality')

        G = graph.copy()

        # reduce the graph node/edge labels to avoid eye-bleeding
        if not verbose:
            node_mapping = dict()
            # node_reduced_set = set()
            for n in G:
                # if simplified node name results in collision, increase verbosity until there is no longer a collision
                idx = -1
                n_new = '.'.join(n.split('.')[idx:])
                while n_new in set(node_mapping.values()):
                    idx -= 1
                    n_new = '.'.join(n.split('.')[idx:])

                if n.startswith(Source().__name__):
                    n_new = ' '

                node_mapping[n] = n_new

            # edge name collisions aren't a problem since this is a multi-graph
            G = nx.relabel_nodes(G, node_mapping)
            for edge in G.edges(data=True):
                label = edge[-1]['label']
                edge[-1]['label'] = label.split('/')[-1]

        # graphviz produces fairly nice multigraph visualizations by default
        d = nx.drawing.nx_pydot.to_pydot(G)
        png_str = d.create_png()
        sio = BytesIO()
        sio.write(png_str)
        sio.seek(0)

        img = mpimg.imread(sio)
        imgplot = plt.imshow(img)
        plt.axis('off')
        if path is not None:
            if os.path.isdir(path):
                path = os.path.join(path, 'workspace.png')
            plt.savefig(path)
        else:
            plt.show()
