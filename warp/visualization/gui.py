import datetime

from warp.utils import GlobalImport
# from warp.graph import Ancestry
import warp.globals
import warp.constants as constants


__all__ = ['create_server']


def add_layout(app, workspace):
    # sessions = sorted(workspace.sessions, key=lambda k: float(k))
    sessions = workspace.sessions
    session_timestamps = workspace.session_timestamps
    sessions = sorted(enumerate(sessions), key=lambda k: session_timestamps[k[0]])
    sessions = list(zip(*sessions))[1]  # unzip and take dir names

    dropdown_options = [{
            'label': o, #datetime.fromtimestamp(float(o)), 
            'value': o} 
        for o in sessions] + [{'label': 'new session...', 'value': 'new'}]

    app.layout = html.Div([
        html.P(id='null'),
        html.Div(
            children=[
                html.H1('Pipeline'),
                html.P([
                    'Select a session: ', 
                    dcc.Dropdown(
                        id='session-id-selector', 
                        # value=f'Session: {workspace.home.session_id}', 
                        options=dropdown_options,
                        placeholder=f'Currently loaded: {workspace.home.session_id}',  #datetime.fromtimestamp(float(workspace.home.session_id))}',
                    ),
                ]),
            ],
            className='row',
            style=dict(textAlign='center')),
        html.Div(
            className='row',
            children=[
                html.Div(
                    className='row',
                    style=dict(border='2px black solid'),
                    children=[
                        visdcc.Network(
                            id='pipeline-graph', 
                            options=dict(
                                height='600px', 
                                width='100%',
                                interaction=dict(hover='true'),
                                # layout=dict(hierarchical=dict(
                                #     # enabled='true',
                                #     sortMethod='directed')),
                                physics={
                                    # enabled='true',
                                    'solver': 'forceAtlas2Based',
                                    'forceAtlas2Based.springLength': 200}
                            ),
                        ),
                    ],
                ),
            html.Div(
                id='actions',
                className='row',
                style=dict(textAlign='center'),
                children=[
                    html.H3('Actions'),
                    html.Button('Status', id='node-status-button', style={'margin-left': '15px'}),
                    html.Button('Backfill', id='node-backfill-button', style={'margin-left': '15px'}),
                    html.Button('Build', id='node-build-button', style={'margin-left': '15px'}),
                    html.P(),
                    html.Button('Reset Workspace', id='workspace-reset-button', style={'margin-left': '15px'}),
                    '\tWARNING: this will delete all non-static saved products in the current session.'
                ]
            ),
            html.Div(
                className='row',
                children=[
                    html.H3('Source Code'),
                    dcc.Markdown(id='source-code-markdown'),
                ]
            ),
        ])
    ])


def create_server(graph, workspace, verbose=False):
    with GlobalImport(globals()):
        from datetime import datetime
        from textwrap import dedent as d
        import json
        import time

        import dash
        import dash_core_components as dcc
        import dash_html_components as html
        from dash.dependencies import Input, Output, State
        import visdcc

    external_stylesheets = ['https://cdnjs.cloudflare.com/ajax/libs/vis/4.20.1/vis.min.css']
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    app.title = 'Pipeline'

    add_layout(app, workspace)


    # TODO: I know it's bad to have an everything function, but visdcc seems to be breaking in unexpected ways otherwise
    n_clicks_prev = dict(
        status=0,
        build=0,
        backfill=0,
        workspace=0)
    @app.callback(
        Output('pipeline-graph', 'data'),
        Output('source-code-markdown', 'children'),
        Output('session-id-selector', 'options'),
        Output('session-id-selector', 'placeholder'),
        [Input('pipeline-graph', 'selection'), 
         Input('node-status-button', 'n_clicks'),
         Input('node-build-button', 'n_clicks'),
         Input('node-backfill-button', 'n_clicks'),
         Input('workspace-reset-button', 'n_clicks'),
         Input('session-id-selector', 'value')])
    def display_pipe_status(
        selection, 
        n_clicks_status,
        n_clicks_build,
        n_clicks_backfill,
        n_clicks_workspace,
        session_id,
    ) -> None:
        # sessions = sorted(workspace.sessions, key=lambda k: float(k))
        sessions = workspace.sessions
        session_placeholder = f'Currently loaded: {workspace.home.session_id}'  #datetime.fromtimestamp(float(workspace.home.session_id))}'
        session_dropdown_options = [{
                'label': o,  #datetime.fromtimestamp(float(o)), 
                'value': o} 
            for o in sessions] + [{'label': 'new session...', 'value': 'new'}]
        if (session_id is not None) and (session_id != workspace.home.session_id):
            if session_id == 'new':
                session_id = str(time.time())
                workspace.create_session(session_id)
            else:
                workspace.load_session(session_id)
            session_placeholder = f'Currently loaded: {workspace.home.session_id}'  #datetime.fromtimestamp(float(session_id))}'

        state_change_clicked = False
        source_str = ''
        lineage = set()
        if selection is not None:
            source_str = """### {:s}\n```python\n{:s}\n```"""
            for n in selection['nodes']:
                source_str = source_str.format(n, workspace.view_pipe(n, return_str=True))
                # lineage = set(Ancestry.lineage(graph, node_name=n))
                lineage = set(workspace.PG.get_lineage(nj))

                if (n_clicks_status is not None) and (n_clicks_status > n_clicks_prev['status']):
                    workspace.status(n)
                    n_clicks_prev['status'] = n_clicks_status
                
                if (n_clicks_build is not None) and (n_clicks_build > n_clicks_prev['build']):
                    workspace.build(n)
                    n_clicks_prev['build'] = n_clicks_build
                    state_change_clicked = True
                
                if (n_clicks_backfill is not None) and (n_clicks_backfill > n_clicks_prev['backfill']):
                    workspace.backfill(n)
                    n_clicks_prev['backfill'] = n_clicks_backfill
                    state_change_clicked = True
        
        if (n_clicks_workspace is not None) and (n_clicks_workspace > n_clicks_prev['workspace']):
            session_id = workspace.home.session_id
            workspace.clear_cache(session_id=session_id)
            n_clicks_prev['workspace'] = n_clicks_workspace
            state_change_clicked = True

        nodes = map(
            lambda n: dict(
                id=n, 
                label=n.split('.')[-1], 
                title=n,
                color={
                    'background': 
                        'ForestGreen' if (
                            (workspace._is_pipe_built(n) and 
                            n not in workspace._gap_pipes(n))  # inefficient
                            or workspace.PG.is_source_pipe(n)
                        ) else 'FireBrick',
                    'border'    : 'Orange' if n in lineage else 'Black',
                    'highlight' : {
                        'background': 
                            'LimeGreen' if (
                                (workspace._is_pipe_built(n) and 
                                n not in workspace._gap_pipes(n))  # inefficient
                                or workspace.PG.is_source_pipe(n)
                            ) else 'LightCoral',
                    },
                },
                font=dict(color='Gainsboro'),
                shape='box',
                borderWidth=2), 
            graph.nodes)
        edges = map(
            lambda e: {
                'id'    : str(e), 
                'from'  : e[0], 
                'to'    : e[1], 
                'arrows': 'to', 
                'label' : e[2].split('/')[-1],
                'title' : e[2].format(warp.globals.product_dir()),
                'color' : {
                    'color': 'Black'}}, 
            graph.edges(data='label'))
        data = dict(
            nodes=list(nodes), 
            edges=list(edges))

        return data, source_str, session_dropdown_options, session_placeholder


    app.run_server(
        host=constants.WARP_HOST_NAME, 
        port=constants.WARP_PORT,
        use_reloader=False)
