import dash
from dash import html, callback, Output, Input, dcc
import dash_ag_grid as dag
import logging

from common.app import style, plotter
from volatility.lib import plotter as vol_plotter
from volatility.models.vol_types import VolatilityModelType

import main

logger = logging.Logger(__name__)
dash.register_page(__name__)

DIV_STYLE = style.get_div_style()
DROPDOWN_STYLE = style.get_dropdown_style()
FORM_STYLE = style.get_form_style()
GRAPH_STYLE = style.get_graph_style()
GRID_STYLE = style.get_grid_style()

layout = html.Div([
    dcc.Tabs(children=[
        dcc.Tab(children=html.Div([
            html.Div([
                html.Button('Load Futures', id='load_futures'),
            ], style=FORM_STYLE),
            dcc.Loading(
                id='futures-spreads-status',
                type='default',
            ),
            html.Div(id='futures-spreads'),
        ], style=DIV_STYLE), label='Futures'),
        dcc.Tab(children=html.Div([
            html.Div([
                html.Div([
                    dcc.Dropdown([vst.value for vst in VolatilityModelType], id='model-type-dropdown')
                ], style=DROPDOWN_STYLE),
                html.Button('Load Option Surfaces', id='load_options'),
            ], style=FORM_STYLE),
            dcc.Loading(
                id='option-surfaces-status',
                type='default',
            ),
            html.Div(id='option-surfaces'),
        ], style=DIV_STYLE), label='Options Surfaces'),
    ]),
])

@callback(
    Output(component_id='option-surfaces', component_property='children'),
    Output(component_id='option-surfaces-status', component_property='children'),
    Input(component_id='model-type-dropdown', component_property='value'),
    Input(component_id='load_options', component_property='n_clicks'),
    prevent_initial_call=True,
)
def load_options(model_type: str, *_):
    try:
        tabvals = []
        for vsm in main.get_option_models():
            try:
                vs = vsm.build(model_type)
                if not vs:
                    continue
                vs_fig = vol_plotter.get_surface_figure(*vsm.get_vols_graph(vs))
                gr_fig = vol_plotter.get_surface_figure(*vsm.get_greeks_graph(vs), title='Greeks', mesh_ids=[])
                rows, colnames = vsm.get_calibration_summary(vs)
            except Exception as ex:
                logger.error(f'Exception in Surface {vsm.name}: {ex}')
                continue
            columns = [dict(field=col) for col in colnames]
            records = [dict(zip(colnames, row)) for row in rows]
            tabvals.append(dcc.Tab(children=[
                dcc.Graph(figure=vs_fig, style=GRAPH_STYLE),
                dcc.Graph(figure=gr_fig, style=GRAPH_STYLE),
                dag.AgGrid(
                    rowData=records, columnDefs=columns,
                    **GRID_STYLE
                ),
            ], label=vsm.name))
        return dcc.Tabs(children=tabvals), None
    except Exception as ex:
        logger.critical(f'Exception in Models: {ex}')
        return None, None

@callback(
    Output(component_id='futures-spreads', component_property='children'),
    Output(component_id='futures-spreads-status', component_property='children'),
    Input(component_id='load_futures', component_property='n_clicks'),
)
def load_futures(*_):
    try:
        tabvals = []
        for label, graph_data, kwargs in main.get_futures_data():
            fig = plotter.get_figure(*graph_data, **kwargs)
            tabvals.append(dcc.Tab(children=[
                dcc.Graph(figure=fig, style=GRAPH_STYLE),
            ], label=label))
        return dcc.Tabs(children=tabvals), None
    except Exception as ex:
        logger.critical(f'Exception in Models: {ex}')
        return None, None
