import dash
from dash import html, callback, Output, Input, State, dcc
import dash_ag_grid as dag
import datetime as dtm
import pandas as pd

from common.app import style

import main

dash.register_page(__name__, path='/')

DIV_STYLE = style.get_div_style()
GRID_STYLE = style.get_grid_style()

layout = html.Div([
    html.Div([
        dcc.DatePickerSingle(
            id='val-date-picker',
            clearable=True,
        ),
        dcc.Input(id='tenors-input', placeholder='Tenors', type='text', debounce=True),
        html.Button('Load Analytics', id='load_analytics'),
        dcc.Loading(
            id='analytics-table-status',
            type='default',
        ),
    ], style=DIV_STYLE),
    html.Div(id='analytics-table'),
])

def get_tuple_format(fmt: str):
    return {'function': f"params.value[0] d3.format('{fmt}')(params.value[1])"}

@callback(
    Output(component_id='analytics-table', component_property='children'),
    Output(component_id='analytics-table-status', component_property='children'),
    State(component_id='val-date-picker', component_property='date'),
    State(component_id='tenors-input', component_property='value'),
    Input(component_id='load_analytics', component_property='n_clicks'),
)
def load_analytics(date_str: str, tenors_str: str, *_):
    tabvals = []
    date_select = dtm.date.fromisoformat(date_str) if date_str else None
    tenors = [t.strip() for t in tenors_str.split(',')] if tenors_str else None
    table_dict = main.get_analytics_table(date_select, tenors)
    for label, table in table_dict.items():
        values_df = pd.DataFrame(table).reset_index(names=['Name'])
        values_cols = [dict(field=col) for col in values_df.columns]
        for col in values_cols[1:]:
            if label == 'Lag':
                col.update(dict(valueFormatter=get_tuple_format(',.3%')))
            elif col['field'] != 'Price':
                col.update(dict(valueFormatter=style.get_grid_number_format(',.3%')))
        tabvals.append(dcc.Tab([
            dag.AgGrid(
                rowData=values_df.to_dict('records'), columnDefs=values_cols,
                **GRID_STYLE
            ),
        ], label=label))
    return dcc.Tabs(children=tabvals), None
