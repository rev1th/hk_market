import dash
from dash import Dash, html, dcc
from common.app import style


DIV_STYLE = style.get_div_style()

app = Dash(__name__, use_pages=True)
app.layout = html.Div([
    html.H3('HKEX Quant app'),
    html.Div(children=[
        dcc.Link(page['name'], href=page['relative_path'], style=DIV_STYLE)
        for page in dash.page_registry.values()
    ], style=DIV_STYLE),
    dash.page_container
])


if __name__ == '__main__':
    app.run(port=8051)
