from dash import Dash
from flask import Flask


from webserver.data_provider import DataProvider
from webserver.layout import create_layout
from webserver.callbacks import register_callbacks

def create_app(data_provider: DataProvider):
    server = Flask(__name__)
    app = Dash(__name__, server=server, suppress_callback_exceptions=True, title="Gnag Stats Dashboard")
    app.layout = create_layout(data_provider)
    register_callbacks(app, data_provider)

    return app
