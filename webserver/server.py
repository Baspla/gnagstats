from dash import Dash
from flask import Flask

from webserver.data_provider import DataProvider
from webserver.layout import create_layout
from webserver.callbacks import build_initial_figures

def create_app(data_provider: DataProvider):
    server = Flask(__name__)
    app = Dash(__name__, server=server, suppress_callback_exceptions=True, title="Gnag Stats Dashboard")

    figs = build_initial_figures(data_provider)
    app.layout = create_layout(voice_fig=figs.get("voice"), game_fig=figs.get("game"))

    return app
