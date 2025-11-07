# layout.py

from dash import html, dcc

import pytz
cet = pytz.timezone("Europe/Berlin")

def create_layout(voice_fig=None, game_fig=None):
    """Erstellt das Layout und setzt (falls übergeben) die vorab berechneten Figuren.

    Parameters
    ----------
    voice_fig : plotly.graph_objects.Figure | None
        Vorberechnete Voice Aktivitäts-Grafik
    game_fig : plotly.graph_objects.Figure | None
        Vorberechnete Game Aktivitäts-Grafik
    """
    return html.Div([
        dcc.Location(id='url', refresh=False),
        dcc.Interval(
            id='interval-refresh',
            interval=10*60*1000,  # 10 Minuten in Millisekunden
            n_intervals=0
        ),
        html.H2("Gnag Stats"),
        html.Div([
            dcc.Graph(id="graph-24h-voice-activity", figure=voice_fig, config={'displayModeBar': False}),
            dcc.Graph(id="graph-24h-game-activity", figure=game_fig, config={'displayModeBar': False}),
        ], id="plots"),
    ])
