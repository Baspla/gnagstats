# layout.py

from dash import html, dcc
import datetime

from webserver.data_provider import DataProvider
import pytz
cet = pytz.timezone("Europe/Berlin")

def get_first_date(data_provider: DataProvider):
    ts = data_provider._query_first_timestamp()
    if ts is None:
        return None
    return datetime.datetime.fromtimestamp(ts, cet).date()

def create_layout(data_provider: DataProvider = None):
    first_date = get_first_date(data_provider) if data_provider is not None else None
    last_date = datetime.datetime.now(cet).date()
    return html.Div(
        [
            dcc.Location(id='url', refresh=False),
            html.H2("Gnag Stats"),
            html.Div(
                [
                    dcc.DatePickerRange(
                        id="timerange-picker",
                        display_format="YYYY-MM-DD",
                        start_date_placeholder_text="Startdatum",
                        end_date_placeholder_text="Enddatum",
                        min_date_allowed=first_date,
                        max_date_allowed=last_date,
                        end_date=last_date,
                        start_date=first_date,
                        first_day_of_week=1,
                    ),
                    html.Button("Aktualisieren", id="reload-btn", n_clicks=0),
                ],
                id="controls",
            ),
            html.Div(
                [
                    dcc.Graph(id="graph-playtime-pie"),
                    dcc.Graph(id="graph-24h-voice-activity"),
                    dcc.Graph(id="graph-24h-game-activity"),
                    dcc.Graph(id="network-voice-activity"),
                    dcc.Graph(id="network-game-activity"),
                    dcc.Graph(id="heatmap-user-game-activity"),
                    dcc.Graph(id="recent-activity-list"),
                ],
                id="plots",
            ),
        ]
    )
