# layout.py

from dash import html, dcc
import datetime

from webserver.data_provider import DataProvider

def get_first_date(data_provider: DataProvider):
    ts = data_provider._query_first_timestamp()
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts).date()

def create_layout(data_provider: DataProvider = None):
    first_date = get_first_date(data_provider) if data_provider is not None else None
    last_date = datetime.date.today()
    return html.Div(
        [
            html.H2("Gnag Stats"),
            html.Div(
                [
                    html.Button("Reload", id="reload-btn", n_clicks=0),
                    dcc.DatePickerRange(
                        id="timerange-picker",
                        display_format="YYYY-MM-DD",
                        start_date_placeholder_text="Startdatum",
                        end_date_placeholder_text="Enddatum",
                        min_date_allowed=first_date,
                        max_date_allowed=last_date,
                        end_date=last_date,
                        start_date=first_date,
                    ),
                ],
                id="controls",
            ),
            html.Div(
                [
                    dcc.Graph(id="graph-playtime-pie"),
                ],
                id="plots",
            ),
        ]
    )
