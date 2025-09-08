import logging
import threading
import time


def create_app(database):
    """Create a Dash app that shows the same stats as the previous Flask/Jinja page."""
    from dash import Dash, html, dcc, dash_table, Input, Output
    from newsletter_creator import seconds_to_human_readable

    # Initialize Dash app (Flask server accessible via app.server)
    app = Dash(__name__, title="Statistik-Übersicht")

    def _fetch_stats():
        now = int(time.time())
        week_ago = now - 7 * 24 * 3600
        steam_most_played = database.get_steam_most_played_games(week_ago, now)
        discord_busiest_channels = database.get_discord_busiest_voice_channels(week_ago, now)
        return steam_most_played, discord_busiest_channels

    # Define columns for DataTables
    steam_columns = [
        {"name": "Spiel", "id": "game"},
        {"name": "Gespielte Zeit", "id": "played_time"},
    ]
    discord_columns = [
        {"name": "Channel", "id": "channel"},
        {"name": "Gesamte Zeit", "id": "total_time"},
    ]

    app.layout = html.Div(
        [
            html.H1("Statistik-Übersicht"),
            html.Section(
                [
                    html.H2("Meistgespielte Steam-Spiele (letzte 7 Tage)"),
                    dash_table.DataTable(
                        id="steam-table",
                        columns=steam_columns,
                        data=[],
                        page_size=10,
                        style_table={"width": "100%", "marginTop": "0.5em"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_header={"backgroundColor": "#f4f4f4", "fontWeight": "bold"},
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Section(
                [
                    html.H2("Aktivste Discord-Voice-Channels (letzte 7 Tage)"),
                    dash_table.DataTable(
                        id="discord-table",
                        columns=discord_columns,
                        data=[],
                        page_size=10,
                        style_table={"width": "100%", "marginTop": "0.5em"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_header={"backgroundColor": "#f4f4f4", "fontWeight": "bold"},
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            dcc.Interval(id="refresh-interval", interval=60_000, n_intervals=0),
        ],
        style={"fontFamily": "Arial, sans-serif", "margin": "2em"},
    )

    @app.callback(
        Output("steam-table", "data"),
        Output("discord-table", "data"),
        Input("refresh-interval", "n_intervals"),
    )
    def _refresh_tables(_):  # noqa: D401
        steam_most_played, discord_busiest_channels = _fetch_stats()

        # steam_most_played structure used previously: entry[0] = game name, entry[2] = seconds played
        steam_rows = [
            {
                "game": entry[0],
                "played_time": seconds_to_human_readable(entry[2] if len(entry) > 2 else 0),
            }
            for entry in (steam_most_played or [])
        ]

        # discord_busiest_channels structure: (name, seconds)
        discord_rows = [
            {"channel": name, "total_time": seconds_to_human_readable(seconds)}
            for (name, seconds) in (discord_busiest_channels or [])
        ]

        return steam_rows, discord_rows

    return app


def run_webserver(database, host="127.0.0.1", port=5000):
    app = create_app(database)
    logging.info(f"Starting Dash web server at http://{host}:{port}/ using Waitress.")
    from waitress import serve
    # Serve the underlying Flask server of the Dash app
    threading.Thread(
        target=serve,
        args=(app.server,),
        kwargs={"host": host, "port": port},
        daemon=True,
    ).start()
