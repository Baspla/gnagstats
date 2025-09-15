"""Dash Webserver – nur Layout & Callback Logik.

Die eigentliche Datenbeschaffung & Aggregationen liegen jetzt in
`web_data.py` (WebDataLoader) und `web_aggregations.py`.

Dieses Modul konzentriert sich auf:
 - App-Erstellung
 - Layout Definition
 - Callback Wiring
"""
import logging
import threading
from typing import Optional

import pandas as pd
from waitress import serve

from data_storage.db import Database
from web_data import WebDataLoader
from web_aggregations import (
    agg_playtime_per_game,
    agg_daily_hours,
    agg_heatmap_user_top_games,
    agg_hours_by_hour_of_day,
    recent_players_df,
    agg_daily_peak_voice_users,
    build_user_game_network,
    build_voice_user_network,
    build_voice_24h_timeline,
)


def create_app(database: Database):
    """Create a Dash app with dedicated web queries and rich visualizations."""
    from dash import Dash, html, dcc, Input, Output
    from dash import dash_table
    import plotly.express as px
    import plotly.graph_objects as go
    import networkx as nx

    # Initialize Dash app (Flask server accessible via app.server)
    app = Dash(__name__, title="Gnag Stats")

    # Tailwind über CDN einbinden (schnelle Integration). Für Production kann ein
    # eigener Build in /assets genutzt werden.
    app.index_string = """<!DOCTYPE html>\n<html>\n<head>\n    {%metas%}\n    <title>{%title%}</title>\n    {%favicon%}\n    <script src=\"https://cdn.tailwindcss.com\"></script>\n    <script>tailwind.config = { theme: { extend: { colors: { brand: { 50:'#f5faff',100:'#e0f2ff',200:'#b9e4ff',300:'#89d1ff',400:'#53b9ff',500:'#1d9fff',600:'#007ddb',700:'#005fa8',800:'#004476',900:'#012e50'} } } } };</script>\n    {%css%}\n</head>\n<body class=\"min-h-screen bg-gray-50 text-gray-800 antialiased\">\n    <div id=\"_dash-app-wrapper\" class=\"flex flex-col min-h-screen\">{%app_entry%}\n      <footer class=\"mt-auto text-center text-xs text-gray-400 py-6\">GNAG Stats © <span id=year></span></footer>\n    </div>\n    <script>document.getElementById('year').textContent = new Date().getFullYear();</script>\n    {%config%}\n    {%scripts%}\n    {%renderer%}\n</body>\n</html>"""

    # Zentraler Loader (mit Cache)
    data_loader = WebDataLoader(database)

    # Convenience Wrapper (Kurzformen für alte Namen)
    _load_df_steam_game_activity = data_loader.load_steam_game_activity
    _load_df_discord_voice_activity = data_loader.load_discord_voice_activity
    _load_df_discord_voice_channels = data_loader.load_discord_voice_channels
    _load_df_discord_game_activity = data_loader.load_discord_game_activity

    # Alle Aggregations-/Builder-Funktionen stammen nun aus web_aggregations


    # Build figures once at load time (no auto-refresh)
    df_steam_game_activity_initial = _load_df_steam_game_activity(force=True)
    df_discord_voice_activity_initial = _load_df_discord_voice_activity(force=True)
    df_discord_voice_channels_initial = _load_df_discord_voice_channels(force=True)
    df_discord_game_activity_initial = _load_df_discord_game_activity(force=True)
    fig_voice_24h_timeline_initial = build_voice_24h_timeline(df_discord_voice_activity_initial)

    # Ermittlung globaler Min/Max-Datum für DatePicker
    from datetime import date, timedelta
    
    max_date_global = date.today()
    if not df_steam_game_activity_initial.empty:
        min_date_global = df_steam_game_activity_initial["timestamp_dt"].min().date()
    elif not df_discord_voice_channels_initial.empty:
        min_date_global = df_discord_voice_channels_initial["timestamp_dt"].min().date()
    else:
        min_date_global = max_date_global - timedelta(days=30)

    # Default für Top-N Spiele (wird durch Slider steuerbar)
    default_top_n_games = 40

    # 1) Bar: playtime per game (Initial – wird durch Callback aktualisiert)
    per_game = agg_playtime_per_game(df_steam_game_activity_initial)
    fig_bar = px.bar(per_game.head(default_top_n_games), x="total_hours_played", y="game_name", orientation="h", color="game_name")
    fig_bar.update_layout(showlegend=False, xaxis_title="Stunden", yaxis_title="Spiel")
    # Dynamische Höhe für viele Kategorien, damit die Eltern-Div scrollen kann
    try:
        n_bars = int(min(len(per_game), default_top_n_games)) if per_game is not None else 0
    except Exception:
        n_bars = 0
    fig_bar.update_layout(height=max(500, min(2400, 34 * max(1, n_bars))))
    # Pie: percentage per game (gleicher Datensatz wie Balken)
    fig_pie = px.pie(per_game.head(default_top_n_games), values="total_hours_played", names="game_name")
    fig_pie.update_layout(margin=dict(l=0, r=0, t=0, b=0))

    # 2) Line: total time per day
    daily = agg_daily_hours(df_steam_game_activity_initial)
    # Lücken nicht auf 0 zeichnen: 0-Stunden auf None setzen, damit die Linie unterbrochen wird
    daily_no_gap = daily.copy()
    if not daily_no_gap.empty and "total_hours" in daily_no_gap.columns:
        daily_no_gap.loc[daily_no_gap["total_hours"] <= 0, "total_hours"] = None
    fig_line = px.bar(daily_no_gap, x="date", y="total_hours")
    # Nur horizontale Auswahl erlauben (Selection entlang X), vertikale Achse fix
    fig_line.update_layout(dragmode="select", selectdirection="h")
    fig_line.update_yaxes(fixedrange=True)
    fig_line.update_layout(xaxis_title="Datum", yaxis_title="Stunden pro Tag")

    # 3) Heatmap user vs top games
    heat = agg_heatmap_user_top_games(df_steam_game_activity_initial)
    if heat.empty:
        fig_heat = px.imshow([[0]], labels=dict(x="Spiel", y="Nutzer", color="Stunden"))
        fig_heat.update_xaxes(visible=False)
        fig_heat.update_yaxes(visible=False)
    else:
        fig_heat = px.imshow(
            heat.values,
            x=list(heat.columns),
            y=list(heat.index),
            color_continuous_scale="YlGnBu",
            aspect="auto",
            labels=dict(color="Stunden"),
        )
        fig_heat.update_layout(xaxis_title="Spiel", yaxis_title="Nutzer")
    # Heatmap: kein Zoom/Selection
    fig_heat.update_xaxes(fixedrange=True)
    fig_heat.update_yaxes(fixedrange=True)

    # 4) Vertical bar: hours by hour of day
    by_hour = agg_hours_by_hour_of_day(df_steam_game_activity_initial)
    fig_hour = px.bar(by_hour, x="hour_of_day", y="total_hours")
    # Nur horizontale Auswahl erlauben (Selection entlang X), vertikale Achse fix
    fig_hour.update_layout(dragmode="select", selectdirection="h")
    fig_hour.update_yaxes(fixedrange=True)
    fig_hour.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

    # 4b) Tabelle: Aktive Spieler letzte 5.5 Minuten
    recent_players_initial = recent_players_df(df_steam_game_activity_initial)
    recent_players_data_initial = recent_players_initial.to_dict("records")

    # 5) Punkte: Tages-Peaks der Gesamtnutzer in Sprachkanälen
    voice_users_daily_peak = agg_daily_peak_voice_users(df_discord_voice_channels_initial)
    # Balken statt Punkte für Tages-Peaks
    fig_voice_users = px.bar(voice_users_daily_peak, x="date", y="total_users")
    # Nur horizontale Auswahl erlauben (Selection entlang X), vertikale Achse fix
    fig_voice_users.update_layout(dragmode="select", selectdirection="h")
    fig_voice_users.update_yaxes(fixedrange=True)
    fig_voice_users.update_layout(xaxis_title="Datum", yaxis_title="Peak Nutzer/Tag")

    # 6) Netzwerkgraph: Nutzer <-> Spiele
    # Netzwerk nur auf Top-N Spiele beschränken
    top_games_initial = per_game.head(default_top_n_games)["game_name"].tolist() if not per_game.empty else []
    network_df_initial = (
        df_steam_game_activity_initial[
            df_steam_game_activity_initial["game_name"].isin(top_games_initial)
        ] if top_games_initial else df_steam_game_activity_initial
    )
    fig_network = build_user_game_network(network_df_initial, min_game_hours=0.0)

    app.layout = html.Div(
        [
            # Header / Controls
            html.Div(
                [
                    html.H1("GNAG Stats", className="text-3xl font-bold tracking-tight text-brand-700 text-center w-full"),
                    html.Div(
                        [
                            html.Label("Zeitraum:"),
                            dcc.DatePickerRange(
                                id="date-range",
                                min_date_allowed=min_date_global,
                                max_date_allowed=max_date_global,
                                start_date=min_date_global,
                                end_date=max_date_global,
                                first_day_of_week=1,
                                display_format="YYYY-MM-DD",
                                minimum_nights=0,
                            ),
                            html.Div(
                                [
                                    html.Label("Top-N Spiele:"),
                                    dcc.Slider(
                                        id="top-n-games-slider",
                                        min=3,
                                        max=40,
                                        step=1,
                                        value=default_top_n_games,
                                        marks={3:"3",5:"5",10:"10",20:"20",30:"30",40:"40"},
                                        tooltip={"placement": "bottom", "always_visible": False},
                                    ),
                                ],
                                className="flex-1 min-w-[260px]",
                            ),
                        ],
                        className="flex flex-wrap items-center gap-4 w-full mb-4",
                    ),
                ],
                className="sticky top-0 bg-white z-50 pt-4 px-8 pb-2 shadow-sm backdrop-blur supports-[backdrop-filter]:bg-white/80 border-b border-gray-200",
            ),

            # Spielzeit pro Spiel
            html.Div(
                [
                    html.H2("Spielzeit pro Spiel", id="hdr-playtime-per-game", className="text-xl font-semibold mb-4"),
                    html.Div(
                        [
                            html.Div(
                                dcc.Graph(id="graph-playtime-per-game", figure=fig_bar),
                                className="flex-1 min-w-0 h-[600px] overflow-y-auto overflow-x-hidden md:w-full",
                            ),
                            html.Div(
                                dcc.Graph(id="graph-playtime-pie", figure=fig_pie),
                                className="flex-1 min-w-0 md:w-full",
                            ),
                        ],
                        className="flex flex-col md:flex-row gap-4 items-stretch",
                    ),
                ],
                className="mb-8",
            ),

            # Gesamtzeit pro Tag
            html.Div(
                [
                    html.H2("Gesamtzeit pro Tag", id="hdr-daily", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-daily-time",
                        figure=fig_line,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": ["zoom2d","pan2d","lasso2d","zoomIn2d","zoomOut2d","autoScale2d","resetScale2d"],
                        },
                    ),
                ],
                className="mb-8",
            ),

            # Heatmap
            html.Div(
                [
                    html.H2("Spielzeit Nutzer vs. Top-Spiele", id="hdr-heatmap-user-game", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-heatmap-user-game",
                        figure=fig_heat,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": ["zoom2d","pan2d","select2d","lasso2d","zoomIn2d","zoomOut2d","autoScale2d","resetScale2d"],
                        },
                    ),
                ],
                className="mb-8",
            ),

            # Stunden nach Tagesstunde + Aktive Gamer
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H2("Spielzeit nach Tagesstunde", id="hdr-by-hour", className="text-xl font-semibold"),
                                    dcc.Graph(
                                        id="graph-by-hour",
                                        figure=fig_hour,
                                        config={
                                            "displaylogo": False,
                                            "scrollZoom": False,
                                            "modeBarButtonsToRemove": ["zoom2d","pan2d","lasso2d","zoomIn2d","zoomOut2d","autoScale2d","resetScale2d"],
                                        },
                                    ),
                                ],
                                className="flex flex-col gap-2 flex-1 min-w-0 md:w-full",
                            ),
                            html.Div(
                                [
                                    html.H2("Aktive Gamer", className="text-xl font-semibold"),
                                    dash_table.DataTable(
                                        id="table-recent-players",
                                        columns=[{"name": "Name", "id": "name"},{"name": "Spiel", "id": "game"}],
                                        data=recent_players_data_initial,
                                        style_table={"height": "600px", "overflowY": "auto"},
                                        style_cell={"padding": "4px 8px", "textAlign": "left", "fontSize": "13px"},
                                        style_header={"backgroundColor": "#f5f5f5", "fontWeight": "bold"},
                                        page_action="none",
                                        sort_action="native",
                                        filter_action="none",
                                    ),
                                ],
                                className="flex flex-col flex-1 min-w-0 md:w-full",
                            ),
                        ],
                        className="flex flex-col md:flex-row gap-4 items-stretch mb-8",
                    ),
                ]
            ),

            # Voice Nutzer Peaks
            html.Div(
                [
                    html.H2("Tages-Peaks der Gesamtnutzer in Sprachkanälen", id="hdr-voice-users-over-time", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-voice-users-over-time",
                        figure=fig_voice_users,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": ["zoom2d","pan2d","lasso2d","zoomIn2d","zoomOut2d","autoScale2d","resetScale2d"],
                        },
                    ),
                ],
                className="mb-8",
            ),

            # Nutzer-Spiel Netzwerk
            html.Div(
                [
                    html.H2("Nutzer–Spiel Netzwerk", id="hdr-user-game-network", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-user-game-network",
                        figure=fig_network,
                        config={"displaylogo": False, "scrollZoom": False, "displayModeBar": True},
                        className="h-[700px]",
                    ),
                ],
                className="mb-8",
            ),

            # Voice User Netzwerk
            html.Div(
                [
                    html.H2("Discord Voice Nutzer Netzwerk", id="hdr-voice-user-network", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-voice-user-network",
                        figure=build_voice_user_network(df_discord_voice_activity_initial),
                        config={"displaylogo": False, "scrollZoom": False, "displayModeBar": True},
                        className="h-[700px]",
                    ),
                ],
                className="mb-8",
            ),

            # 24h Timeline
            html.Div(
                [
                    html.H2("Voice Aktivität (letzte 24h)", id="hdr-voice-24h-timeline", className="text-xl font-semibold mb-4"),
                    dcc.Graph(
                        id="graph-voice-24h-timeline",
                        figure=fig_voice_24h_timeline_initial,
                        config={
                            "displaylogo": False,
                            "scrollZoom": True,
                            "modeBarButtonsToRemove": ["zoom2d","pan2d","lasso2d","zoomIn2d","zoomOut2d","autoScale2d","resetScale2d"],
                        },
                        className="h-[600px]",
                    ),
                ],
                className="mb-8",
            ),
        ],
        className="container mx-auto px-4 lg:px-8 font-sans my-8",
    )

    # Callback zur Aktualisierung aller Diagramme anhand des gewählten Datumsbereichs
    @app.callback(
        [
            Output("graph-playtime-per-game", "figure"),
            Output("graph-playtime-pie", "figure"),
            Output("graph-daily-time", "figure"),
            Output("graph-heatmap-user-game", "figure"),
            Output("graph-by-hour", "figure"),
            Output("graph-voice-users-over-time", "figure"),
            Output("graph-user-game-network", "figure"),
            Output("graph-voice-user-network", "figure"),
            Output("graph-voice-24h-timeline", "figure"),
            Output("table-recent-players", "data"),
        ],
        [Input("date-range", "start_date"), Input("date-range", "end_date"), Input("top-n-games-slider", "value")],
    )
    def _update_all(start_date, end_date, top_n):  # noqa: D401
        """Update all graphs when date range or top-n changes."""
        import plotly.express as px  # Lokal, falls Dash Hot Reload
        import plotly.graph_objects as go
        df_steam = _load_df_steam_game_activity()
        df_voice_channels = _load_df_discord_voice_channels()
        df_voice_activity = _load_df_discord_voice_activity()

        # Filterfunktion
        def _filter(df: pd.DataFrame):
            if df.empty or start_date is None or end_date is None or "timestamp_dt" not in df.columns:
                return df
            try:
                start = pd.to_datetime(start_date)
                end = pd.to_datetime(end_date) + pd.Timedelta(days=1)  # end_date inklusiv
            except Exception:
                return df
            return df[(df["timestamp_dt"] >= start) & (df["timestamp_dt"] < end)]

        df_steam_f = _filter(df_steam)
        df_voice_channels_f = _filter(df_voice_channels)

        # Re-Aggregationen (Top-N Spiele bestimmen)
        per_game_f = agg_playtime_per_game(df_steam_f)
        if top_n is None:
            top_n = default_top_n_games
        try:
            top_n_int = int(top_n)
        except Exception:
            top_n_int = default_top_n_games
        top_games = per_game_f.head(top_n_int)["game_name"].tolist() if not per_game_f.empty else []
        if top_games:
            df_steam_top = df_steam_f[df_steam_f["game_name"].isin(top_games)]
        else:
            df_steam_top = df_steam_f

        per_game_top = per_game_f.head(top_n_int)
        fig_bar_f = px.bar(per_game_top, x="total_hours_played", y="game_name", orientation="h", color="game_name")
        fig_bar_f.update_layout(showlegend=False, xaxis_title="Stunden", yaxis_title="Spiel")
        try:
            n_bars_local = int(min(len(per_game_top), top_n_int)) if per_game_top is not None else 0
        except Exception:
            n_bars_local = 0
        fig_bar_f.update_layout(height=max(500, min(2400, 34 * max(1, n_bars_local))))

        fig_pie_f = px.pie(per_game_top, values="total_hours_played", names="game_name")
        fig_pie_f.update_layout(margin=dict(l=0, r=0, t=0, b=0))

        daily_f = agg_daily_hours(df_steam_top)
        daily_no_gap_f = daily_f.copy()
        if not daily_no_gap_f.empty and "total_hours" in daily_no_gap_f.columns:
            daily_no_gap_f.loc[daily_no_gap_f["total_hours"] <= 0, "total_hours"] = None
        fig_line_f = px.bar(daily_no_gap_f, x="date", y="total_hours")
        fig_line_f.update_layout(dragmode="select", selectdirection="h")
        fig_line_f.update_yaxes(fixedrange=True)
        fig_line_f.update_layout(xaxis_title="Datum", yaxis_title="Stunden pro Tag")

        heat_f = agg_heatmap_user_top_games(df_steam_top, top_games=top_n_int)
        if heat_f.empty:
            fig_heat_f = px.imshow([[0]], labels=dict(x="Spiel", y="Nutzer", color="Stunden"))
            fig_heat_f.update_xaxes(visible=False)
            fig_heat_f.update_yaxes(visible=False)
        else:
            fig_heat_f = px.imshow(
                heat_f.values,
                x=list(heat_f.columns),
                y=list(heat_f.index),
                color_continuous_scale="YlGnBu",
                aspect="auto",
                labels=dict(color="Stunden"),
            )
            fig_heat_f.update_layout(xaxis_title="Spiel", yaxis_title="Nutzer")
        fig_heat_f.update_xaxes(fixedrange=True)
        fig_heat_f.update_yaxes(fixedrange=True)

        by_hour_f = agg_hours_by_hour_of_day(df_steam_top)
        fig_hour_f = px.bar(by_hour_f, x="hour_of_day", y="total_hours")
        fig_hour_f.update_layout(dragmode="select", selectdirection="h")
        fig_hour_f.update_yaxes(fixedrange=True)
        fig_hour_f.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

        voice_users_daily_peak_f = agg_daily_peak_voice_users(df_voice_channels_f)
        fig_voice_users_f = px.bar(voice_users_daily_peak_f, x="date", y="total_users")
        fig_voice_users_f.update_layout(dragmode="select", selectdirection="h")
        fig_voice_users_f.update_yaxes(fixedrange=True)
        fig_voice_users_f.update_layout(xaxis_title="Datum", yaxis_title="Peak Nutzer/Tag")

        network_df = df_steam_top
        fig_network_f = build_user_game_network(network_df, min_game_hours=0.0)
        df_voice_activity_f = _filter(df_voice_activity)
        fig_voice_user_network_f = build_voice_user_network(df_voice_activity_f)
        fig_voice_24h_timeline_f = build_voice_24h_timeline(df_voice_activity)

        recent_players_latest = recent_players_df(df_steam)
        recent_players_data = recent_players_latest.to_dict("records") if not recent_players_latest.empty else []

        return [
            fig_bar_f,
            fig_pie_f,
            fig_line_f,
            fig_heat_f,
            fig_hour_f,
            fig_voice_users_f,
            fig_network_f,
            fig_voice_user_network_f,
            fig_voice_24h_timeline_f,
            recent_players_data,
        ]

    return app


def run_webserver(database, host="127.0.0.1", port=5000):
    app = create_app(database)
    logging.info(f"Starting Dash web server at http://{host}:{port}/ using Waitress.")
    # Serve the underlying Flask server of the Dash app
    threading.Thread(
        target=serve,
        args=(app.server,),
        kwargs={"host": host, "port": port},
        daemon=True,
    ).start()
