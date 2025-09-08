import logging
import threading
import time
from typing import Optional

import pandas as pd

from db import Database


def create_app(database: Database):
    """Create a Dash app with dedicated web queries and rich visualizations."""
    from dash import Dash, html, dcc, Input, Output, State
    import plotly.express as px

    # Initialize Dash app (Flask server accessible via app.server)
    app = Dash(__name__, title="GNAG Stats – Übersicht")

    # Cached DataFrame and last refresh timestamp
    df_cache: Optional[pd.DataFrame] = None
    last_loaded: Optional[int] = None

    def _load_dataframe(force: bool = False) -> pd.DataFrame:
        nonlocal df_cache, last_loaded
        now = int(time.time())
        # Refresh every 5 minutes or on demand
        if (not force) and df_cache is not None and last_loaded and (now - last_loaded < 300):
            return df_cache

        rows = database.web_query_get_steam_game_activity()
        # rows: [(timestamp, steam_id, game_name, collection_interval), ...]
        df = pd.DataFrame(rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"]) if rows else pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval"]) 
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            # minutes per snapshot falls back to 5 if collection_interval missing
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
        df_cache = df
        last_loaded = now
        logging.debug(f"Loaded DataFrame for web app with {len(df)} rows.")
        return df

    def _agg_playtime_per_game(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["game_name", "total_hours_played"])
        # One snapshot = collection_interval seconds; convert to hours
        grouped = df.groupby("game_name").size().reset_index(name="snapshots")
        grouped["total_minutes_played"] = grouped["snapshots"] * df["minutes_per_snapshot"].median()
        grouped["total_hours_played"] = grouped["total_minutes_played"] / 60.0
        return grouped.sort_values("total_hours_played", ascending=False)

    def _agg_daily_hours(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["date", "total_hours"])
        ts = df.set_index("timestamp_dt")
        # Count snapshots per day, convert to hours
        daily = ts.resample("D").size().rename("snapshots").to_frame()
        daily["total_minutes"] = daily["snapshots"] * df["minutes_per_snapshot"].median()
        daily["total_hours"] = daily["total_minutes"] / 60.0
        daily = daily.reset_index().rename(columns={"timestamp_dt": "date"})
        return daily

    def _agg_heatmap_user_top_games(df: pd.DataFrame, top_users: int = 10, top_games: int = 15) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        # Hours per user per game
        cubed = df.groupby(["steam_id", "game_name"]).size().reset_index(name="snapshots")
        cubed["hours"] = (cubed["snapshots"] * df["minutes_per_snapshot"].median()) / 60.0
        # Top users/games by total hours
        top_users_idx = (
            cubed.groupby("steam_id")["hours"].sum().sort_values(ascending=False).head(top_users).index
        )
        top_games_idx = (
            cubed.groupby("game_name")["hours"].sum().sort_values(ascending=False).head(top_games).index
        )
        filtered = cubed[cubed["steam_id"].isin(top_users_idx) & cubed["game_name"].isin(top_games_idx)]
        pivot = filtered.pivot_table(index="steam_id", columns="game_name", values="hours", fill_value=0.0)
        return pivot

    def _agg_hours_by_hour_of_day(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["hour_of_day", "total_hours"])        
        tmp = df.copy()
        tmp["hour_of_day"] = tmp["timestamp_dt"].dt.hour
        hourly = tmp.groupby("hour_of_day").size().rename("snapshots").to_frame().reset_index()
        hourly["total_minutes"] = hourly["snapshots"] * df["minutes_per_snapshot"].median()
        hourly["total_hours"] = hourly["total_minutes"] / 60.0
        return hourly.sort_values("hour_of_day")

    app.layout = html.Div(
        [
            html.H1("GNAG Stats – Übersicht"),
            html.Div(
                [
                    html.Button("Neu laden", id="btn-refresh", n_clicks=0, style={"marginRight": "1em"}),
                    dcc.Interval(id="auto-refresh", interval=300_000, n_intervals=0),  # 5 min
                    html.Span(id="status-text", style={"marginLeft": "1em", "color": "#666"}),
                ],
                style={"marginBottom": "1em"},
            ),
            html.Div(
                [
                    html.H2("Spielzeit pro Spiel (Balken)"),
                    dcc.Graph(id="graph-playtime-per-game"),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Gesamtzeit pro Tag (Linie)", id="hdr-daily"),
                    dcc.Graph(id="graph-daily-time"),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Heatmap: Spielzeit Nutzer vs. Top-Spiele"),
                    dcc.Graph(id="graph-heatmap-user-game"),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Spielzeit nach Tagesstunde (Balken)"),
                    dcc.Graph(id="graph-by-hour"),
                ],
                style={"marginBottom": "2em"},
            ),
        ],
        style={"fontFamily": "Arial, sans-serif", "margin": "2em"},
    )

    @app.callback(
        Output("graph-playtime-per-game", "figure"),
        Output("graph-daily-time", "figure"),
        Output("graph-heatmap-user-game", "figure"),
        Output("graph-by-hour", "figure"),
        Output("status-text", "children"),
        Input("auto-refresh", "n_intervals"),
        Input("btn-refresh", "n_clicks"),
        prevent_initial_call=False,
    )
    def _refresh_graphs(_intervals, _clicks):  # noqa: D401
        df = _load_dataframe(force=_clicks and _clicks > 0)

        # 1) Bar: playtime per game
        per_game = _agg_playtime_per_game(df)
        fig_bar = px.bar(per_game.head(40), x="total_hours_played", y="game_name", orientation="h", color="game_name")
        fig_bar.update_layout(showlegend=False, xaxis_title="Stunden", yaxis_title="Spiel")

        # 2) Line: total time per day
        daily = _agg_daily_hours(df)
        fig_line = px.line(daily, x="date", y="total_hours")
        fig_line.update_layout(xaxis_title="Datum", yaxis_title="Stunden pro Tag")

        # 3) Heatmap user vs top games
        heat = _agg_heatmap_user_top_games(df)
        if heat.empty:
            fig_heat = px.imshow([[0]], labels=dict(x="Spiel", y="Nutzer", color="Stunden"))
            fig_heat.update_xaxes(visible=False)
            fig_heat.update_yaxes(visible=False)
        else:
            fig_heat = px.imshow(heat.values, x=list(heat.columns), y=list(heat.index), color_continuous_scale="YlGnBu", aspect="auto", labels=dict(color="Stunden"))
            fig_heat.update_layout(xaxis_title="Spiel", yaxis_title="Nutzer")

        # 4) Vertical bar: hours by hour of day
        by_hour = _agg_hours_by_hour_of_day(df)
        fig_hour = px.bar(by_hour, x="hour_of_day", y="total_hours")
        fig_hour.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

        status = f"Zuletzt geladen: {time.strftime('%Y-%m-%d %H:%M:%S')} – Zeilen: {len(df)}"
        return fig_bar, fig_line, fig_heat, fig_hour, status

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
