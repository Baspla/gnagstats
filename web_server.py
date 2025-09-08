import logging
import threading
import time
from typing import Optional

import pandas as pd

from db import Database
from json_data import load_json_data, get_steam_id_to_name_map, get_discord_id_to_name_map
from config import WEB_CACHE_TTL_MINUTES, JSON_DATA_PATH
from waitress import serve


def create_app(database: Database):
    """Create a Dash app with dedicated web queries and rich visualizations."""
    from dash import Dash, html, dcc
    import plotly.express as px

    # Initialize Dash app (Flask server accessible via app.server)
    app = Dash(__name__, title="Gnag Stats")

    # Cached DataFrames and last refresh timestamps
    df_steam_cache: Optional[pd.DataFrame] = None
    df_discord_voice_activity_cache: Optional[pd.DataFrame] = None
    df_discord_voice_channels_cache: Optional[pd.DataFrame] = None
    df_discord_game_activity_cache: Optional[pd.DataFrame] = None
    last_loaded_steam: Optional[int] = None
    last_loaded_dva: Optional[int] = None
    last_loaded_dvc: Optional[int] = None
    last_loaded_dga: Optional[int] = None

    def _load_df_steam_game_activity(force: bool = False) -> pd.DataFrame:
        nonlocal df_steam_cache, last_loaded_steam
        now = int(time.time())
        # Refresh every N minutes or on demand
        ttl_seconds = max(1, WEB_CACHE_TTL_MINUTES) * 60
        if (not force) and df_steam_cache is not None and last_loaded_steam and (now - last_loaded_steam < ttl_seconds):
            return df_steam_cache

        rows = database.web_query_get_steam_game_activity()
        # rows: [(timestamp, steam_id, game_name, collection_interval), ...]
        df = (
            pd.DataFrame(rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            # minutes per snapshot falls back to 5 if collection_interval missing
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            # Map steam_id to display name from JSON
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_steam_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            if id_map:
                df["user_name"] = df["steam_id"].astype(str).map(id_map).fillna(df["steam_id"].astype(str))
            else:
                df["user_name"] = df["steam_id"].astype(str)
        df_steam_cache = df
        last_loaded_steam = now
        logging.debug(f"Loaded DataFrame for web app with {len(df)} rows.")
        return df

    def _load_df_discord_voice_activity(force: bool = False) -> pd.DataFrame:
        nonlocal df_discord_voice_activity_cache, last_loaded_dva
        now = int(time.time())
        ttl_seconds = max(1, WEB_CACHE_TTL_MINUTES) * 60
        if (not force) and df_discord_voice_activity_cache is not None and last_loaded_dva and (now - last_loaded_dva < ttl_seconds):
            return df_discord_voice_activity_cache

        rows = database.web_query_get_discord_voice_activity()
        # rows: [(timestamp, discord_id, channel_name, guild_id, collection_interval), ...]
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            # Map discord_id to display name from JSON
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_discord_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            if id_map:
                df["user_name"] = df["discord_id"].astype(str).map(id_map).fillna(df["discord_id"].astype(str))
            else:
                df["user_name"] = df["discord_id"].astype(str)
        df_discord_voice_activity_cache = df
        last_loaded_dva = now
        logging.debug(f"Loaded Discord voice activity DataFrame with {len(df)} rows.")
        return df

    def _load_df_discord_voice_channels(force: bool = False) -> pd.DataFrame:
        nonlocal df_discord_voice_channels_cache, last_loaded_dvc
        now = int(time.time())
        ttl_seconds = max(1, WEB_CACHE_TTL_MINUTES) * 60
        if (not force) and df_discord_voice_channels_cache is not None and last_loaded_dvc and (now - last_loaded_dvc < ttl_seconds):
            return df_discord_voice_channels_cache

        rows = database.web_query_get_discord_voice_channels()
        # rows: [(timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval), ...]
        df = (
            pd.DataFrame(rows, columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
        df_discord_voice_channels_cache = df
        last_loaded_dvc = now
        logging.debug(f"Loaded Discord voice channels DataFrame with {len(df)} rows.")
        return df

    def _load_df_discord_game_activity(force: bool = False) -> pd.DataFrame:
        nonlocal df_discord_game_activity_cache, last_loaded_dga
        now = int(time.time())
        ttl_seconds = max(1, WEB_CACHE_TTL_MINUTES) * 60
        if (not force) and df_discord_game_activity_cache is not None and last_loaded_dga and (now - last_loaded_dga < ttl_seconds):
            return df_discord_game_activity_cache

        rows = database.web_query_get_discord_game_activity()
        # rows: [(timestamp, discord_id, game_name, collection_interval), ...]
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "game_name", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            # Map discord_id to display name from JSON
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_discord_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            if id_map:
                df["user_name"] = df["discord_id"].astype(str).map(id_map).fillna(df["discord_id"].astype(str))
            else:
                df["user_name"] = df["discord_id"].astype(str)
        df_discord_game_activity_cache = df
        last_loaded_dga = now
        logging.debug(f"Loaded Discord game activity DataFrame with {len(df)} rows.")
        return df

    def _agg_heatmap_user_top_games(df: pd.DataFrame, top_users: int = 10, top_games: int = 15) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        # Hours per user per game
        cubed = df.groupby(["user_name", "game_name"]).size().reset_index(name="snapshots")
        cubed["hours"] = (cubed["snapshots"] * df["minutes_per_snapshot"].median()) / 60.0
        # Top users/games by total hours
        top_users_idx = (
            cubed.groupby("user_name")["hours"].sum().sort_values(ascending=False).head(top_users).index
        )
        top_games_idx = (
            cubed.groupby("game_name")["hours"].sum().sort_values(ascending=False).head(top_games).index
        )
        filtered = cubed[cubed["user_name"].isin(top_users_idx) & cubed["game_name"].isin(top_games_idx)]
        pivot = filtered.pivot_table(index="user_name", columns="game_name", values="hours", fill_value=0.0)
        return pivot
    
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


    def _agg_hours_by_hour_of_day(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["hour_of_day", "total_hours"])        
        tmp = df.copy()
        tmp["hour_of_day"] = tmp["timestamp_dt"].dt.hour
        hourly = tmp.groupby("hour_of_day").size().rename("snapshots").to_frame().reset_index()
        hourly["total_minutes"] = hourly["snapshots"] * df["minutes_per_snapshot"].median()
        hourly["total_hours"] = hourly["total_minutes"] / 60.0
        return hourly.sort_values("hour_of_day")

    def _agg_total_voice_users_over_time(df: pd.DataFrame) -> pd.DataFrame:
        """Summe der Nutzer über alle Sprachkanäle je Zeitstempel."""
        if df.empty:
            return pd.DataFrame(columns=["timestamp_dt", "total_users"])        
        tmp = df.copy()
        # user_count kann float/NaN sein -> sicher in int konvertieren
        tmp["user_count"] = pd.to_numeric(tmp["user_count"], errors="coerce").fillna(0).astype(int)
        aggregated = (
            tmp.groupby("timestamp_dt")["user_count"].sum()
            .rename("total_users").to_frame().reset_index()
            .sort_values("timestamp_dt")
        )
        return aggregated

    # Build figures once at load time (no auto-refresh)
    df_steam_game_activity_initial = _load_df_steam_game_activity(force=True)
    # Preload other DataFrames as well
    df_discord_voice_activity_initial = _load_df_discord_voice_activity(force=True)
    df_discord_voice_channels_initial = _load_df_discord_voice_channels(force=True)
    df_discord_game_activity_initial = _load_df_discord_game_activity(force=True)

    # 1) Bar: playtime per game
    per_game = _agg_playtime_per_game(df_steam_game_activity_initial)
    fig_bar = px.bar(per_game.head(40), x="total_hours_played", y="game_name", orientation="h", color="game_name")
    fig_bar.update_layout(showlegend=False, xaxis_title="Stunden", yaxis_title="Spiel")
    # Dynamische Höhe für viele Kategorien, damit die Eltern-Div scrollen kann
    try:
        n_bars = int(min(len(per_game), 40)) if per_game is not None else 0
    except Exception:
        n_bars = 0
    fig_bar.update_layout(height=max(500, min(2400, 34 * max(1, n_bars))))
    # Pie: percentage per game (gleicher Datensatz wie Balken)
    fig_pie = px.pie(per_game.head(40), values="total_hours_played", names="game_name")
    fig_pie.update_layout(margin=dict(l=0, r=0, t=0, b=0))

    # 2) Line: total time per day
    daily = _agg_daily_hours(df_steam_game_activity_initial)
    fig_line = px.line(daily, x="date", y="total_hours")
    fig_line.update_layout(xaxis_title="Datum", yaxis_title="Stunden pro Tag")

    # 3) Heatmap user vs top games
    heat = _agg_heatmap_user_top_games(df_steam_game_activity_initial)
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

    # 4) Vertical bar: hours by hour of day
    by_hour = _agg_hours_by_hour_of_day(df_steam_game_activity_initial)
    fig_hour = px.bar(by_hour, x="hour_of_day", y="total_hours")
    fig_hour.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

    # 5) Line: Gesamtanzahl Nutzer in Sprachkanälen über die Zeit
    voice_users_over_time = _agg_total_voice_users_over_time(df_discord_voice_channels_initial)
    fig_voice_users = px.line(voice_users_over_time, x="timestamp_dt", y="total_users")
    fig_voice_users.update_layout(xaxis_title="Zeit", yaxis_title="Gesamtnutzer in Sprachkanälen")

    app.layout = html.Div(
        [
            html.H1("GNAG Stats", style={"textAlign": "center"}),
            html.Div(
                [
                    html.H2("Spielzeit pro Spiel", id="hdr-playtime-per-game"),
                    html.Div(
                        [
                            html.Div(
                                dcc.Graph(id="graph-playtime-per-game", figure=fig_bar),
                                style={
                                    "flex": "1",
                                    "minWidth": 0,
                                    # Nur vertikal scrollen, um viele Labels zu handeln
                                    "height": "600px",
                                    "overflowY": "auto",
                                    "overflowX": "hidden",
                                },
                            ),
                            html.Div(
                                dcc.Graph(id="graph-playtime-pie", figure=fig_pie),
                                style={"flex": "1", "minWidth": 0},
                            ),
                        ],
                        style={"display": "flex", "gap": "1rem", "alignItems": "stretch"},
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Gesamtzeit pro Tag", id="hdr-daily"),
                    dcc.Graph(id="graph-daily-time", figure=fig_line),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Spielzeit Nutzer vs. Top-Spiele", id="hdr-heatmap-user-game"),
                    dcc.Graph(id="graph-heatmap-user-game", figure=fig_heat),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Spielzeit nach Tagesstunde", id="hdr-by-hour"),
                    dcc.Graph(id="graph-by-hour", figure=fig_hour),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Gesamtnutzer in Sprachkanälen", id="hdr-voice-users-over-time"),
                    dcc.Graph(id="graph-voice-users-over-time", figure=fig_voice_users),
                ],
                style={"marginBottom": "2em"},
            ),
        ],
        style={"fontFamily": "Arial, sans-serif", "margin": "2em"},
    )

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
