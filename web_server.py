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
    from dash import Dash, html, dcc, Input, Output
    from dash import dash_table
    import plotly.express as px
    import plotly.graph_objects as go
    import networkx as nx

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

    def _recent_players_df(df: pd.DataFrame, window_minutes: float = 5.5) -> pd.DataFrame:
        """Spieler & Spiele, die innerhalb des letzten window_minutes gesehen wurden.

        Liefert eindeutige Paare (user_name, game_name) -> Columns name, game.
        Gibt leeres DF mit passender Struktur zurück, falls keine Daten.
        """
        if df.empty:
            return pd.DataFrame(columns=["name", "game"])
        now_ts = int(time.time())
        cutoff = now_ts - int(window_minutes * 60)
        cols = set(df.columns)
        required = {"timestamp", "user_name", "game_name"}
        if not required.issubset(cols):
            return pd.DataFrame(columns=["name", "game"])
        recent = df[df["timestamp"] >= cutoff].copy()
        if recent.empty:
            return pd.DataFrame(columns=["name", "game"])
        recent = recent.dropna(subset=["user_name", "game_name"])  # Nur gültige
        if recent.empty:
            return pd.DataFrame(columns=["name", "game"])
        recent_pairs = (
            recent[["user_name", "game_name"]]
            .drop_duplicates()
            .rename(columns={"user_name": "name", "game_name": "game"})
            .sort_values(["name", "game"])  # stabile Ausgabe
        )
        return recent_pairs.reset_index(drop=True)

    def _build_user_game_network(df: pd.DataFrame, min_game_hours: float = 20.0):
        """
        Build a bipartite network (Users <-> Games) where:
        - Edge weight = total hours a user played a game
        - Game node size = total hours across all users (scaled)
        - User node size = constant
        - Prune games with total hours < min_game_hours
        Returns Plotly Figure.
        """
        if df.empty:
            # Empty placeholder figure
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=0, b=0)
            ))

        tmp = df.copy()
        # Compute hours per user-game edge
        per_edge = (
            tmp.groupby(["user_name", "game_name"]).size().reset_index(name="snapshots")
        )
        minutes_per_snapshot = float(tmp["minutes_per_snapshot"].median() if not tmp["minutes_per_snapshot"].isna().all() else 5.0)
        per_edge["hours"] = (per_edge["snapshots"] * minutes_per_snapshot) / 60.0

        # Total hours per game and prune
        game_totals = per_edge.groupby("game_name")["hours"].sum().rename("total_hours").reset_index()
        keep_games = set(game_totals[game_totals["total_hours"] >= float(min_game_hours)]["game_name"].tolist())
        per_edge = per_edge[per_edge["game_name"].isin(keep_games)]

        if per_edge.empty:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=0, b=0)
            ))

        # Recompute totals after pruning
        game_totals = per_edge.groupby("game_name")["hours"].sum().rename("total_hours").reset_index()
        user_totals = per_edge.groupby("user_name")["hours"].sum().rename("total_hours").reset_index()

        # Build NetworkX graph
        G = nx.Graph()
        # Add game nodes with attribute kind='game' and size based on total hours
        game_hours_map = dict(zip(game_totals["game_name"], game_totals["total_hours"]))
        for g, h in game_hours_map.items():
            G.add_node(("game", g), kind="game", label=g, total_hours=float(h))
        # Add user nodes with constant size, only those connected to remaining games
        users = per_edge["user_name"].unique().tolist()
        for u in users:
            G.add_node(("user", u), kind="user", label=u)
        # Add edges with weight = hours
        for _, row in per_edge.iterrows():
            G.add_edge(("user", row["user_name"]), ("game", row["game_name"]), weight=float(row["hours"]))

        if G.number_of_edges() == 0:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=0, b=0)
            ))

        # Position using spring layout with weight influence
        pos = nx.spring_layout(G, weight="weight", k=None, iterations=200, seed=42)

        # Build per-edge traces with hover showing hours (weight)
        weights_all = [float(d.get("weight", 1.0)) for _, _, d in G.edges(data=True)]
        w_max = max(weights_all) if weights_all else 1.0
        edge_traces = []
        for u, v, d in G.edges(data=True):
            w = float(d.get("weight", 1.0))
            width = 1.0 + 7.0 * (w / w_max) if w_max > 0 else 4.0
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            u_label = G.nodes[u].get("label", str(u))
            v_label = G.nodes[v].get("label", str(v))
            hover_text = f"{u_label} – {v_label}: {w:.1f} h"
            edge_traces.append(
                go.Scatter(
                    x=[x0, x1],
                    y=[y0, y1],
                    mode="lines",
                    line=dict(width=width, color="#aaaaaa"),
                    hoverinfo="text",
                    text=[hover_text, hover_text],
                    hovertemplate="%{text}<extra></extra>",
                    opacity=0.55,
                    showlegend=False,
                )
            )

        # Node traces for users and games separately to manage sizes/colors
        user_x, user_y, user_text = [], [], []
        game_x, game_y, game_text, game_sizes = [], [], [], []
        # Size mapping for games
        g_hours_vals = list(game_hours_map.values())
        g_min = min(g_hours_vals) if g_hours_vals else 0.0
        g_max = max(g_hours_vals) if g_hours_vals else 1.0
        # Visual size range in px
        g_size_min, g_size_max = 12, 36

        for n, attrs in G.nodes(data=True):
            x, y = pos[n]
            if attrs.get("kind") == "user":
                user_x.append(x)
                user_y.append(y)
                user_text.append(attrs.get("label", ""))
            else:
                game_x.append(x)
                game_y.append(y)
                game_text.append(f"{attrs.get('label','')}<br>{attrs.get('total_hours',0):.1f} h total")
                h = float(attrs.get("total_hours", 0.0))
                if g_max > g_min:
                    size = g_size_min + (g_size_max - g_size_min) * ((h - g_min) / (g_max - g_min))
                else:
                    size = (g_size_min + g_size_max) / 2
                game_sizes.append(size)

        user_trace = go.Scatter(
            x=user_x, y=user_y,
            mode="markers",
            hoverinfo="text",
            text=user_text,
            marker=dict(
                size=12,
                color="#1f77b4",
                line=dict(width=1, color="#ffffff"),
            ),
            name="User",
        )

        game_trace = go.Scatter(
            x=game_x, y=game_y,
            mode="markers",
            hoverinfo="text",
            text=game_text,
            marker=dict(
                size=game_sizes,
                color="#ff7f0e",
                line=dict(width=1, color="#333333"),
            ),
            name="Game",
        )

        fig = go.Figure(
            data=[*edge_traces, user_trace, game_trace],
            layout=go.Layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="closest",
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(visible=False), yaxis=dict(visible=False),
            ),
        )
        return fig

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

    def _agg_daily_peak_voice_users(df: pd.DataFrame) -> pd.DataFrame:
        """Tages-Peak der Gesamtnutzer in Sprachkanälen (Max je Tag)."""
        per_ts = _agg_total_voice_users_over_time(df)
        if per_ts.empty:
            return pd.DataFrame(columns=["date", "total_users"])
        tmp = per_ts.copy()
        tmp["date"] = tmp["timestamp_dt"].dt.floor("D")
        daily_peak = tmp.groupby("date")["total_users"].max().reset_index()
        return daily_peak.sort_values("date")

    def _build_voice_user_network(
        df: pd.DataFrame,
        min_shared_hours: float = 0.0,
        min_node_size: int = 15,
        max_node_size: int = 60,
        min_edge_width: float = 0.5,
        max_edge_width: float = 8.0,
        layout_k: float = 0.6,
        layout_iterations: int = 200,
        layout_seed: int = 42,
    ):
        """Erzeugt ein interaktives Netzwerk der Discord Voice Beziehungen.

        - Kanten-Gewicht = gemeinsame Voice-Stunden (Users gleichzeitig im selben Channel / Snapshot)
        - Node-Größe = gesamte Voice-Stunden des Users
        - Unterschiedliche Farbe je User
        """
        import plotly.graph_objects as go
        import networkx as nx
        from itertools import combinations
        from collections import Counter
        import plotly.express as px

        if df.empty or df["user_name"].nunique() < 2:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=30, b=0),
                title="Discord Voice User Network (keine Daten)"
            ))

        tmp = df.copy()
        if "timestamp_dt" not in tmp.columns:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=30, b=0),
                title="Discord Voice User Network (Fehlende timestamp_dt)"
            ))

        # Sicherstellen minutes_per_snapshot existiert
        if "minutes_per_snapshot" not in tmp.columns or tmp["minutes_per_snapshot"].isna().all():
            tmp["minutes_per_snapshot"] = 5.0

        pair_minutes = Counter()
        # Gruppiere nach Zeitstempel & Channel
        for (_, _channel), group in tmp.groupby(["timestamp_dt", "channel_name"], sort=False):
            users = sorted(group["user_name"].dropna().unique().tolist())
            if len(users) < 2:
                continue
            minutes_slot = float(group["minutes_per_snapshot"].median())
            for u1, u2 in combinations(users, 2):
                if u1 == u2:
                    continue
                key = tuple(sorted((u1, u2)))
                pair_minutes[key] += minutes_slot

        if not pair_minutes:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=30, b=0),
                title="Discord Voice User Network (keine gemeinsamen Zeiten)"
            ))

        # DataFrame aus Paaren
        data_rows = []
        for (u1, u2), minutes in pair_minutes.items():
            hours = minutes / 60.0
            if hours >= float(min_shared_hours):
                data_rows.append({"user1": u1, "user2": u2, "shared_minutes": minutes, "shared_hours": hours})
        pair_df = pd.DataFrame(data_rows)
        if pair_df.empty:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=30, b=0),
                title=f"Discord Voice User Network (Filter: >= {min_shared_hours} h)"
            ))

        # Gesamtstunden pro User
        user_minutes = tmp.groupby("user_name")["minutes_per_snapshot"].sum().rename("total_minutes")
        user_hours = (user_minutes / 60.0).rename("total_hours")
        user_hours_df = user_hours.reset_index()
        user_hours_map = dict(zip(user_hours_df["user_name"], user_hours_df["total_hours"]))

        # Graph aufbauen
        G = nx.Graph()
        for user, hours in user_hours_map.items():
            G.add_node(user, total_hours=float(hours))
        for _, row in pair_df.iterrows():
            G.add_edge(row["user1"], row["user2"], weight=float(row["shared_hours"]))

        if G.number_of_edges() == 0:
            return go.Figure(layout=dict(
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                margin=dict(l=0, r=0, t=30, b=0),
                title="Discord Voice User Network (keine Kanten)"
            ))

        pos = nx.spring_layout(G, k=layout_k, iterations=layout_iterations, seed=layout_seed, weight="weight")

        # Node Größen skalieren
        hours_vals = [float(G.nodes[n].get("total_hours", 0.0)) for n in G.nodes()]
        h_min = min(hours_vals) if hours_vals else 0.0
        h_max = max(hours_vals) if hours_vals else 1.0
        def _scale(h: float):
            if h_max > h_min:
                return min_node_size + (max_node_size - min_node_size) * ((h - h_min) / (h_max - h_min))
            return (min_node_size + max_node_size) / 2.0
        node_sizes = [_scale(float(G.nodes[n].get("total_hours", 0.0))) for n in G.nodes()]

        # Farben pro User
        users = list(G.nodes())
        palette = px.colors.qualitative.Dark24 + px.colors.qualitative.Light24 + px.colors.qualitative.Plotly
        # Falls mehr User als Palette, wiederholen
        colors = [palette[i % len(palette)] for i in range(len(users))]
        color_map = {u: c for u, c in zip(users, colors)}

        # Kantenbreiten skalieren
        edge_weights = [float(d.get("weight", 0.0)) for _, _, d in G.edges(data=True)]
        ew_min = min(edge_weights) if edge_weights else 0.0
        ew_max = max(edge_weights) if edge_weights else 1.0
        def _edge_scale(w: float):
            if ew_max > ew_min:
                return min_edge_width + (max_edge_width - min_edge_width) * ((w - ew_min) / (ew_max - ew_min))
            return (min_edge_width + max_edge_width) / 2.0

        edge_traces = []
        for u, v, d in G.edges(data=True):
            w = float(d.get("weight", 0.0))
            width = _edge_scale(w)
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_traces.append(
                go.Scatter(
                    x=[x0, x1], y=[y0, y1], mode="lines",
                    line=dict(width=width, color="rgba(120,120,120,0.5)"),
                    hoverinfo="text",
                    text=[f"{u} – {v}<br>Gemeinsame Voice-Stunden: {w:.2f}", f"{u} – {v}<br>Gemeinsame Voice-Stunden: {w:.2f}"],
                    hovertemplate="%{text}<extra></extra>",
                    showlegend=False,
                )
            )

        node_x = [pos[u][0] for u in users]
        node_y = [pos[u][1] for u in users]
        node_hover = [f"{u}<br>Gesamt Voice-Stunden: {user_hours_map.get(u,0):.2f}" for u in users]
        node_trace = go.Scatter(
            x=node_x, y=node_y, mode="markers+text",
            hoverinfo="text", hovertext=node_hover, text=users,
            textposition="top center",
            marker=dict(
                size=node_sizes,
                color=[color_map[u] for u in users],
                line=dict(width=1.5, color="#1f1f1f"),
            ),
            showlegend=False,
        )

        fig = go.Figure(
            data=[*edge_traces, node_trace],
            layout=go.Layout(
                title=(
                    "Discord Voice User Network"\
                    "<br><span style='font-size:12px'>Kantenbreite = gemeinsame Voice-Stunden, Node-Größe = gesamte Voice-Stunden</span>"
                ),
                title_x=0.5,
                margin=dict(l=10, r=10, t=60, b=10),
                hovermode="closest",
                showlegend=False,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            ),
        )
        return fig


    # Build figures once at load time (no auto-refresh)
    df_steam_game_activity_initial = _load_df_steam_game_activity(force=True)
    # Preload other DataFrames as well
    df_discord_voice_activity_initial = _load_df_discord_voice_activity(force=True)
    df_discord_voice_channels_initial = _load_df_discord_voice_channels(force=True)
    df_discord_game_activity_initial = _load_df_discord_game_activity(force=True)

    # Ermittlung globaler Min/Max-Datum für DatePicker
    from datetime import date, timedelta
    if not df_steam_game_activity_initial.empty:
        min_date_global = df_steam_game_activity_initial["timestamp_dt"].min().date()
        max_date_global = df_steam_game_activity_initial["timestamp_dt"].max().date()
    elif not df_discord_voice_channels_initial.empty:
        min_date_global = df_discord_voice_channels_initial["timestamp_dt"].min().date()
        max_date_global = df_discord_voice_channels_initial["timestamp_dt"].max().date()
    else:
        max_date_global = date.today()
        min_date_global = max_date_global - timedelta(days=30)

    # Default für Top-N Spiele (wird durch Slider steuerbar)
    default_top_n_games = 40

    # 1) Bar: playtime per game (Initial – wird durch Callback aktualisiert)
    per_game = _agg_playtime_per_game(df_steam_game_activity_initial)
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
    daily = _agg_daily_hours(df_steam_game_activity_initial)
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
    # Heatmap: kein Zoom/Selection
    fig_heat.update_xaxes(fixedrange=True)
    fig_heat.update_yaxes(fixedrange=True)

    # 4) Vertical bar: hours by hour of day
    by_hour = _agg_hours_by_hour_of_day(df_steam_game_activity_initial)
    fig_hour = px.bar(by_hour, x="hour_of_day", y="total_hours")
    # Nur horizontale Auswahl erlauben (Selection entlang X), vertikale Achse fix
    fig_hour.update_layout(dragmode="select", selectdirection="h")
    fig_hour.update_yaxes(fixedrange=True)
    fig_hour.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

    # 4b) Tabelle: Aktive Spieler letzte 5.5 Minuten
    recent_players_initial = _recent_players_df(df_steam_game_activity_initial)
    recent_players_data_initial = recent_players_initial.to_dict("records")

    # 5) Punkte: Tages-Peaks der Gesamtnutzer in Sprachkanälen
    voice_users_daily_peak = _agg_daily_peak_voice_users(df_discord_voice_channels_initial)
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
    fig_network = _build_user_game_network(network_df_initial, min_game_hours=0.0)

    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H1("GNAG Stats", style={"textAlign": "center"}),
                    html.Div(
                        [
                            html.Label("Zeitraum:"),
                            dcc.DatePickerRange(
                                id="date-range",
                                min_date_allowed=min_date_global,
                                max_date_allowed=max_date_global,
                                start_date=min_date_global,
                                end_date=max_date_global,
                                first_day_of_week=1,  # Monday
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
                                        marks={3:"3", 5: "5", 10: "10", 20: "20", 30: "30", 40: "40"},
                                        tooltip={"placement": "bottom", "always_visible": False},
                                    ),
                                ],
                                style={"minWidth": "260px", "flex": "1"},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "gap": "1rem",
                            "alignItems": "center",
                            "flexWrap": "wrap",
                            "marginBottom": "1em",
                            "alignContent": "center",
                            "width": "100%",
                        },
                    ),
                ],
                style={
                    "position": "sticky",
                    "top": "0",
                    "background": "white",
                    "zIndex": 100,
                    "paddingTop": "1em",
                    "paddingLeft": "2em",
                    "paddingRight": "2em",
                    "paddingBottom": "0.5em",
                    "boxShadow": "0 2px 8px rgba(0,0,0,0.04)",
                },
            ),
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
                    dcc.Graph(
                        id="graph-daily-time",
                        figure=fig_line,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": [
                                "zoom2d",
                                "pan2d",
                                "lasso2d",
                                "zoomIn2d",
                                "zoomOut2d",
                                "autoScale2d",
                                "resetScale2d",
                            ],
                        },
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Spielzeit Nutzer vs. Top-Spiele", id="hdr-heatmap-user-game"),
                    dcc.Graph(
                        id="graph-heatmap-user-game",
                        figure=fig_heat,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": [
                                "zoom2d",
                                "pan2d",
                                "select2d",
                                "lasso2d",
                                "zoomIn2d",
                                "zoomOut2d",
                                "autoScale2d",
                                "resetScale2d",
                            ],
                        },
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    # Flex-Container mit zwei Spalten: Links aktive Gamer Tabelle, rechts Titel + Graph
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H2("Spielzeit nach Tagesstunde", id="hdr-by-hour", style={"marginTop": 0}),
                                    dcc.Graph(
                                        id="graph-by-hour",
                                        figure=fig_hour,
                                        config={
                                            "displaylogo": False,
                                            "scrollZoom": False,
                                            "modeBarButtonsToRemove": [
                                                "zoom2d",
                                                "pan2d",
                                                "lasso2d",
                                                "zoomIn2d",
                                                "zoomOut2d",
                                                "autoScale2d",
                                                "resetScale2d",
                                            ],
                                        },
                                    ),
                                ],
                                style={
                                    "flex": "1",
                                    "minWidth": 0,
                                    "display": "flex",
                                    "flexDirection": "column",
                                    "gap": "0.5rem",
                                },
                            ),
                            html.Div(
                                [
                                    html.H2("Aktive Gamer", style={"marginTop": 0}),
                                    dash_table.DataTable(
                                        id="table-recent-players",
                                        columns=[
                                            {"name": "Name", "id": "name"},
                                            {"name": "Spiel", "id": "game"},
                                        ],
                                        data=recent_players_data_initial,
                                        style_table={
                                            "height": "600px",
                                            "overflowY": "auto",
                                        },
                                        style_cell={
                                            "padding": "4px 8px",
                                            "textAlign": "left",
                                            "fontSize": "13px",
                                        },
                                        style_header={
                                            "backgroundColor": "#f5f5f5",
                                            "fontWeight": "bold",
                                        },
                                        page_action="none",
                                        sort_action="native",
                                        filter_action="none",
                                    ),
                                ],
                                style={
                                    "flex": "1",
                                    "minWidth": 0,
                                    "display": "flex",
                                    "flexDirection": "column",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "gap": "1rem",
                            "alignItems": "stretch",
                            "marginBottom": "2em",
                        },
                    ),
                ]
            ),
            html.Div(
                [
                    html.H2("Tages-Peaks der Gesamtnutzer in Sprachkanälen", id="hdr-voice-users-over-time"),
                    dcc.Graph(
                        id="graph-voice-users-over-time",
                        figure=fig_voice_users,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "modeBarButtonsToRemove": [
                                "zoom2d",
                                "pan2d",
                                "lasso2d",
                                "zoomIn2d",
                                "zoomOut2d",
                                "autoScale2d",
                                "resetScale2d",
                            ],
                        },
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Nutzer–Spiel Netzwerk", id="hdr-user-game-network"),
                    dcc.Graph(
                        id="graph-user-game-network",
                        figure=fig_network,
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "displayModeBar": True,
                        },
                        style={"height": "700px"},
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
            html.Div(
                [
                    html.H2("Discord Voice Nutzer Netzwerk", id="hdr-voice-user-network"),
                    dcc.Graph(
                        id="graph-voice-user-network",
                        figure=_build_voice_user_network(df_discord_voice_activity_initial),
                        config={
                            "displaylogo": False,
                            "scrollZoom": False,
                            "displayModeBar": True,
                        },
                        style={"height": "700px"},
                    ),
                ],
                style={"marginBottom": "2em"},
            ),
        ],
        style={"fontFamily": "Arial, sans-serif", "margin": "2em"},
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
            Output("table-recent-players", "data"),
        ],
        [Input("date-range", "start_date"), Input("date-range", "end_date"), Input("top-n-games-slider", "value")],
    )
    def _update_all(start_date, end_date, top_n):  # noqa: D401
        """Update all graphs when date range or top-n changes."""
        import plotly.express as px  # Lokal, falls Dash Hot Reload
        import plotly.graph_objects as go
        # Laden (ggf. Cache)
        df_steam = _load_df_steam_game_activity()
        df_voice_channels = _load_df_discord_voice_channels()
        df_voice_activity = _load_df_discord_voice_activity()

        # Filterfunktion
        def _filter(df: pd.DataFrame):
            if df.empty or start_date is None or end_date is None or "timestamp_dt" not in df.columns:
                return df
            # end_date inklusiv -> +1 Tag exklusiv
            try:
                start = pd.to_datetime(start_date)
                end = pd.to_datetime(end_date) + pd.Timedelta(days=1)
            except Exception:
                return df
            return df[(df["timestamp_dt"] >= start) & (df["timestamp_dt"] < end)]

        df_steam_f = _filter(df_steam)
        df_voice_channels_f = _filter(df_voice_channels)

        # Re-Aggregationen (Top-N Spiele bestimmen)
        per_game_f = _agg_playtime_per_game(df_steam_f)
        if top_n is None:
            top_n = default_top_n_games
        try:
            top_n_int = int(top_n)
        except Exception:
            top_n_int = default_top_n_games
        top_games = per_game_f.head(top_n_int)["game_name"].tolist() if not per_game_f.empty else []
        # Gefiltertes Steam-DF nur mit Top-N Spielen für Diagramme
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

        daily_f = _agg_daily_hours(df_steam_top)
        daily_no_gap_f = daily_f.copy()
        if not daily_no_gap_f.empty and "total_hours" in daily_no_gap_f.columns:
            daily_no_gap_f.loc[daily_no_gap_f["total_hours"] <= 0, "total_hours"] = None
        fig_line_f = px.bar(daily_no_gap_f, x="date", y="total_hours")
        fig_line_f.update_layout(dragmode="select", selectdirection="h")
        fig_line_f.update_yaxes(fixedrange=True)
        fig_line_f.update_layout(xaxis_title="Datum", yaxis_title="Stunden pro Tag")

        heat_f = _agg_heatmap_user_top_games(df_steam_top, top_games=top_n_int)
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

        by_hour_f = _agg_hours_by_hour_of_day(df_steam_top)
        fig_hour_f = px.bar(by_hour_f, x="hour_of_day", y="total_hours")
        fig_hour_f.update_layout(dragmode="select", selectdirection="h")
        fig_hour_f.update_yaxes(fixedrange=True)
        fig_hour_f.update_layout(xaxis_title="Stunde (0-23)", yaxis_title="Stunden")

        voice_users_daily_peak_f = _agg_daily_peak_voice_users(df_voice_channels_f)
        # Balken statt Punkte für Tages-Peaks
        fig_voice_users_f = px.bar(voice_users_daily_peak_f, x="date", y="total_users")
        fig_voice_users_f.update_layout(dragmode="select", selectdirection="h")
        fig_voice_users_f.update_yaxes(fixedrange=True)
        fig_voice_users_f.update_layout(xaxis_title="Datum", yaxis_title="Peak Nutzer/Tag")

        # Netzwerk (verwende Steam Game Activity DataFrame – gleiche Filter)
        network_df = df_steam_top
        fig_network_f = _build_user_game_network(network_df, min_game_hours=0.0)
        # Voice User Netzwerk
        df_voice_activity_f = _filter(df_voice_activity)
        fig_voice_user_network_f = _build_voice_user_network(df_voice_activity_f)

        # Tabelle aktuelle Spieler
        recent_players_df = _recent_players_df(df_steam)
        recent_players_data = recent_players_df.to_dict("records") if not recent_players_df.empty else []

        return [
            fig_bar_f,
            fig_pie_f,
            fig_line_f,
            fig_heat_f,
            fig_hour_f,
            fig_voice_users_f,
            fig_network_f,
            fig_voice_user_network_f,
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
