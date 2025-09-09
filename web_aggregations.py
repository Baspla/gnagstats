"""Aggregations- und Transformationsfunktionen für die Web-Visualisierungen.

Enthält reine Pure-Functions ohne Seiteneffekte, die DataFrames entgegennehmen und
entweder ein transformiertes DataFrame oder direkt einen Plotly Figure liefern.
"""
from __future__ import annotations

import time
import math
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx

# ------------------------------------------------------------
# Grund-Aggregationen
# ------------------------------------------------------------

def agg_heatmap_user_top_games(df: pd.DataFrame, top_users: int = 10, top_games: int = 15) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    cubed = df.groupby(["user_name", "game_name"]).size().reset_index(name="snapshots")
    cubed["hours"] = (cubed["snapshots"] * df["minutes_per_snapshot"].median()) / 60.0
    top_users_idx = cubed.groupby("user_name")["hours"].sum().sort_values(ascending=False).head(top_users).index
    top_games_idx = cubed.groupby("game_name")["hours"].sum().sort_values(ascending=False).head(top_games).index
    filtered = cubed[cubed["user_name"].isin(top_users_idx) & cubed["game_name"].isin(top_games_idx)]
    pivot = filtered.pivot_table(index="user_name", columns="game_name", values="hours", fill_value=0.0)
    return pivot


def agg_playtime_per_game(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["game_name", "total_hours_played"])
    grouped = df.groupby("game_name").size().reset_index(name="snapshots")
    grouped["total_minutes_played"] = grouped["snapshots"] * df["minutes_per_snapshot"].median()
    grouped["total_hours_played"] = grouped["total_minutes_played"] / 60.0
    return grouped.sort_values("total_hours_played", ascending=False)


def agg_daily_hours(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "total_hours"])
    ts = df.set_index("timestamp_dt")
    daily = ts.resample("D").size().rename("snapshots").to_frame()
    daily["total_minutes"] = daily["snapshots"] * df["minutes_per_snapshot"].median()
    daily["total_hours"] = daily["total_minutes"] / 60.0
    return daily.reset_index().rename(columns={"timestamp_dt": "date"})


def agg_hours_by_hour_of_day(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["hour_of_day", "total_hours"])
    tmp = df.copy()
    tmp["hour_of_day"] = tmp["timestamp_dt"].dt.hour
    hourly = tmp.groupby("hour_of_day").size().rename("snapshots").to_frame().reset_index()
    hourly["total_minutes"] = hourly["snapshots"] * df["minutes_per_snapshot"].median()
    hourly["total_hours"] = hourly["total_minutes"] / 60.0
    return hourly.sort_values("hour_of_day")


def recent_players_df(df: pd.DataFrame, window_minutes: float = 5.5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["name", "game"])
    now_ts = int(time.time())
    cutoff = now_ts - int(window_minutes * 60)
    if not {"timestamp", "user_name", "game_name"}.issubset(df.columns):
        return pd.DataFrame(columns=["name", "game"])
    recent = df[df["timestamp"] >= cutoff].copy()
    if recent.empty:
        return pd.DataFrame(columns=["name", "game"])
    recent = recent.dropna(subset=["user_name", "game_name"])
    if recent.empty:
        return pd.DataFrame(columns=["name", "game"])
    pairs = (
        recent[["user_name", "game_name"]]
        .drop_duplicates()
        .rename(columns={"user_name": "name", "game_name": "game"})
        .sort_values(["name", "game"])
    )
    return pairs.reset_index(drop=True)


def agg_total_voice_users_over_time(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["timestamp_dt", "total_users"])
    tmp = df.copy()
    tmp["user_count"] = pd.to_numeric(tmp["user_count"], errors="coerce").fillna(0).astype(int)
    aggregated = (
        tmp.groupby("timestamp_dt")["user_count"].sum().rename("total_users").to_frame().reset_index().sort_values("timestamp_dt")
    )
    return aggregated


def agg_daily_peak_voice_users(df: pd.DataFrame) -> pd.DataFrame:
    per_ts = agg_total_voice_users_over_time(df)
    if per_ts.empty:
        return pd.DataFrame(columns=["date", "total_users"])
    tmp = per_ts.copy()
    tmp["date"] = tmp["timestamp_dt"].dt.floor("D")
    daily_peak = tmp.groupby("date")["total_users"].max().reset_index()
    return daily_peak.sort_values("date")

# ------------------------------------------------------------
# Figure Builder (Plotly)
# ------------------------------------------------------------

def build_user_game_network(df: pd.DataFrame, min_game_hours: float = 20.0) -> go.Figure:
    if df.empty:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=0, b=0)))
    tmp = df.copy()
    per_edge = tmp.groupby(["user_name", "game_name"]).size().reset_index(name="snapshots")
    minutes_per_snapshot = float(tmp["minutes_per_snapshot"].median() if not tmp["minutes_per_snapshot"].isna().all() else 5.0)
    per_edge["hours"] = (per_edge["snapshots"] * minutes_per_snapshot) / 60.0
    game_totals = per_edge.groupby("game_name")["hours"].sum().rename("total_hours").reset_index()
    keep_games = set(game_totals[game_totals["total_hours"] >= float(min_game_hours)]["game_name"].tolist())
    per_edge = per_edge[per_edge["game_name"].isin(keep_games)]
    if per_edge.empty:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=0, b=0)))
    game_totals = per_edge.groupby("game_name")["hours"].sum().rename("total_hours").reset_index()
    G = nx.Graph()
    game_hours_map = dict(zip(game_totals["game_name"], game_totals["total_hours"]))
    for g, h in game_hours_map.items():
        G.add_node(("game", g), kind="game", label=g, total_hours=float(h))
    users = per_edge["user_name"].unique().tolist()
    for u in users:
        G.add_node(("user", u), kind="user", label=u)
    for _, row in per_edge.iterrows():
        G.add_edge(("user", row["user_name"]), ("game", row["game_name"]), weight=float(row["hours"]))
    if G.number_of_edges() == 0:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=0, b=0)))
    pos = nx.spring_layout(G, weight="weight", k=None, iterations=200, seed=42)
    weights_all = [float(d.get("weight", 1.0)) for _, _, d in G.edges(data=True)]
    w_max = max(weights_all) if weights_all else 1.0
    edge_traces = []
    for u, v, d in G.edges(data=True):
        w = float(d.get("weight", 1.0))
        width = 1.0 + 7.0 * (w / w_max) if w_max > 0 else 4.0
        x0, y0 = pos[u]; x1, y1 = pos[v]
        u_label = G.nodes[u].get("label", str(u)); v_label = G.nodes[v].get("label", str(v))
        hover_text = f"{u_label} – {v_label}: {w:.1f} h"
        edge_traces.append(go.Scatter(x=[x0, x1], y=[y0, y1], mode="lines", line=dict(width=width, color="#aaaaaa"), hoverinfo="text", text=[hover_text, hover_text], hovertemplate="%{text}<extra></extra>", opacity=0.55, showlegend=False))
    user_x=user_y=user_text=[]; game_x=game_y=game_text=game_sizes=[]
    g_hours_vals = list(game_hours_map.values()); g_min=min(g_hours_vals) if g_hours_vals else 0.0; g_max=max(g_hours_vals) if g_hours_vals else 1.0
    g_size_min, g_size_max = 12, 36
    user_x=[];user_y=[];user_text=[];game_x=[];game_y=[];game_text=[];game_sizes=[]
    for n, attrs in G.nodes(data=True):
        x, y = pos[n]
        if attrs.get("kind") == "user":
            user_x.append(x); user_y.append(y); user_text.append(attrs.get("label", ""))
        else:
            game_x.append(x); game_y.append(y); game_text.append(f"{attrs.get('label','')}<br>{attrs.get('total_hours',0):.1f} h total")
            h = float(attrs.get("total_hours", 0.0))
            if g_max > g_min:
                size = g_size_min + (g_size_max - g_size_min) * ((h - g_min) / (g_max - g_min))
            else:
                size = (g_size_min + g_size_max) / 2
            game_sizes.append(size)
    user_trace = go.Scatter(x=user_x, y=user_y, mode="markers", hoverinfo="text", text=user_text, marker=dict(size=12, color="#1f77b4", line=dict(width=1, color="#ffffff")), name="User")
    game_trace = go.Scatter(x=game_x, y=game_y, mode="markers", hoverinfo="text", text=game_text, marker=dict(size=game_sizes, color="#ff7f0e", line=dict(width=1, color="#333333")), name="Game")
    fig = go.Figure(data=[*edge_traces, user_trace, game_trace], layout=go.Layout(showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), hovermode="closest", margin=dict(l=10, r=10, t=10, b=10), xaxis=dict(visible=False), yaxis=dict(visible=False)))
    return fig


def build_voice_user_network(df: pd.DataFrame, min_shared_hours: float = 0.0, min_node_size: int = 15, max_node_size: int = 60, min_edge_width: float = 0.5, max_edge_width: float = 8.0, layout_k: float = 0.6, layout_iterations: int = 200, layout_seed: int = 42) -> go.Figure:
    from itertools import combinations
    from collections import Counter

    if df.empty or df["user_name"].nunique() < 2:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=30, b=0), title="Discord Voice User Network (keine Daten)"))
    tmp = df.copy()
    if "timestamp_dt" not in tmp.columns:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=30, b=0), title="Discord Voice User Network (Fehlende timestamp_dt)"))
    if "minutes_per_snapshot" not in tmp.columns or tmp["minutes_per_snapshot"].isna().all():
        tmp["minutes_per_snapshot"] = 5.0
    pair_minutes = Counter()
    for (_, _channel), group in tmp.groupby(["timestamp_dt", "channel_name"], sort=False):
        users = sorted(group["user_name"].dropna().unique().tolist())
        if len(users) < 2: continue
        minutes_slot = float(group["minutes_per_snapshot"].median())
        for u1, u2 in combinations(users, 2):
            if u1 == u2: continue
            key = tuple(sorted((u1, u2))); pair_minutes[key] += minutes_slot
    if not pair_minutes:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=30, b=0), title="Discord Voice User Network (keine gemeinsamen Zeiten)"))
    data_rows = []
    for (u1, u2), minutes in pair_minutes.items():
        hours = minutes / 60.0
        if hours >= float(min_shared_hours):
            data_rows.append({"user1": u1, "user2": u2, "shared_minutes": minutes, "shared_hours": hours})
    pair_df = pd.DataFrame(data_rows)
    if pair_df.empty:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=30, b=0), title=f"Discord Voice User Network (Filter: >= {min_shared_hours} h)"))
    user_minutes = tmp.groupby("user_name")["minutes_per_snapshot"].sum().rename("total_minutes")
    user_hours = (user_minutes / 60.0).rename("total_hours")
    user_hours_map = dict(zip(user_hours.index, user_hours.values))
    G = nx.Graph()
    for user, hours in user_hours_map.items():
        G.add_node(user, total_hours=float(hours))
    for _, row in pair_df.iterrows():
        G.add_edge(row["user1"], row["user2"], weight=float(row["shared_hours"]))
    if G.number_of_edges() == 0:
        return go.Figure(layout=dict(xaxis=dict(visible=False), yaxis=dict(visible=False), margin=dict(l=0, r=0, t=30, b=0), title="Discord Voice User Network (keine Kanten)"))
    pos = nx.spring_layout(G, k=layout_k, iterations=layout_iterations, seed=layout_seed, weight="weight")
    hours_vals = [float(G.nodes[n].get("total_hours", 0.0)) for n in G.nodes()]
    h_min = min(hours_vals) if hours_vals else 0.0; h_max = max(hours_vals) if hours_vals else 1.0
    def _scale(h: float):
        if h_max > h_min:
            return min_node_size + (max_node_size - min_node_size) * ((h - h_min) / (h_max - h_min))
        return (min_node_size + max_node_size) / 2.0
    node_sizes = [_scale(float(G.nodes[n].get("total_hours", 0.0))) for n in G.nodes()]
    users = list(G.nodes())
    palette = px.colors.qualitative.Dark24 + px.colors.qualitative.Light24 + px.colors.qualitative.Plotly
    colors = [palette[i % len(palette)] for i in range(len(users))]
    color_map = {u: c for u, c in zip(users, colors)}
    edge_weights = [float(d.get("weight", 0.0)) for _, _, d in G.edges(data=True)]
    ew_min = min(edge_weights) if edge_weights else 0.0; ew_max = max(edge_weights) if edge_weights else 1.0
    def _edge_scale(w: float):
        if ew_max > ew_min:
            return min_edge_width + (max_edge_width - min_edge_width) * ((w - ew_min) / (ew_max - ew_min))
        return (min_edge_width + max_edge_width) / 2.0
    edge_traces = []
    for u, v, d in G.edges(data=True):
        w = float(d.get("weight", 0.0)); width = _edge_scale(w); x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_traces.append(go.Scatter(x=[x0, x1], y=[y0, y1], mode="lines", line=dict(width=width, color="rgba(120,120,120,0.5)"), hoverinfo="text", text=[f"{u} – {v}<br>Gemeinsame Voice-Stunden: {w:.2f}", f"{u} – {v}<br>Gemeinsame Voice-Stunden: {w:.2f}"], hovertemplate="%{text}<extra></extra>", showlegend=False))
    node_x=[pos[u][0] for u in users]; node_y=[pos[u][1] for u in users]
    node_hover=[f"{u}<br>Gesamt Voice-Stunden: {user_hours_map.get(u,0):.2f}" for u in users]
    node_trace = go.Scatter(x=node_x, y=node_y, mode="markers+text", hoverinfo="text", hovertext=node_hover, text=users, textposition="top center", marker=dict(size=node_sizes, color=[color_map[u] for u in users], line=dict(width=1.5, color="#1f1f1f")), showlegend=False)
    fig = go.Figure(data=[*edge_traces, node_trace], layout=go.Layout(title=("Discord Voice User Network""<br><span style='font-size:12px'>Kantenbreite = gemeinsame Voice-Stunden, Node-Größe = gesamte Voice-Stunden</span>"), title_x=0.5, margin=dict(l=10, r=10, t=60, b=10), hovermode="closest", showlegend=False, xaxis=dict(showgrid=False, zeroline=False, showticklabels=False), yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
    return fig


def build_voice_24h_timeline(df: pd.DataFrame) -> go.Figure:
    from datetime import datetime
    if df.empty:
        return go.Figure(layout=dict(title="Voice Aktivität letzte 24h (keine Daten)"))
    if "user_name" not in df.columns:
        return go.Figure(layout=dict(title="Voice Aktivität letzte 24h (fehlende user_name Spalte)"))
    now_ts = int(time.time()); cutoff_ts = now_ts - 24 * 3600
    recent = df[df["timestamp"] >= cutoff_ts].copy()
    if recent.empty:
        return go.Figure(layout=dict(title="Voice Aktivität letzte 24h (keine Daten im Zeitraum)"))
    if "timestamp_dt" not in recent.columns:
        recent["timestamp_dt"] = pd.to_datetime(recent["timestamp"], unit="s")
    default_interval = float(recent["collection_interval"].dropna().median() if not recent["collection_interval"].dropna().empty else 300.0)
    cutoff_dt = datetime.fromtimestamp(cutoff_ts); now_dt = datetime.fromtimestamp(now_ts)
    sessions = []
    for user, g in recent.groupby("user_name"):
        g = g.sort_values("timestamp").reset_index(drop=True)
        current=None; prev_row=None
        for _, row in g.iterrows():
            ts = int(row["timestamp"]); chan = row.get("channel_name", "?") or "?"; interv = row.get("collection_interval")
            try:
                interv = float(interv)
                if not math.isfinite(interv) or interv <= 0: raise ValueError
            except Exception:
                interv = default_interval
            snapshot_end = ts + interv
            if current is None:
                current = {"user_name": user, "channel_name": chan, "start_ts": ts, "end_ts": snapshot_end}
            else:
                gap = ts - prev_row["timestamp"] if prev_row is not None else 0
                prev_interv = prev_row.get("collection_interval") if prev_row is not None else default_interval
                try:
                    prev_interv = float(prev_interv)
                    if not math.isfinite(prev_interv) or prev_interv <= 0: raise ValueError
                except Exception:
                    prev_interv = default_interval
                max_gap = 2 * max(prev_interv, interv)
                if chan == current["channel_name"] and gap <= max_gap:
                    if snapshot_end > current["end_ts"]: current["end_ts"] = snapshot_end
                else:
                    if current["end_ts"] > current["start_ts"]: sessions.append(current)
                    current = {"user_name": user, "channel_name": chan, "start_ts": ts, "end_ts": snapshot_end}
            prev_row = row
        if current is not None and current["end_ts"] > current["start_ts"]: sessions.append(current)
    if not sessions:
        return go.Figure(layout=dict(title="Voice Aktivität letzte 24h (keine Sessions)"))
    sess_df = pd.DataFrame(sessions)
    sess_df["start_ts"] = sess_df["start_ts"].clip(lower=cutoff_ts); sess_df["end_ts"] = sess_df["end_ts"].clip(upper=now_ts)
    sess_df["start_dt"] = pd.to_datetime(sess_df["start_ts"], unit="s"); sess_df["end_dt"] = pd.to_datetime(sess_df["end_ts"], unit="s")
    sess_df["duration_seconds"] = (sess_df["end_ts"] - sess_df["start_ts"]).astype(float)
    sess_df["duration_minutes"] = sess_df["duration_seconds"] / 60.0; sess_df["duration_hours"] = sess_df["duration_minutes"] / 60.0
    sess_df = sess_df[sess_df["duration_seconds"] > 0]
    if sess_df.empty: return go.Figure(layout=dict(title="Voice Aktivität letzte 24h (keine Sessions)", height=400))
    fig = px.timeline(sess_df, x_start="start_dt", x_end="end_dt", y="user_name", color="channel_name", hover_data={"channel_name": True, "duration_minutes":":.1f", "duration_hours":":.2f", "start_dt": True, "end_dt": True}, title="Voice Aktivität letzte 24h")
    users_sorted = sorted(sess_df["user_name"].unique())
    fig.update_yaxes(categoryorder="array", categoryarray=users_sorted, autorange="reversed", fixedrange=True)
    fig.update_xaxes(range=[cutoff_dt, now_dt], tickformat="%H:%M", title_text="Zeit (UTC, letzte 24h)", fixedrange=True)
    fig.update_yaxes(title_text="User")
    fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), margin=dict(l=40, r=20, t=60, b=40), hoverlabel=dict(bgcolor="white"), height=max(400, min(1200, 30 * len(users_sorted))))
    fig.add_shape(type="line", x0=now_dt, x1=now_dt, y0=0, y1=1, xref="x", yref="paper", line=dict(color="red", width=2, dash="dot"))
    fig.add_annotation(x=now_dt, y=1, xref="x", yref="paper", showarrow=False, text="Jetzt", align="right", yanchor="bottom", font=dict(color="red"))
    return fig
