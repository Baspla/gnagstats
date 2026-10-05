"""Utility-Funktionen zum einmaligen Berechnen der Dash Graphen.

Früher wurden die Graphen bei jedem Callback neu erzeugt. Jetzt werden sie
einmal beim Start des Webservers berechnet und als statische Figuren
im Layout gesetzt. Falls die Daten später doch dynamisch werden sollen,
kann man erneut einen Callback hinzufügen, der die Build-Funktionen aufruft.
"""

from typing import Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pytz

from data_storage.db import minutes_to_human_readable

# Ziel-Zeitzone für Darstellung (automatische Umstellung Sommer/Winterzeit)
LOCAL_TZ = pytz.timezone("Europe/Berlin")
from datavis.data_provider import DataProvider, Params


def _build_voice_activity_figure(df_voice_intervals: pd.DataFrame) -> go.Figure:
    # Referenzzeitraum: immer die letzten 24h (Ende = jetzt in LOCAL_TZ)
    end_dt = pd.Timestamp.now(tz=LOCAL_TZ)
    start_dt = end_dt - pd.Timedelta(hours=24)
    def _empty_figure(title: str) -> go.Figure:
        fig = go.Figure()
        fig.update_layout(
            title=title,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        # Achse immer voller 24h Bereich + kein Zoomen / Panning
        fig.update_xaxes(range=[start_dt, end_dt], title_text='Zeit (Europe/Berlin)', fixedrange=True)
        fig.update_yaxes(visible=False, fixedrange=True)
        return fig
    if df_voice_intervals.empty:
        return _empty_figure("Voice-Aktivität der letzten 24 Stunden (keine Daten)")
    try:
        required_columns = ['user_name', 'start_ts', 'end_ts', 'duration_minutes']
        for col in required_columns:
            if col not in df_voice_intervals.columns:
                return _empty_figure(f"Voice-Aktivität der letzten 24 Stunden (Fehler: Spalte '{col}' fehlt)")
        df_clean = df_voice_intervals.copy().dropna(subset=['user_name', 'start_ts', 'end_ts'])
        if df_clean.empty:
            return _empty_figure("Voice-Aktivität der letzten 24 Stunden (keine gültigen Daten)")
        # Zeitstempel zuerst als UTC interpretieren, dann in Europe/Berlin konvertieren (inkl. DST)
        df_clean['start_dt'] = pd.to_datetime(df_clean['start_ts'], unit='s', utc=True, errors='coerce').dt.tz_convert(LOCAL_TZ)
        df_clean['end_dt'] = pd.to_datetime(df_clean['end_ts'], unit='s', utc=True, errors='coerce').dt.tz_convert(LOCAL_TZ)
        df_clean = df_clean.dropna(subset=['start_dt', 'end_dt'])
        if df_clean.empty:
            return _empty_figure("Voice-Aktivität der letzten 24 Stunden (ungültige Zeitstempel)")
        df_clean['dauer'] = df_clean['duration_minutes'].apply(lambda x: minutes_to_human_readable(x) if pd.notna(x) else "Unbekannt")
        fig = px.timeline(
            df_clean,
            x_start='start_dt', x_end='end_dt', y='user_name', color='channel_name',
            title='Voice-Aktivität der letzten 24 Stunden',
            labels={
                'user_name': 'Benutzer', 'start_dt': 'Startzeit', 'end_dt': 'Endzeit',
                'channel_name': 'Channel', 'dauer': 'Dauer'
            },
            hover_data={'user_name': True, 'channel_name': True, 'start_dt': True, 'end_dt': True, 'dauer': True}
        )
        fig.update_yaxes(title_text='Benutzer', autorange="reversed", fixedrange=True)
        # Immer fester 24h Bereich
        fig.update_xaxes(title_text='Zeit (Europe/Berlin)', range=[start_dt, end_dt], fixedrange=True)
        fig.update_layout(legend_title_text='Channel')
        return fig
    except Exception as e:  # pragma: no cover - defensiver Fallback
        return _empty_figure(f"Voice-Aktivität der letzten 24 Stunden (Fehler: {str(e)})")


def _build_game_activity_figure(df_game_intervals: pd.DataFrame) -> go.Figure:
    end_dt = pd.Timestamp.now(tz=LOCAL_TZ)
    start_dt = end_dt - pd.Timedelta(hours=24)
    def _empty_figure(title: str) -> go.Figure:
        fig = go.Figure()
        fig.update_layout(
            title=title,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        fig.update_xaxes(range=[start_dt, end_dt], title_text='Zeit (Europe/Berlin)', fixedrange=True)
        fig.update_yaxes(visible=False, fixedrange=True)
        return fig
    if df_game_intervals.empty:
        return _empty_figure("Spielaktivität der letzten 24 Stunden (keine Daten)")
    try:
        required_columns = ['user_name', 'start_ts', 'end_ts', 'game_name', 'duration_minutes', 'source']
        for col in required_columns:
            if col not in df_game_intervals.columns:
                return _empty_figure(f"Spielaktivität der letzten 24 Stunden (Fehler: Spalte '{col}' fehlt)")
        df_clean = df_game_intervals.copy().dropna(subset=['user_name', 'start_ts', 'end_ts', 'game_name', 'source'])
        if df_clean.empty:
            return _empty_figure("Spielaktivität der letzten 24 Stunden (keine gültigen Daten)")
        # Zeitstempel als UTC -> Europe/Berlin (mit DST)
        df_clean['start_dt'] = pd.to_datetime(df_clean['start_ts'], unit='s', utc=True, errors='coerce').dt.tz_convert(LOCAL_TZ)
        df_clean['end_dt'] = pd.to_datetime(df_clean['end_ts'], unit='s', utc=True, errors='coerce').dt.tz_convert(LOCAL_TZ)
        df_clean = df_clean.dropna(subset=['start_dt', 'end_dt'])
        if df_clean.empty:
            return _empty_figure("Spielaktivität der letzten 24 Stunden (ungültige Zeitstempel)")
        df_clean['dauer'] = df_clean['duration_minutes'].apply(lambda x: minutes_to_human_readable(x) if pd.notna(x) else "Unbekannt")
        fig = px.timeline(
            df_clean,
            x_start='start_dt', x_end='end_dt', y='user_name', color='game_name',
            title='Spielaktivität der letzten 24 Stunden',
            labels={
                'user_name': 'Benutzer', 'start_dt': 'Startzeit', 'end_dt': 'Endzeit',
                'game_name': 'Spiel', 'dauer': 'Dauer', 'source': 'Quelle'
            },
            hover_data={'user_name': True, 'game_name': True, 'start_dt': True, 'end_dt': True, 'dauer': True, 'source': True}
        )
        fig.update_yaxes(title_text='Benutzer', autorange="reversed", fixedrange=True)
        fig.update_xaxes(title_text='Zeit (Europe/Berlin)', range=[start_dt, end_dt], fixedrange=True)
        fig.update_layout(legend_title_text='Spiel')
        return fig
    except Exception as e:  # pragma: no cover
        return _empty_figure(f"Spielaktivität der letzten 24 Stunden (Fehler: {str(e)})")


def build_calendar_heatmap(
    daily_minutes: Dict[str, float],
    year: int,
    title: str,
    colorscale: str = "Greens",
) -> go.Figure:
    """Build a calendar heatmap (year–to–today) from a dict of daily minutes.

    X‑axis = weeks (with month labels), Y‑axis = day of week (Mo‑So).

    Parameters
    ----------
    daily_minutes : dict mapping ``"YYYY-MM-DD"`` → minutes.
    year : calendar year (e.g. 2026).
    title : figure title.
    colorscale : any Plotly colorscale, e.g. ``"Greens"`` or ``"Blues"``.

    Returns
    -------
    go.Figure
    """
    from datetime import date, timedelta
    from typing import Dict, List, Optional

    today = date.today()
    start = date(year, 1, 1)
    
    # Cap the calendar at today's date to remove empty future weeks
    if year == today.year:
        actual_end = today
    elif year > today.year:
        actual_end = start
    else:
        actual_end = date(year, 12, 31)

    # Find the first Monday to align the grid
    first_monday = start - timedelta(days=start.weekday())
    last_sunday = actual_end + timedelta(days=(6 - actual_end.weekday()))

    # Calculate total weeks up to the actual end date
    total_weeks = (last_sunday - first_monday).days // 7
    if (last_sunday - first_monday).days % 7 != 0 or total_weeks == 0:
        total_weeks += 1

    # Grid: z[weekday][week_index] — 7 rows x total_weeks columns
    z: List[List[Optional[float]]] = [[None] * total_weeks for _ in range(7)]
    z_text: List[List[str]] = [[""] * total_weeks for _ in range(7)]

    current = first_monday
    for w in range(total_weeks):
        for d in range(7):
            date_str = current.strftime("%Y-%m-%d")
            if start <= current <= actual_end:
                val = daily_minutes.get(date_str, 0)
                if val > 0:
                    z[d][w] = val
                    z_text[d][w] = f"Date: {date_str}<br>Value: {val:.1f} min"
                else:
                    # None macht die Zelle komplett transparent
                    z[d][w] = None  
                    z_text[d][w] = f"Date: {date_str}<br>No activity"
            else:
                z[d][w] = None 
            current += timedelta(days=1)

    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    
    # Use abbreviated month names to prevent overlapping
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    # Calculate X-axis ticks for the first week of each month
    tickvals = []
    ticktext = []
    current_date = first_monday
    prev_month = 0
    for w in range(total_weeks):
        thu = current_date + timedelta(days=3)
        m = thu.month
        if m != prev_month:
            tickvals.append(w)
            ticktext.append(month_names[m-1])
            prev_month = m
        current_date += timedelta(days=7)

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            y=day_labels,
            colorscale=colorscale,
            xgap=2, ygap=2,  
            text=z_text,
            hoverongaps=True, # Wichtig: Erlaubt Tooltips auf transparenten (None) Zellen
            hovertemplate="%{text}<extra></extra>",
            showscale=True,
            colorbar=dict(title="Minutes"),
        )
    )

    fig.update_layout(
        title=title,
        yaxis=dict(
            autorange="reversed", 
            showticklabels=True,
            showgrid=False,
            zeroline=False
        ),
        xaxis=dict(
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            showgrid=False,
            zeroline=False,
            side="bottom",
            tickangle=0
        ),
        # Beide Hintergründe auf komplett transparent setzen
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=50, r=50, t=60, b=40),
        height=250,
        font=dict(size=10),
    )
    return fig


def build_figures(data_provider: DataProvider):
    """Returns
    -------
    dict: {'voice': go.Figure, 'game': go.Figure}
    """
    now_ts = int(pd.Timestamp.now(tz=LOCAL_TZ).timestamp())
    params = Params(start=now_ts - 24 * 60 * 60, end=now_ts)
    bundle = data_provider.load_all(params)
    voice_fig = _build_voice_activity_figure(bundle.get("voice_intervals", pd.DataFrame()))
    game_fig = _build_game_activity_figure(bundle.get("game_intervals", pd.DataFrame()))
    return {"voice": voice_fig, "game": game_fig}