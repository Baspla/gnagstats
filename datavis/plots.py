"""Utility-Funktionen zum einmaligen Berechnen der Dash Graphen.

Früher wurden die Graphen bei jedem Callback neu erzeugt. Jetzt werden sie
einmal beim Start des Webservers berechnet und als statische Figuren
im Layout gesetzt. Falls die Daten später doch dynamisch werden sollen,
kann man erneut einen Callback hinzufügen, der die Build-Funktionen aufruft.
"""

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