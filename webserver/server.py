import logging
import threading
import time
from typing import Dict

from dash import Dash
from flask import Flask

from config import WEB_FIGURE_REFRESH_MINUTES
from webserver.data_provider import DataProvider
from webserver.layout import create_layout
from webserver.callbacks import build_initial_figures, register_callbacks


class FigureCache:
    """Thread-sicherer (einfacher) Cache für die aktuell vorgerenderten Figuren."""

    def __init__(self):
        self._lock = threading.Lock()
        self._figures: Dict[str, object] = {}

    def set(self, figures: Dict[str, object]):
        with self._lock:
            self._figures = figures

    def get(self) -> Dict[str, object]:
        with self._lock:
            return dict(self._figures)


def _background_refresh(data_provider: DataProvider, cache: FigureCache, interval_minutes: int):
    interval_seconds = max(60, interval_minutes * 60)  # mindestens 1 Minute
    logging.info(f"Starting background figure refresh thread (interval={interval_minutes} min)")
    while True:
        try:
            figures = build_initial_figures(data_provider)
            cache.set(figures)
            logging.debug("Dashboard figures rebuilt and cached.")
        except Exception as e:  # pragma: no cover - robust gegen Fehler
            logging.exception(f"Error while rebuilding figures: {e}")
        time.sleep(interval_seconds)


def create_app(data_provider: DataProvider):
    server = Flask(__name__)
    app = Dash(__name__, server=server, suppress_callback_exceptions=True, title="Gnag Stats Dashboard")

    # Initial einmal erzeugen
    cache = FigureCache()
    cache.set(build_initial_figures(data_provider))

    # Hintergrund-Thread für regelmäßiges Rebuild
    t = threading.Thread(
        target=_background_refresh,
        args=(data_provider, cache, WEB_FIGURE_REFRESH_MINUTES),
        daemon=True,
        name="FigureRefreshThread",
    )
    t.start()

    # Dynamic layout function greift immer auf aktuellen Cache zu
    def dynamic_layout():
        figs = cache.get()
        return create_layout(voice_fig=figs.get("voice"), game_fig=figs.get("game"))

    app.layout = dynamic_layout
    
    # Callbacks registrieren (z.B. für Page Refresh)
    register_callbacks(app)
    
    return app
