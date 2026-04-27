from __future__ import annotations

from functools import lru_cache

from flask import Flask, jsonify

from data_storage.db import Database
from datavis.data_provider import DataProvider


@lru_cache(maxsize=1)
def _get_provider() -> DataProvider:
    return DataProvider(Database())


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/api/recent")
    def recent_games():
        provider = _get_provider()
        return jsonify(provider.recent_games_by_user(days=30))

    return app
