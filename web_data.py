"""Daten-Lade- und Cache-Schicht für das Web-Frontend.

Enthält eine Klasse `WebDataLoader`, die die notwendigen DataFrames (Steam / Discord)
lädt, aufbereitet (Zeitstempel, Minuten pro Snapshot, Namensauflösung) und per TTL cached.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional
import logging
import pandas as pd

from db import Database
from json_data import (
    load_json_data,
    get_steam_id_to_name_map,
    get_discord_id_to_name_map,
)
from config import WEB_CACHE_TTL_MINUTES, JSON_DATA_PATH


@dataclass
class _CacheEntry:
    df: pd.DataFrame
    loaded_ts: int


class WebDataLoader:
    """Kapselt das Laden & Caching aller benötigten Roh-Datenframes.

    Alle *load_* Methoden liefern ein DataFrame mit vorbereiteten Standardspalten:
    - timestamp_dt (datetime)
    - minutes_per_snapshot (float)
    - user_name (falls anwendbar)
    """

    def __init__(self, database: Database, ttl_minutes: int | None = None):
        self.db = database
        self.ttl_seconds = max(1, ttl_minutes or WEB_CACHE_TTL_MINUTES) * 60
        self._steam: Optional[_CacheEntry] = None
        self._discord_voice_activity: Optional[_CacheEntry] = None
        self._discord_voice_channels: Optional[_CacheEntry] = None
        self._discord_game_activity: Optional[_CacheEntry] = None

    # ------------------------------------------------------------
    # interner Helfer
    # ------------------------------------------------------------
    def _is_valid(self, entry: Optional[_CacheEntry]) -> bool:
        if entry is None:
            return False
        return (time.time() - entry.loaded_ts) < self.ttl_seconds

    # ------------------------------------------------------------
    # Steam Game Activity
    # ------------------------------------------------------------
    def load_steam_game_activity(self, force: bool = False) -> pd.DataFrame:
        if not force and self._is_valid(self._steam):
            return self._steam.df

        rows = self.db.web_query_get_steam_game_activity()
        df = (
            pd.DataFrame(rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_steam_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            df["user_name"] = df["steam_id"].astype(str).map(id_map).fillna(df["steam_id"].astype(str)) if id_map else df["steam_id"].astype(str)
        self._steam = _CacheEntry(df=df, loaded_ts=int(time.time()))
        logging.debug("(cache) loaded steam_game_activity rows=%s", len(df))
        return df

    # ------------------------------------------------------------
    # Discord Voice Activity (User in Channels)
    # ------------------------------------------------------------
    def load_discord_voice_activity(self, force: bool = False) -> pd.DataFrame:
        if not force and self._is_valid(self._discord_voice_activity):
            return self._discord_voice_activity.df
        rows = self.db.web_query_get_discord_voice_activity()
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_discord_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            df["user_name"] = df["discord_id"].astype(str).map(id_map).fillna(df["discord_id"].astype(str)) if id_map else df["discord_id"].astype(str)
        self._discord_voice_activity = _CacheEntry(df=df, loaded_ts=int(time.time()))
        logging.debug("(cache) loaded discord_voice_activity rows=%s", len(df))
        return df

    # ------------------------------------------------------------
    # Discord Voice Channels (Channel-Level Stats)
    # ------------------------------------------------------------
    def load_discord_voice_channels(self, force: bool = False) -> pd.DataFrame:
        if not force and self._is_valid(self._discord_voice_channels):
            return self._discord_voice_channels.df
        rows = self.db.web_query_get_discord_voice_channels()
        df = (
            pd.DataFrame(rows, columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
        self._discord_voice_channels = _CacheEntry(df=df, loaded_ts=int(time.time()))
        logging.debug("(cache) loaded discord_voice_channels rows=%s", len(df))
        return df

    # ------------------------------------------------------------
    # Discord Game Activity
    # ------------------------------------------------------------
    def load_discord_game_activity(self, force: bool = False) -> pd.DataFrame:
        if not force and self._is_valid(self._discord_game_activity):
            return self._discord_game_activity.df
        rows = self.db.web_query_get_discord_game_activity()
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "game_name", "collection_interval"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_discord_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            df["user_name"] = df["discord_id"].astype(str).map(id_map).fillna(df["discord_id"].astype(str)) if id_map else df["discord_id"].astype(str)
        self._discord_game_activity = _CacheEntry(df=df, loaded_ts=int(time.time()))
        logging.debug("(cache) loaded discord_game_activity rows=%s", len(df))
        return df

