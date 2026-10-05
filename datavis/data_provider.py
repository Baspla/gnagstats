from __future__ import annotations
from dataclasses import dataclass
from functools import lru_cache
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, cast
import pandas as pd
import uuid
import math

from config import JSON_DATA_PATH
from data_storage.db import Database
from data_storage.json_data import (
    get_user_id_to_name_map,
    get_steam_id_to_user_id_map,
    get_discord_id_to_user_id_map,
    get_user_data,
    load_json_data
)

@dataclass(frozen=True)
class Params:
    start: int | None
    end: int | None

class DataProvider:
    def __init__(self, db: Database):
        self.db = db
        self._registry: Dict[str, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]] = {}
        self.json_data = load_json_data(JSON_DATA_PATH)


    def _query_steam_game_activity(self,start: int | None, end: int | None) -> pd.DataFrame:
        rows = self.db.get_steam_game_activity(start, end)
        df = (
            pd.DataFrame(rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval", 
                                       "timestamp_dt","minutes_per_snapshot","user_id", "user_name"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            id_map = get_user_id_to_name_map(self.json_data) if isinstance(self.json_data, dict) else {}
            steam_id_map = get_steam_id_to_user_id_map(self.json_data) if isinstance(self.json_data, dict) else {}
            df["user_id"] = df["steam_id"].astype(str).map(steam_id_map).fillna(df["steam_id"].astype(str)) if steam_id_map else df["steam_id"].astype(str)
            df["user_name"] = df["user_id"].astype(str).map(id_map).fillna(df["user_id"].astype(str)) if id_map else df["user_id"].astype(str)
        return df

    def _query_discord_game_activity(self,start: int | None, end: int | None) -> pd.DataFrame:
        rows = self.db.get_discord_game_activity(start, end)
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "game_name", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "game_name", "collection_interval",
                                       "timestamp_dt","minutes_per_snapshot","user_id", "user_name"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            id_map = get_user_id_to_name_map(self.json_data) if isinstance(self.json_data, dict) else {}
            discord_id_map = get_discord_id_to_user_id_map(self.json_data) if isinstance(self.json_data, dict) else {}
            df["user_id"] = df["discord_id"].astype(str).map(discord_id_map).fillna(df["discord_id"].astype(str)) if discord_id_map else df["discord_id"].astype(str)
            df["user_name"] = df["user_id"].astype(str).map(id_map).fillna(df["user_id"].astype(str)) if id_map else df["user_id"].astype(str)
        return df

    def _query_discord_voice_activity(self,start: int | None, end: int | None) -> pd.DataFrame:
        rows = self.db.get_discord_voice_activity(start, end)
        df = (
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval",
                                        "self_deaf", "self_mute", "self_stream", "self_video"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval",
                                       "self_deaf", "self_mute", "self_stream", "self_video",
                                       "minutes_per_snapshot","timestamp_dt","user_id","user_name"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
            id_map = get_user_id_to_name_map(self.json_data) if isinstance(self.json_data, dict) else {}
            discord_id_map = get_discord_id_to_user_id_map(self.json_data) if isinstance(self.json_data, dict) else {}
            df["user_id"] = df["discord_id"].astype(str).map(discord_id_map).fillna(df["discord_id"].astype(str)) if discord_id_map else df["discord_id"].astype(str)
            df["user_name"] = df["user_id"].astype(str).map(id_map).fillna(df["user_id"].astype(str)) if id_map else df["user_id"].astype(str)
        return df

    def _query_discord_channels(self,start: int | None, end: int | None) -> pd.DataFrame:
        rows = self.db.web_query_get_discord_voice_channels(start, end)
        df = (
            pd.DataFrame(rows, columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "channel_name", "guild_id", "user_count", "tracked_users", "collection_interval",
                                        "minutes_per_snapshot","timestamp_dt"])
        )
        if not df.empty:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
            df["minutes_per_snapshot"] = (df["collection_interval"].fillna(300) / 60).astype(float)
        return df

    def _compute_game_activity(self, df_steam: pd.DataFrame, df_discord: pd.DataFrame) -> pd.DataFrame:
        # Merge Steam and Discord game activity dataframes
        # Steam data takes precedence over Discord data
        # If user_name and timestamp match but game_name differs, keep only the steam entry

        # Vereinheitliche die Spaltennamen für den Merge
        steam = df_steam.copy()
        discord = df_discord.copy()
        steam['source'] = 'steam'
        discord['source'] = 'discord'

        # Vereinheitliche die relevanten Spalten
        for col in ['user_name', 'minutes_per_snapshot']:
            if col not in steam.columns:
                steam[col] = None
            if col not in discord.columns:
                discord[col] = None
        steam = steam[['timestamp', 'user_name', 'game_name', 'minutes_per_snapshot', 'source']]
        discord = discord[['timestamp', 'user_name', 'game_name', 'minutes_per_snapshot', 'source']]

        # Kombiniere beide DataFrames falls sie nicht leer sind
        if steam.empty:
            logging.debug("Steam dataframe is empty, returning Discord dataframe only.")
            combined = discord
        elif discord.empty:
            logging.debug("Discord dataframe is empty, returning Steam dataframe only.")
            combined = steam
        else:
            combined = pd.concat([steam, discord], ignore_index=True)
        
        # Sortiere nach Quelle, damit Steam-Einträge zuerst kommen
        combined = combined.sort_values(by=['user_name', 'timestamp', 'source'], ascending=[True, True, True])

        # Entferne Discord-Einträge, wenn Steam-Eintrag für user_name und timestamp existiert
        # (Steam hat Vorrang)
        # Erzeuge einen eindeutigen Schlüssel aus user_name und timestamp
        combined['key'] = combined['user_name'].astype(str) + '_' + combined['timestamp'].astype(str)

        # Markiere, ob für einen Schlüssel ein Steam-Eintrag existiert
        steam_keys = set(steam['user_name'].astype(str) + '_' + steam['timestamp'].astype(str))
        # Filter: Behalte alle Steam-Einträge und Discord-Einträge, deren Schlüssel nicht in steam_keys sind
        result = combined[(combined['source'] == 'steam') | (~combined['key'].isin(steam_keys))]

        # Entferne Hilfsspalten
        result = result.drop(columns=['key'])

        # Optional: Sortiere nach Zeit
        result = result.sort_values(by=['user_name', 'timestamp'])
        result = result.reset_index(drop=True)
        return result

    def _compute_voice_activity_intervals(self, df_voice: pd.DataFrame) -> pd.DataFrame:
        if df_voice.empty:
            return pd.DataFrame(columns=["user_name", "channel_name", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        # Standardisiere die Spaltennamen
        df = df_voice.copy()
        if "timestamp" not in df.columns:
            return pd.DataFrame(columns=["user_name", "channel_name", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        if "user_name" not in df.columns:
            # Versuche user_name aus user_id zu holen
            if "user_id" in df.columns:
                df["user_name"] = df["user_id"]
            else:
                return pd.DataFrame(columns=["user_name", "channel_name", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        if "channel_name" not in df.columns:
            df["channel_name"] = "?"
        if "collection_interval" not in df.columns:
            df["collection_interval"] = 300.0
        # Session-Konstruktion ähnlich build_voice_24h_timeline
        sessions = []
        for user, g in df.groupby("user_name"):
            g = g.sort_values("timestamp").reset_index(drop=True)
            current = None
            prev_row = None
            default_interval = float(g["collection_interval"].dropna().median() if not g["collection_interval"].dropna().empty else 300.0)
            for _, row in g.iterrows():
                ts = int(row["timestamp"])
                chan = row.get("channel_name", "?") or "?"
                interv = row.get("collection_interval")
                try:
                    interv = float(interv or default_interval)
                    if not math.isfinite(interv) or interv <= 0:
                        raise ValueError
                except Exception:
                    interv = default_interval
                snapshot_end = ts + interv
                if current is None:
                    current = {"user_name": user, "channel_name": chan, "start_ts": ts, "end_ts": snapshot_end}
                else:
                    gap = ts - prev_row["timestamp"] if prev_row is not None else 0
                    prev_interv = prev_row.get("collection_interval") if prev_row is not None else default_interval
                    try:
                        prev_interv = float(prev_interv or default_interval)
                        if not math.isfinite(prev_interv) or prev_interv <= 0:
                            raise ValueError
                    except Exception:
                        prev_interv = default_interval
                    max_gap = 2 * max(prev_interv, interv)
                    if chan == current["channel_name"] and gap <= max_gap:
                        if snapshot_end > current["end_ts"]:
                            current["end_ts"] = snapshot_end
                    else:
                        if current["end_ts"] > current["start_ts"]:
                            sessions.append(current)
                        current = {"user_name": user, "channel_name": chan, "start_ts": ts, "end_ts": snapshot_end}
                prev_row = row
            if current is not None and current["end_ts"] > current["start_ts"]:
                sessions.append(current)
        if not sessions:
            return pd.DataFrame(columns=["user_name", "channel_name", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        sess_df = pd.DataFrame(sessions)
        # Zeitstempel zu Datetime konvertieren
        sess_df["start_dt"] = pd.to_datetime(sess_df["start_ts"], unit="s")
        sess_df["end_dt"] = pd.to_datetime(sess_df["end_ts"], unit="s")
        sess_df["duration_seconds"] = (sess_df["end_ts"] - sess_df["start_ts"]).astype(float)
        sess_df["duration_minutes"] = sess_df["duration_seconds"] / 60.0
        sess_df["duration_hours"] = sess_df["duration_minutes"] / 60.0
        sess_df = sess_df[sess_df["duration_seconds"] > 0]
        return sess_df.reset_index(drop=True)

    def _compute_game_activity_intervals(self, df_game: pd.DataFrame) -> pd.DataFrame:
        import math
        if df_game.empty:
            return pd.DataFrame(columns=["user_name", "game_name", "source", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        df = df_game.copy()
        if "timestamp" not in df.columns:
            return pd.DataFrame(columns=["user_name", "game_name", "source", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        if "user_name" not in df.columns:
            if "user_id" in df.columns:
                df["user_name"] = df["user_id"]
            else:
                return pd.DataFrame(columns=["user_name", "game_name", "source", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        if "game_name" not in df.columns:
            df["game_name"] = "?"
        if "collection_interval" not in df.columns:
            df["collection_interval"] = 300.0
        if "source" not in df.columns:
            df["source"] = "unknown"
        sessions = []
        for (user, game, source), g in df.groupby(["user_name", "game_name", "source"]):
            g = g.sort_values("timestamp").reset_index(drop=True)
            current = None
            prev_row = None
            default_interval = float(g["collection_interval"].dropna().median() if not g["collection_interval"].dropna().empty else 300.0)
            for _, row in g.iterrows():
                ts = int(row["timestamp"])
                interv = row.get("collection_interval")
                try:
                    interv = float(interv or default_interval)
                    if not math.isfinite(interv) or interv <= 0:
                        raise ValueError
                except Exception:
                    interv = default_interval
                snapshot_end = ts + interv
                if current is None:
                    current = {"user_name": user, "game_name": game, "source": source, "start_ts": ts, "end_ts": snapshot_end}
                else:
                    gap = ts - prev_row["timestamp"] if prev_row is not None else 0
                    prev_interv = prev_row.get("collection_interval") if prev_row is not None else default_interval
                    try:
                        prev_interv = float(prev_interv or default_interval)
                        if not math.isfinite(prev_interv) or prev_interv <= 0:
                            raise ValueError
                    except Exception:
                        prev_interv = default_interval
                    max_gap = 2 * max(prev_interv, interv)
                    if gap <= max_gap:
                        if snapshot_end > current["end_ts"]:
                            current["end_ts"] = snapshot_end
                    else:
                        if current["end_ts"] > current["start_ts"]:
                            sessions.append(current)
                        current = {"user_name": user, "game_name": game, "source": source, "start_ts": ts, "end_ts": snapshot_end}
                prev_row = row
            if current is not None and current["end_ts"] > current["start_ts"]:
                sessions.append(current)
        if not sessions:
            return pd.DataFrame(columns=["user_name", "game_name", "source", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        sess_df = pd.DataFrame(sessions)
        sess_df["start_dt"] = pd.to_datetime(sess_df["start_ts"], unit="s")
        sess_df["end_dt"] = pd.to_datetime(sess_df["end_ts"], unit="s")
        sess_df["duration_seconds"] = (sess_df["end_ts"] - sess_df["start_ts"]).astype(float)
        sess_df["duration_minutes"] = sess_df["duration_seconds"] / 60.0
        sess_df["duration_hours"] = sess_df["duration_minutes"] / 60.0
        sess_df = sess_df[sess_df["duration_seconds"] > 0]
        return sess_df.reset_index(drop=True)
    
    def _aggregate_game_calendar_minutes(
        self,
        game_intervals: pd.DataFrame,
        start_ts: int,
        end_ts: int,
    ) -> Dict[Tuple[str, str], float]:
        """Aggregate game intervals as non-overlapping local calendar-day minutes.

        The existing continuous session builder (``_compute_game_activity_intervals``)
        intentionally merges snapshots across midnight to preserve long sessions for
        timeline visualisations.  For the calendar heatmap, however, each day must
        show only the time actually played during that local (Europe/Berlin) day.
        This method:
          1. Converts each interval's start/end to Europe/Berlin.
          2. Splits any interval that crosses local midnight.
          3. Clips each piece to the caller's time range.
          4. Merges overlapping pieces per (user, date).
          5. Returns total minutes per (user, date).
        """
        if game_intervals.empty:
            return {}

        local_tz = ZoneInfo("Europe/Berlin")
        pieces: Dict[tuple[str, str], list[tuple[float, float]]] = {}

        for row in game_intervals.itertuples(index=False):
            user_name = str(getattr(row, "user_name", ""))
            try:
                interval_start = max(float(row.start_ts), float(start_ts))
                interval_end = min(float(row.end_ts), float(end_ts))
            except (TypeError, ValueError):
                continue
            if not user_name or interval_end <= interval_start:
                continue

            current_ts = interval_start
            while current_ts < interval_end:
                current_dt = datetime.fromtimestamp(current_ts, tz=local_tz)
                next_midnight = datetime.combine(
                    current_dt.date() + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=local_tz,
                )
                piece_end = min(interval_end, next_midnight.timestamp())
                key = (user_name, current_dt.date().isoformat())
                pieces.setdefault(key, []).append((current_ts, piece_end))
                current_ts = piece_end

        totals: Dict[tuple[str, str], float] = {}
        for key, intervals in pieces.items():
            intervals.sort()
            merged: list[list[float]] = []
            for ia, ib in intervals:
                if merged and ia <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], ib)
                else:
                    merged.append([ia, ib])
            totals[key] = sum(b - a for a, b in merged) / 60.0
        return totals
    
    def _aggregate_game_calendar_minutes_by_game(
        self,
        game_intervals: pd.DataFrame,
        start_ts: int,
        end_ts: int,
    ) -> Dict[Tuple[str, str], Dict[str, float]]:
        """Aggregate played minutes per user, local date, and game."""
        if game_intervals.empty:
            return {}

        local_tz = ZoneInfo("Europe/Berlin")
        pieces: Dict[tuple[str, str, str], list[tuple[float, float]]] = {}
        for row in game_intervals.itertuples(index=False):
            user_name = str(getattr(row, "user_name", ""))
            game_name = str(getattr(row, "game_name", ""))
            try:
                interval_start = max(float(row.start_ts), float(start_ts))
                interval_end = min(float(row.end_ts), float(end_ts))
            except (TypeError, ValueError):
                continue
            if not user_name or not game_name or interval_end <= interval_start:
                continue

            current_ts = interval_start
            while current_ts < interval_end:
                current_dt = datetime.fromtimestamp(current_ts, tz=local_tz)
                next_midnight = datetime.combine(
                    current_dt.date() + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=local_tz,
                )
                piece_end = min(interval_end, next_midnight.timestamp())
                key = (user_name, current_dt.date().isoformat(), game_name)
                pieces.setdefault(key, []).append((current_ts, piece_end))
                current_ts = piece_end

        totals: Dict[tuple[str, str], Dict[str, float]] = {}
        for (user_name, date_str, game_name), intervals in pieces.items():
            intervals.sort()
            merged: list[list[float]] = []
            for interval_start, interval_end in intervals:
                if merged and interval_start <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], interval_end)
                else:
                    merged.append([interval_start, interval_end])
            totals.setdefault((user_name, date_str), {})[game_name] = (
                sum(end - start for start, end in merged) / 60.0
            )
        return totals

    def _query_first_timestamp(self) -> int | None:
        return self.db.web_query_get_first_timestamp()

    # --- Öffentliche Aggregations-Schnittstelle für das Web-Frontend ---
    def load_all(self, params: Params) -> Dict[str, pd.DataFrame]:
        """Lädt und bereitet alle für das Dashboard benötigten DataFrames auf.

        Returns
        -------
        dict
            Enthält Rohdaten & Intervall-Daten:
            {
              'voice_raw': df_discord_voice_activity,
              'voice_intervals': df_voice_intervals,
              'game_raw': df_combined_games_raw,
              'game_intervals': df_game_intervals
            }
        """
        start = params.start
        end = params.end
        # Rohdaten laden
        df_voice_raw = self._query_discord_voice_activity(start, end)
        df_discord_game_raw = self._query_discord_game_activity(start, end)
        df_steam_game_raw = self._query_steam_game_activity(start, end)
        # Spiele zusammenführen mit Priorisierung
        df_game_merged = self._compute_game_activity(df_steam_game_raw, df_discord_game_raw)
        # Intervalle berechnen
        df_voice_intervals = self._compute_voice_activity_intervals(df_voice_raw)
        df_game_intervals = self._compute_game_activity_intervals(df_game_merged)
        return {
            'voice_raw': df_voice_raw,
            'voice_intervals': df_voice_intervals,
            'game_raw': df_game_merged,
            'game_intervals': df_game_intervals,
        }

    def recent_games_by_user(self, days: int = 30) -> Dict[str, list[str]]:
        """Return distinct games played per user in the last ``days`` days.

        The result is keyed by user id and sorted by each game's most recent
        activity first.
        """
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        steam_rows = self._query_steam_game_activity(start_ts, end_ts)
        discord_rows = self._query_discord_game_activity(start_ts, end_ts)
        if steam_rows.empty and discord_rows.empty:
            return {}

        combined = pd.concat(
            [
                steam_rows[["timestamp", "user_id", "game_name"]] if not steam_rows.empty else pd.DataFrame(columns=["timestamp", "user_id", "game_name"]),
                discord_rows[["timestamp", "user_id", "game_name"]] if not discord_rows.empty else pd.DataFrame(columns=["timestamp", "user_id", "game_name"]),
            ],
            ignore_index=True,
        )

        if combined.empty:
            return {}

        combined = combined.dropna(subset=["user_id", "game_name", "timestamp"])
        if combined.empty:
            return {}

        combined["user_id"] = combined["user_id"].astype(str)
        combined["game_name"] = combined["game_name"].astype(str)
        combined["timestamp"] = pd.to_numeric(combined["timestamp"], errors="coerce")
        combined = combined.dropna(subset=["timestamp"])
        if combined.empty:
            return {}

        latest = combined.groupby(["user_id", "game_name"], as_index=False).agg(timestamp=("timestamp", "max"))
        latest["_sort_key"] = list(zip(latest["user_id"], -latest["timestamp"], latest["game_name"]))
        latest = latest.sort_values(by="_sort_key")
        latest = latest.drop(columns=["_sort_key"])

        result: Dict[str, list[str]] = {}
        for user_id, group in latest.groupby("user_id", sort=True):
            user_key = str(user_id)
            result[user_key] = group["game_name"].tolist()
        return result

    def get_user_stats_7d(self, game_limit: int = 10) -> Dict[str, dict]:
        """Return per-user statistics.

        The totals (voice_hours, game_hours) cover the last 7 days.
        The per-game list shows the most recent ``game_limit`` games
        without a time restriction.
        The ``daily_game_minutes``/``daily_voice_minutes`` dicts contain
        *all* activity from Jan 1 of the current year up to now, keyed
        by ``YYYY-MM-DD`` – intended for calendar heatmaps.

        Returns a dict keyed by user display name, each containing:
          - voice_hours        (float) – hours in voice channels (7d)
          - game_hours         (float) – total playtime across all games (7d)
          - games               (list) – per-game dicts with keys
              game_name, last_played_ts
          - daily_game_minutes  (dict) – date → minutes played
          - daily_voice_minutes (dict) – date → minutes in voice
        """
        now = datetime.now()
        end_ts = int(now.timestamp())
        start_7d_ts = int((now - timedelta(days=7)).timestamp())

        # ---- 7‑day queries for totals ----
        df_steam = self._query_steam_game_activity(start_7d_ts, end_ts)
        df_discord_game = self._query_discord_game_activity(start_7d_ts, end_ts)
        df_game_merged = self._compute_game_activity(df_steam, df_discord_game)
        df_game_intervals = self._compute_game_activity_intervals(df_game_merged)

        df_voice_raw = self._query_discord_voice_activity(start_7d_ts, end_ts)
        df_voice_intervals = self._compute_voice_activity_intervals(df_voice_raw)

        # ---- collect known user names from JSON config ----
        known_names: set[str] = set()
        if isinstance(self.json_data, dict):
            for u in get_user_data(self.json_data):
                if name := u.get("name"):
                    known_names.add(name)

        # ---- also include any user_names appearing in the data ----
        for df in (df_game_intervals, df_voice_intervals):
            if not df.empty and "user_name" in df.columns:
                known_names.update(str(v) for v in df["user_name"].unique() if pd.notna(v))

        known_names.discard("")
        known_names.discard("?")
        known_names.discard("nan")
        known_names.discard("None")

        # ---- build result skeleton ----
        result: Dict[str, dict] = {}
        for name in sorted(known_names, key=str.casefold):
            result[name] = {"voice_hours": 0.0, "game_hours": 0.0, "games": [],
                            "daily_game_minutes": {}, "daily_top_games": {},
                            "daily_voice_minutes": {}}

        # ---- aggregate 7‑day game hours ----
        if not df_game_intervals.empty:
            game_totals = (
                df_game_intervals.groupby("user_name")["duration_hours"]
                .sum()
                .to_dict()
            )
            for uname, hours in game_totals.items():
                user_name = str(uname)
                if user_name in result:
                    result[user_name]["game_hours"] = round(float(hours), 2)

        # ---- aggregate 7‑day voice hours ----
        if not df_voice_intervals.empty:
            voice_totals = (
                df_voice_intervals.groupby("user_name")["duration_hours"]
                .sum()
                .to_dict()
            )
            for uname, hours in voice_totals.items():
                user_name = str(uname)
                if user_name in result:
                    result[user_name]["voice_hours"] = round(float(hours), 2)

        # ---- daily aggregates YTD (for calendar heatmaps) ----
        year_start_ts = int(datetime(now.year, 1, 1).timestamp())

        df_steam_ytd = self._query_steam_game_activity(year_start_ts, end_ts)
        df_discord_game_ytd = self._query_discord_game_activity(year_start_ts, end_ts)
        df_game_ytd = self._compute_game_activity(df_steam_ytd, df_discord_game_ytd)
        df_game_intervals_ytd = self._compute_game_activity_intervals(df_game_ytd)

        df_voice_ytd = self._query_discord_voice_activity(year_start_ts, end_ts)
        df_voice_intervals_ytd = self._compute_voice_activity_intervals(df_voice_ytd)

        daily_game = self._aggregate_game_calendar_minutes(
            df_game_intervals_ytd, year_start_ts, end_ts
        )
        for key, mins in daily_game.items():
            user_name, date_str = key
            if user_name in result:
                result[user_name]["daily_game_minutes"][date_str] = round(float(mins), 1)

        # ---- top game per local calendar day (for the game heatmap hover) ----
        if not df_game_intervals_ytd.empty:
            daily_game_by_game = self._aggregate_game_calendar_minutes_by_game(
                df_game_intervals_ytd, year_start_ts, end_ts
            )
            for (user_name, date_str), games in daily_game_by_game.items():
                if user_name in result and games:
                    top_game = max(games.items(), key=lambda item: (item[1], item[0]))[0]
                    result[user_name]["daily_top_games"][date_str] = str(top_game)

        if not df_voice_intervals_ytd.empty:
            df_voice_intervals_ytd["date"] = pd.to_datetime(
                df_voice_intervals_ytd["start_ts"], unit="s"
            ).dt.strftime("%Y-%m-%d")
            daily_voice = df_voice_intervals_ytd.groupby(
                ["user_name", "date"]
            )["duration_minutes"].sum()
            for key, mins in daily_voice.items():
                uname, date_str = cast(tuple[object, object], key)
                user_name = str(uname)
                date_key = str(date_str)
                if user_name in result:
                    result[user_name]["daily_voice_minutes"][date_key] = round(float(mins), 1)

        # ---- per‑game list: no time limit, top ``game_limit`` per user ----
        df_steam_all = self._query_steam_game_activity(0, end_ts)
        df_discord_all = self._query_discord_game_activity(0, end_ts)
        df_all_merged = self._compute_game_activity(df_steam_all, df_discord_all)

        if not df_all_merged.empty and "user_name" in df_all_merged.columns:
            last_play = (
                df_all_merged.groupby(["user_name", "game_name"])["timestamp"]
                .max()
                .reset_index()
            )
            last_play = last_play.sort_values("timestamp", ascending=False)
            for uname in result:
                user_games = last_play[last_play["user_name"] == uname].head(game_limit)
                if user_games.empty:
                    continue
                result[uname]["games"] = [
                    {"game_name": str(r["game_name"]), "last_played_ts": int(r["timestamp"])}
                    for _, r in user_games.iterrows()
                ]

        return result