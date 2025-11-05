from __future__ import annotations
from dataclasses import dataclass
from functools import lru_cache
import logging
from typing import Dict, Tuple
import pandas as pd
import uuid
import math

from config import JSON_DATA_PATH
from data_storage.db import Database
from data_storage.json_data import (
    get_user_id_to_name_map,
    get_steam_id_to_user_id_map,
    get_discord_id_to_user_id_map,
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
            pd.DataFrame(rows, columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval"]) if rows
            else pd.DataFrame(columns=["timestamp", "discord_id", "channel_name", "guild_id", "collection_interval", 
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