import logging
import sqlite3
from datetime import datetime

import pandas as pd

from config import DB_PATH, DATA_COLLECTION_INTERVAL, JSON_DATA_PATH
from data_storage.json_data import get_discord_id_to_user_id_map, get_steam_id_to_user_id_map, get_user_id_to_name_map, load_json_data


class Database:

    #
    # Initialization
    #

    def __init__(self):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        # Create tables if they don't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discord_voice_activity (
                timestamp INTEGER,
                discord_id TEXT,
                channel_name TEXT,
                guild_id TEXT
                    ,collection_interval INTEGER DEFAULT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discord_voice_channels (
                timestamp INTEGER,
                channel_name TEXT,
                guild_id TEXT,
                user_count INTEGER,
                tracked_users INTEGER
                    ,collection_interval INTEGER DEFAULT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discord_game_activity (
                timestamp INTEGER,
                discord_id TEXT,
                game_name TEXT
                    ,collection_interval INTEGER DEFAULT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS steam_game_activity (
                timestamp INTEGER,
                steam_id TEXT,
                game_name TEXT
                    ,collection_interval INTEGER DEFAULT NULL
            )
        ''')
        connection.commit()
        connection.close()
        logging.info("Database is set up.")

    #
    # Inserts
    #

    def insert_discord_voice_channel(self, timestamp: float, channel_name: str, guild_id: str, user_count: int, tracked_users: int):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_voice_channels (timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval)
            VALUES (?, ?, ?, ?, ?, ?)''', (timestamp, channel_name, guild_id, user_count, tracked_users, DATA_COLLECTION_INTERVAL))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord voice channel data: {channel_name}, {guild_id}, {user_count}, {tracked_users}, Interval: {DATA_COLLECTION_INTERVAL}")

    def insert_discord_voice_activity(self, timestamp: float, discord_id: str, channel_name: str, guild_id: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_voice_activity (timestamp, discord_id, channel_name, guild_id, collection_interval)
            VALUES (?, ?, ?, ?, ?)''', (timestamp, discord_id, channel_name, guild_id, DATA_COLLECTION_INTERVAL))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord voice activity data: {discord_id}, {channel_name}, {guild_id}, Interval: {DATA_COLLECTION_INTERVAL}")

    def insert_discord_game_activity(self, timestamp: float, discord_id: str, game_name: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_game_activity (timestamp, discord_id, game_name, collection_interval)
            VALUES (?, ?, ?, ?)''', (timestamp, discord_id, game_name, DATA_COLLECTION_INTERVAL))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord game activity data: {discord_id}, {game_name}, Interval: {DATA_COLLECTION_INTERVAL}")

    def insert_steam_game_activity(self, timestamp: float, steam_id: str, game_name: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO steam_game_activity (timestamp, steam_id, game_name, collection_interval)
            VALUES (?, ?, ?, ?)''', (timestamp, steam_id, game_name, DATA_COLLECTION_INTERVAL))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Steam game activity data: {steam_id}, {game_name}, Interval: {DATA_COLLECTION_INTERVAL}")

    #
    # Queries
    #

    def newsletter_query_get_steam_most_played_games(self, start_time: int, end_time: int):
        """
        Get the most played games in a given time range, including total playtime.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, COUNT(*) as count, SUM(collection_interval) as total_playtime
            FROM steam_game_activity
            WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
            GROUP BY game_name
            ORDER BY count DESC
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def newsletter_query_get_steam_most_concurrent_players(self, start_time: int, end_time: int):
        """
        Get the games played by most players concurrently in a given time range.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, COUNT(DISTINCT steam_id) AS player_count
            FROM steam_game_activity
            WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
            GROUP BY timestamp, game_name
            ORDER BY player_count DESC
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def newsletter_query_get_steam_most_played_together(self, start_time: int, end_time: int):
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
        SELECT game_name, COUNT(*) AS occurrences, SUM(total_playtime) AS total_playtime
            FROM (
                SELECT timestamp, game_name, SUM(collection_interval) AS total_playtime
                FROM steam_game_activity
                WHERE collection_interval IS NOT NULL
                GROUP BY timestamp, game_name
                HAVING COUNT(DISTINCT steam_id) >= 2
            ) grouped_games
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY game_name
        ORDER BY occurrences DESC
        LIMIT 1;
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def newsletter_query_get_discord_busiest_voice_channels(self, start_time: int, end_time: int):
        """
        Get the busiest Discord voice channels in a given time range.
        Calculated by summing up user_count * collection_interval for each channel with usercounts > 1.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT channel_name,
                   SUM(user_count * collection_interval) as total_voicetime
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1 AND collection_interval IS NOT NULL
            GROUP BY channel_name
            ORDER BY total_voicetime DESC
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def newsletter_query_get_discord_total_voice_activity(self, start_time: int, end_time: int):
        """
        Get the total Discord voice activity time (in seconds) in a given time range for channels with >1 users.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(user_count * collection_interval) as total_voicetime
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1 AND collection_interval IS NOT NULL
        ''', (start_time, end_time))
        result = cursor.fetchone()
        connection.close()
        return result

    def newsletter_query_get_discord_total_lonely_voice_activity(self, start_time: int, end_time: int):
        """
        Get the total Discord voice activity time (in seconds) in a given time range for channels with only 1 user.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(user_count * collection_interval) as total_lonely_voicetime
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count = 1 AND collection_interval IS NOT NULL
        ''', (start_time, end_time))
        result = cursor.fetchone()
        connection.close()
        return result

    def newsletter_query_get_discord_unique_active_users(self, start_time: int, end_time: int):
        """
        Get the unique active users in a given time range.
        :param start_time:
        :param end_time:
        :return:
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT COUNT(DISTINCT discord_id) as unique_users
            FROM discord_voice_activity
            WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
        ''', (start_time, end_time))
        result = cursor.fetchone()
        connection.close()
        return result
    
    def query_get_game_activity_sessions(self, start_dt: datetime, end_dt: datetime):
        game_activity = self.query_get_game_activity_dataframe(start_dt, end_dt)
        import math
        if game_activity.empty:
            return pd.DataFrame(columns=["user_name", "game_name", "source", "start_ts", "end_ts", "start_dt", "end_dt", "duration_seconds", "duration_minutes", "duration_hours"])
        df = game_activity.copy()
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
    
    def query_get_game_activity_dataframe(self, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        steam_rows, discord_rows = self._load_raw_game_activity(start_dt, end_dt)
        df = self._build_dataframe(steam_rows, discord_rows)
        return df

    def _load_raw_game_activity(self,start_dt: datetime, end_dt: datetime):
            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())
            steam_rows = self.get_steam_game_activity(start_ts, end_ts)
            discord_rows = self.get_discord_game_activity(start_ts, end_ts)
            return steam_rows, discord_rows

    def _build_dataframe(self,steam_rows, discord_rows):
        # steam
        if steam_rows:
            df_steam = pd.DataFrame(steam_rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"])  # noqa: E501
        else:
            df_steam = pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval"])
        if discord_rows:
            df_discord = pd.DataFrame(discord_rows, columns=["timestamp", "discord_id", "game_name", "collection_interval"])  # noqa: E501
        else:
            df_discord = pd.DataFrame(columns=["timestamp", "discord_id", "game_name", "collection_interval"])

        if df_steam.empty and df_discord.empty:
            return pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"])

        json_data = load_json_data(JSON_DATA_PATH)
        id_map = get_user_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
        steam_id_map = get_steam_id_to_user_id_map(json_data) if isinstance(json_data, dict) else {}
        discord_id_map = get_discord_id_to_user_id_map(json_data) if isinstance(json_data, dict) else {}

        if not df_steam.empty:
            df_steam["user_id"] = df_steam["steam_id"].astype(str).map(steam_id_map).fillna(df_steam["steam_id"].astype(str)) if steam_id_map else df_steam["steam_id"].astype(str)
            df_steam["user_name"] = df_steam["user_id"].astype(str).map(id_map).fillna(df_steam["user_id"].astype(str)) if id_map else df_steam["user_id"].astype(str)
            df_steam["source"] = "steam"
        if not df_discord.empty:
            df_discord["user_id"] = df_discord["discord_id"].astype(str).map(discord_id_map).fillna(df_discord["discord_id"].astype(str)) if discord_id_map else df_discord["discord_id"].astype(str)
            df_discord["user_name"] = df_discord["user_id"].astype(str).map(id_map).fillna(df_discord["user_id"].astype(str)) if id_map else df_discord["user_id"].astype(str)
            df_discord["source"] = "discord"

        df_all = pd.concat([
            df_steam[["timestamp", "user_name", "game_name", "collection_interval", "source"]] if not df_steam.empty else pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"]),
            df_discord[["timestamp", "user_name", "game_name", "collection_interval", "source"]] if not df_discord.empty else pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"]),
        ], ignore_index=True)

        if df_all.empty:
            return df_all

        # Priorität: Steam überschreibt Discord bei gleicher (user_name, timestamp)
        df_all = df_all.sort_values(by=["user_name", "timestamp", "source"], ascending=[True, True, True])
        df_all["key"] = df_all["user_name"].astype(str) + "_" + df_all["timestamp"].astype(str)
        steam_keys = set(df_all[df_all["source"] == "steam"]["key"])  # Keys mit Steam-Eintrag
        df_all = df_all[(df_all["source"] == "steam") | (~df_all["key"].isin(steam_keys))]
        df_all = df_all.drop(columns=["key"])  # Cleanup
        return df_all.reset_index(drop=True)

    def newsletter_query_get_voice_total(self, start_time: datetime, end_time: datetime) -> int:
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(user_count * collection_interval) as total_voicetime
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1 AND collection_interval IS NOT NULL
        ''', (int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchone()
        connection.close()
        return result[0] if result and result[0] is not None else 0

    def newsletter_query_get_voice_alone(self, start_time: datetime, end_time: datetime) -> int:
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(user_count * collection_interval) as total_lonely_voicetime
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count = 1 AND collection_interval IS NOT NULL
        ''', (int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchone()
        connection.close()
        return result[0] if result and result[0] is not None else 0
    
    def newsletter_query_get_voice_together(self, start_time: datetime, end_time: datetime) -> int:
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(collection_interval) as total_voicetime
            FROM (
            SELECT DISTINCT timestamp, collection_interval
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1 AND collection_interval IS NOT NULL
            )
        ''', (int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchone()
        connection.close()
        return result[0] if result and result[0] is not None else 0
    
    def newsletter_query_get_gaming_total(self, start_time: datetime, end_time: datetime) -> int:
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(collection_interval) as total_gaming_time
            FROM discord_game_activity
            WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
        ''', (int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchone()
        discord_time = result[0] if result and result[0] is not None else 0
        cursor.execute('''
            SELECT SUM(collection_interval) as total_gaming_time
            FROM steam_game_activity
            WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
        ''', (int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchone()
        steam_time = result[0] if result and result[0] is not None else 0
        connection.close()
        combined_time = discord_time + steam_time
        return combined_time

    def newsletter_query_get_playtime(self, start_time: datetime, end_time: datetime):
        # Return a list of all games from both steam_game_activity and discord_game_activity with total playtime in the given range.
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, SUM(collection_interval) as total_playtime
            FROM (
                SELECT game_name, collection_interval
                FROM steam_game_activity
                WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
                UNION ALL
                SELECT game_name, collection_interval
                FROM discord_game_activity
                WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
            )
            GROUP BY game_name
            ORDER BY total_playtime DESC
        ''', (int(start_time.timestamp()), int(end_time.timestamp()), int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchall()
        connection.close()
        return result

    def newsletter_query_get_biggest_groups(self, start_time: datetime, end_time: datetime):
        # Return a list of all games by largest concurrent players in the given range from both steam_game_activity and discord_game_activity.
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, COUNT(DISTINCT user_id) AS player_count
            FROM (
                SELECT timestamp, game_name, steam_id AS user_id
                FROM steam_game_activity
                WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
                UNION ALL
                SELECT timestamp, game_name, discord_id AS user_id
                FROM discord_game_activity
                WHERE timestamp BETWEEN ? AND ? AND collection_interval IS NOT NULL
            )
            GROUP BY game_name, timestamp
            ORDER BY player_count DESC
        ''', (int(start_time.timestamp()), int(end_time.timestamp()), int(start_time.timestamp()), int(end_time.timestamp())))
        result = cursor.fetchall()
        connection.close()
        return result
        
    def newsletter_query_get_longest_sessions(self, start_time: datetime, end_time: datetime):
        # Return a set of all games by longest single session in the given range from both steam_game_activity and discord_game_activity.
        game_activity = self.query_get_game_activity_sessions(start_time, end_time)
        grouped = game_activity.groupby(["game_name","user_name","source"])["duration_seconds"].max().reset_index()
        sorted_grouped = grouped.sort_values(by="duration_seconds", ascending=False)
        return sorted_grouped

    #
    # Web queries (separate from newsletter queries)
    #

    def get_steam_game_activity(self, start_time: int | None = None, end_time: int | None = None):
        """
        Return raw steam game activity rows for the web app.
        Each row contains: timestamp, steam_id, game_name, collection_interval.
        Optional start/end timestamps (epoch seconds) can limit the range.
        """
        # Normalize optional datetime inputs
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())

        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()

        if start_time is not None and end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, steam_id, game_name, collection_interval
                FROM steam_game_activity
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
                """,
                (start_time, end_time),
            )
        elif start_time is not None:
            cursor.execute(
                """
                SELECT timestamp, steam_id, game_name, collection_interval
                FROM steam_game_activity
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (start_time,),
            )
        elif end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, steam_id, game_name, collection_interval
                FROM steam_game_activity
                WHERE timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (end_time,),
            )
        else:
            cursor.execute(
                """
                SELECT timestamp, steam_id, game_name, collection_interval
                FROM steam_game_activity
                ORDER BY timestamp ASC
                """
            )

        result = cursor.fetchall()
        connection.close()
        return result

    def get_discord_voice_activity(self, start_time: int | None = None, end_time: int | None = None):
        """
        Return raw discord voice activity rows for the web app.
        Each row contains: timestamp, discord_id, channel_name, guild_id, collection_interval.
        Optional start/end timestamps (epoch seconds) can limit the range.
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())

        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()

        if start_time is not None and end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, channel_name, guild_id, collection_interval
                FROM discord_voice_activity
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
                """,
                (start_time, end_time),
            )
        elif start_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, channel_name, guild_id, collection_interval
                FROM discord_voice_activity
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (start_time,),
            )
        elif end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, channel_name, guild_id, collection_interval
                FROM discord_voice_activity
                WHERE timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (end_time,),
            )
        else:
            cursor.execute(
                """
                SELECT timestamp, discord_id, channel_name, guild_id, collection_interval
                FROM discord_voice_activity
                ORDER BY timestamp ASC
                """
            )

        result = cursor.fetchall()
        connection.close()
        return result

    def web_query_get_discord_voice_channels(self, start_time: int | None = None, end_time: int | None = None):
        """
        Return raw discord voice channel snapshot rows for the web app.
        Each row contains: timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval.
        Optional start/end timestamps (epoch seconds) can limit the range.
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())

        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()

        if start_time is not None and end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval
                FROM discord_voice_channels
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
                """,
                (start_time, end_time),
            )
        elif start_time is not None:
            cursor.execute(
                """
                SELECT timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval
                FROM discord_voice_channels
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (start_time,),
            )
        elif end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval
                FROM discord_voice_channels
                WHERE timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (end_time,),
            )
        else:
            cursor.execute(
                """
                SELECT timestamp, channel_name, guild_id, user_count, tracked_users, collection_interval
                FROM discord_voice_channels
                ORDER BY timestamp ASC
                """
            )

        result = cursor.fetchall()
        connection.close()
        return result

    def get_discord_game_activity(self, start_time: int | None = None, end_time: int | None = None):
        """
        Return raw discord game activity rows for the web app.
        Each row contains: timestamp, discord_id, game_name, collection_interval.
        Optional start/end timestamps (epoch seconds) can limit the range.
        """
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp())

        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()

        if start_time is not None and end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, game_name, collection_interval
                FROM discord_game_activity
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
                """,
                (start_time, end_time),
            )
        elif start_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, game_name, collection_interval
                FROM discord_game_activity
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (start_time,),
            )
        elif end_time is not None:
            cursor.execute(
                """
                SELECT timestamp, discord_id, game_name, collection_interval
                FROM discord_game_activity
                WHERE timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (end_time,),
            )
        else:
            cursor.execute(
                """
                SELECT timestamp, discord_id, game_name, collection_interval
                FROM discord_game_activity
                ORDER BY timestamp ASC
                """
            )

        result = cursor.fetchall()
        connection.close()
        return result
    
    def web_query_get_first_timestamp(self) -> int | None:
        """
        Return the earliest timestamp present in any of the activity tables.
        :return: Earliest timestamp in epoch seconds, or None if no data exists.
        """
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT MIN(min_timestamp) FROM (
                SELECT MIN(timestamp) AS min_timestamp FROM discord_voice_activity
                UNION ALL
                SELECT MIN(timestamp) AS min_timestamp FROM discord_voice_channels
                UNION ALL
                SELECT MIN(timestamp) AS min_timestamp FROM discord_game_activity
                UNION ALL
                SELECT MIN(timestamp) AS min_timestamp FROM steam_game_activity
            )
        ''')
        result = cursor.fetchone()
        connection.close()
        return result[0] if result and result[0] is not None else None

def seconds_to_human_readable(total_seconds: int):
    """
    Konvertiert eine Anzahl von Sekunden in ein menschenlesbares Format.
    :param total_seconds:
    :return:
    """
    if not total_seconds:
        return "0 Minuten"
    days = total_seconds // (24 * 3600)
    hours = (total_seconds % (24 * 3600)) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    output = ""

    if days == 1:
        output += f"{days:<.0f} Tag "
    elif days > 1:
        output += f"{days:.0f} Tage "

    if hours == 1:
        output += f"{hours:.0f} Stunde "
    elif hours > 1:
        output += f"{hours:.0f} Stunden "

    if minutes == 1:
        output += f"{minutes:.0f} Minute "
    elif minutes > 1:
        output += f"{minutes:.0f} Minuten "

    if seconds == 1:
        output += f"{seconds:.0f} Sekunde"
    elif seconds > 1:
        output += f"{seconds:.0f} Sekunden"

    return output.strip()

def timesteps_to_human_readable(timesteps: int, collection_interval = None):
    """
    Konvertiert eine Anzahl von Timesteps in ein menschenlesbares Format.
    Ein Timestep ist collection_interval Sekunden lang (Standard: DATA_COLLECTION_INTERVAL).
    :param timesteps:
    :param collection_interval: Sekunden pro Timestep
    :return:
    """
    if not timesteps:
        return "0 Minuten"
    if collection_interval is None:
        collection_interval = DATA_COLLECTION_INTERVAL
    total_seconds = timesteps * collection_interval
    days = total_seconds // (24 * 3600)
    hours = (total_seconds % (24 * 3600)) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    output = ""

    if days == 1:
        output += f"{days} Tag "
    elif days > 1:
        output += f"{days} Tage "

    if hours == 1:
        output += f"{hours} Stunde "
    elif hours > 1:
        output += f"{hours} Stunden "

    if minutes == 1:
        output += f"{minutes} Minute "
    elif minutes > 1:
        output += f"{minutes} Minuten "

    if seconds == 1:
        output += f"{seconds} Sekunde"
    elif seconds > 1:
        output += f"{seconds} Sekunden"

    return output.strip()

def minutes_to_human_readable(total_minutes: int):
    """
    Konvertiert eine Anzahl von Minuten in ein menschenlesbares Format.
    :param total_minutes:
    :return:
    """
    if not total_minutes:
        return "0 Minuten"
    total_minutes = int(total_minutes)
    days = int(total_minutes // (24 * 60))
    hours = int((total_minutes % (24 * 60)) // 60)
    minutes = int(total_minutes % 60)
    output = ""

    if days == 1:
        output += f"{days} Tag "
    elif days > 1:
        output += f"{days} Tage "

    if hours == 1:
        output += f"{hours} Stunde "
    elif hours > 1:
        output += f"{hours} Stunden "

    if minutes == 1:
        output += f"{minutes} Minute"
    elif minutes > 1:
        output += f"{minutes} Minuten"

    return output.strip()
