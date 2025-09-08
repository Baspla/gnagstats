import logging
import sqlite3
from datetime import datetime

from config import DB_PATH, DATA_COLLECTION_INTERVAL


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

    def get_steam_most_played_games(self, start_time: int, end_time: int):
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

    def get_steam_most_concurrent_players(self, start_time: int, end_time: int):
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

    def get_steam_most_played_together(self, start_time: int, end_time: int):
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

    def get_discord_busiest_voice_channels(self, start_time: int, end_time: int):
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

    def get_discord_total_voice_activity(self, start_time: int, end_time: int):
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

    def get_discord_total_lonely_voice_activity(self, start_time: int, end_time: int):
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

    def get_discord_unique_active_users(self, start_time: int, end_time: int):
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

    #
    # Utilities
    #

def seconds_to_human_readable(total_seconds: int):
    """
    Konvertiert eine Anzahl von Sekunden in ein menschenlesbares Format.
    :param total_seconds:
    :return:
    """
    logging.debug(f"Converting {total_seconds} seconds to human-readable format.")
    if not total_seconds:
        return "0 Minuten"
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

def timesteps_to_human_readable(timesteps: int, collection_interval: int = None):
    """
    Konvertiert eine Anzahl von Timesteps in ein menschenlesbares Format.
    Ein Timestep ist collection_interval Sekunden lang (Standard: DATA_COLLECTION_INTERVAL).
    :param timesteps:
    :param collection_interval: Sekunden pro Timestep
    :return:
    """
    logging.debug(f"Converting {timesteps} timesteps to human-readable format.")
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

