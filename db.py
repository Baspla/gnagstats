import logging
import sqlite3

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
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discord_voice_channels (
                timestamp INTEGER,
                channel_name TEXT,
                guild_id TEXT,
                user_count INTEGER,
                tracked_users INTEGER
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discord_game_activity (
                timestamp INTEGER,
                discord_id TEXT,
                game_name TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS steam_game_activity (
                timestamp INTEGER,
                steam_id TEXT,
                game_name TEXT
            )
        ''')
        connection.commit()
        connection.close()
        logging.info("Database is set up.")

    #
    # Inserts
    #

    def insert_discord_voice_channel(self, channel_name: str, guild_id: str, user_count: int, tracked_users: int):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_voice_channels (timestamp, channel_name, guild_id, user_count, tracked_users)
            VALUES (strftime('%s','now'), ?, ?, ?, ?)
        ''', (channel_name, guild_id, user_count, tracked_users))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord voice channel data: {channel_name}, {guild_id}, {user_count}, {tracked_users}")

    def insert_discord_voice_activity(self, discord_id: str, channel_name: str, guild_id: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_voice_activity (timestamp, discord_id, channel_name, guild_id)
            VALUES (strftime('%s','now'), ?, ?, ?)
        ''', (discord_id, channel_name, guild_id))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord voice activity data: {discord_id}, {channel_name}, {guild_id}")

    def insert_discord_game_activity(self, discord_id: str, game_name: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO discord_game_activity (timestamp, discord_id, game_name)
            VALUES (strftime('%s','now'), ?, ?)
        ''', (discord_id, game_name))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Discord game activity data: {discord_id}, {game_name}")

    def insert_steam_game_activity(self, steam_id: str, game_name: str):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            INSERT INTO steam_game_activity (timestamp, steam_id, game_name)
            VALUES (strftime('%s','now'), ?, ?)
        ''', (steam_id, game_name))
        connection.commit()
        connection.close()
        logging.debug(f"Inserted Steam game activity data: {steam_id}, {game_name}")

    #
    # Queries
    #

    def get_steam_most_played_games(self, start_time: int, end_time: int):
        """
        Get the most played games in a given time range.
        :param start_time:
        :param end_time:
        :return:
        """
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, COUNT(*) as count
            FROM steam_game_activity
            WHERE timestamp BETWEEN ? AND ?
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
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT game_name, COUNT(DISTINCT steam_id) AS player_count
            FROM steam_game_activity
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY timestamp, game_name
            ORDER BY player_count DESC
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def get_steam_most_played_together(self, start_time: int, end_time: int):
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
        SELECT game_name, COUNT(*) AS occurrences
            FROM (
                SELECT timestamp, game_name
            FROM steam_game_activity
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
        Calculated by summing up the user count in each channel with usercounts > 1.
        :param start_time:
        :param end_time:
        :return:
        """
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT channel_name, SUM(user_count) as accumulated_user_count
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1
            GROUP BY channel_name
            ORDER BY accumulated_user_count DESC
        ''', (start_time, end_time))
        result = cursor.fetchall()
        connection.close()
        return result

    def get_discord_total_voice_activity(self, start_time: int, end_time: int):
        """
        Get the total Discord voice activity in a given time range.
        :param start_time:
        :param end_time:
        :return:
        """
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT COUNT(*) as voice_activity_count
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count > 1
        ''', (start_time, end_time))
        result = cursor.fetchone()
        connection.close()
        return result

    def get_discord_total_lonely_voice_activity(self, start_time: int, end_time: int):
        """
        Get the total Discord voice activity in a given time range.
        :param start_time:
        :param end_time:
        :return:
        """
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT SUM(user_count) as accumulated_user_count
            FROM discord_voice_channels
            WHERE timestamp BETWEEN ? AND ? AND user_count = 1
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
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT COUNT(DISTINCT discord_id) as unique_users
            FROM discord_voice_activity
            WHERE timestamp BETWEEN ? AND ?
        ''', (start_time, end_time))
        result = cursor.fetchone()
        connection.close()
        return result

    #
    # Utilities
    #

    def timesteps_to_human_readable(self, timesteps: int):
        """
        Convert a timestep to a human-readable format.
        One timestep is DATA_COLLECTION_INTERVAL minutes long.
        :param timesteps:
        :return:
        """
        logging.debug(f"Converting {timesteps} timesteps to human-readable format.")
        if not timesteps:
            return "0 Minuten"
        timesteps = timesteps * DATA_COLLECTION_INTERVAL
        days = timesteps // (24 * 60)
        hours = (timesteps % (24 * 60)) // 60
        minutes = timesteps % 60
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

