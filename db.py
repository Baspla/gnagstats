import logging
import sqlite3

from config import DB_PATH


class Database:
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