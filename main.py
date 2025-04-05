import asyncio
import logging
import sqlite3
from asyncio import create_task
from calendar import error

import discord

from collector import DataCollector
from config import LOGGING_LEVEL, DB_PATH, JSON_DATA_PATH, DISCORD_API_TOKEN, DISCORD_STATS_ENABLED
from db import Database
from discord_bot import DiscordClient
from json_data import load_json_data, get_data

data = {
    "guild_ids": [],
    "user_steam_ids": [],
    "user_discord_ids": [],
    "user_birthdays": [],
    "user_data": []
}


def setup_logging():
    # Configure logging
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ],
        encoding='utf-8',
        errors='replace',
    )
    logging.raiseExceptions = False
    logging.info("Logging is set up.")


async def main():
    setup_logging()
    database = Database()
    data = get_data()
    discord_client = DiscordClient(intents=discord.Intents.default())
    collector = DataCollector(data, discord_client, database)
    collector_task = create_task(collector.collect_data())

    try:
        if DISCORD_STATS_ENABLED:
            await asyncio.gather(
                discord_client.start(DISCORD_API_TOKEN),
                collector_task
            )
        else:
            logging.info("Discord stats collection is disabled, skipping Discord client start.")
            await collector_task
    except KeyboardInterrupt:
        await discord_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
