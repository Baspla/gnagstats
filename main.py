import asyncio
import logging
import time
from asyncio import create_task
from datetime import datetime as dt
import datetime

import discord
from discord.webhook.async_ import async_context
from steam_web_api import Steam

from collector import DataCollector
from config import LOGGING_LEVEL, DISCORD_API_TOKEN, DISCORD_STATS_ENABLED, STEAM_API_KEY, DATA_COLLECTION_INTERVAL, \
    DEBUG_MODE
from current_events import CurrentEventFetcher
from db import Database
from discord_bot import DiscordClient
from json_data import  get_data
from newsletter_creator import NewsletterCreator

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


async def core_loop(collector,newsletter_creator):
    logging.info("Starting core loop...")
    if DEBUG_MODE:
        logging.info("Debug mode is enabled. Waiting for 10 seconds before starting the newsletter creation.")
        await asyncio.sleep(10)
        day_last_week = dt.now() - datetime.timedelta(days=7)
        newsletter_creator.create_weekly_newsletter(day_last_week.isocalendar())
        newsletter_creator.create_monthly_newsletter(dt.now().year, dt.now().month)
    await asyncio.sleep(DATA_COLLECTION_INTERVAL * 60)
    last_weekly_newsletter_day = None
    last_monthly_newsletter_day = None
    while True:
        start_time = time.time()  # Record the start time
        now = time.localtime()
        day_of_year = now.tm_yday
        # Monday at 9:00 AM
        if now.tm_wday == 0 and now.tm_hour == 9 and 0 <= now.tm_min <= DATA_COLLECTION_INTERVAL * 2 and last_weekly_newsletter_day != day_of_year:
            logging.info("It's time to publish the newsletter!")
            day_last_week = dt.now() - datetime.timedelta(days=7)
            newsletter_creator.create_weekly_newsletter(day_last_week.isocalendar())
            last_weekly_newsletter_day = day_of_year
        # First day of the month at 12:00 PM
        if now.tm_mday == 1 and now.tm_hour == 12 and 0 <= now.tm_min <= DATA_COLLECTION_INTERVAL * 2 and last_monthly_newsletter_day != day_of_year:
            logging.info("It's time to publish the monthly newsletter!")
            newsletter_creator.create_monthly_newsletter(dt.now().year, dt.now().month)
            last_monthly_newsletter_day = day_of_year
        try:
            await collector.collect_discord_data()
        except discord.Forbidden:
            logging.warning("Discord client is not authorized to access the guilds.")
        except discord.HTTPException as e:
            logging.warning(f"Discord HTTP exception: {e}")
        except discord.ClientException as e:
            logging.warning(f"Discord client exception: {e}")
        try:
            await collector.collect_steam_data()
        except Exception as e:
            logging.warning(f"Steam data collection exception: {e}")
        # Calculate the next scheduled time
        elapsed_time :float = time.time() - start_time
        sleep_time = DATA_COLLECTION_INTERVAL * 60 - elapsed_time
        if sleep_time > 0:
            logging.debug(f"Sleeping for {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)
    pass

async def main():
    setup_logging()
    database = Database()
    data = get_data()
    discord_client = DiscordClient(intents=discord.Intents.default())
    steam = Steam(STEAM_API_KEY)
    collector = DataCollector(data, discord_client, database,steam)
    current_event_fetcher = CurrentEventFetcher(discord_client, data,steam)
    newsletter_creator = NewsletterCreator(current_event_fetcher,database)
    coreloop = create_task(core_loop(collector,newsletter_creator))
    try:
        if DISCORD_STATS_ENABLED:
            await asyncio.gather(
                discord_client.start(DISCORD_API_TOKEN),
                coreloop
            )
        else:
            logging.info("Discord stats collection is disabled, skipping Discord client start.")
            await coreloop
    except KeyboardInterrupt:
        await discord_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
