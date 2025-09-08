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



def should_publish_newsletter(newsletter_type, now, interval_minutes, last_newsletter_day):
    day_of_year = now.tm_yday
    if newsletter_type == "weekly":
        # Montag um 9:00 Uhr
        return (
            now.tm_wday == 0 and
            now.tm_hour == 9 and
            0 <= now.tm_min <= interval_minutes * 2 and
            last_newsletter_day != day_of_year
        )
    elif newsletter_type == "monthly":
        # Erster Tag des Monats um 12:00 Uhr
        return (
            now.tm_mday == 1 and
            now.tm_hour == 12 and
            0 <= now.tm_min <= interval_minutes * 2 and
            last_newsletter_day != day_of_year
        )
    return False

async def check_and_publish_newsletter(now, interval_minutes, last_weekly_newsletter_day, last_monthly_newsletter_day, newsletter_creator):
    day_of_year = now.tm_yday
    # Weekly Newsletter
    if should_publish_newsletter("weekly", now, interval_minutes, last_weekly_newsletter_day):
        logging.info("It's time to publish the weekly newsletter!")
        day_last_week = dt.now() - datetime.timedelta(days=7)
        try:
            newsletter_creator.create_weekly_newsletter(day_last_week.isocalendar())
        except Exception as e:
            logging.error(f"Error creating weekly newsletter: {e}")
        last_weekly_newsletter_day = day_of_year
    # Monthly Newsletter
    if should_publish_newsletter("monthly", now, interval_minutes, last_monthly_newsletter_day):
        logging.info("It's time to publish the monthly newsletter!")
        try:
            last_month = dt.now().month - 1 if dt.now().month > 1 else 12
            year = dt.now().year if dt.now().month > 1 else dt.now().year - 1
            newsletter_creator.create_monthly_newsletter(year, last_month)
        except Exception as e:
            logging.error(f"Error creating monthly newsletter: {e}")
        last_monthly_newsletter_day = day_of_year
    return last_weekly_newsletter_day, last_monthly_newsletter_day

async def core_loop(collector, newsletter_creator):
    logging.info("Starting core loop...")
    if DEBUG_MODE:
        logging.info("Debug mode is enabled. Waiting for 10 seconds before starting the newsletter creation.")
        await asyncio.sleep(10)
        day_last_week = dt.now() - datetime.timedelta(days=7)
        newsletter_creator.create_weekly_newsletter(day_last_week.isocalendar())
        last_month = dt.now().month - 1 if dt.now().month > 1 else 12
        year = dt.now().year if dt.now().month > 1 else dt.now().year - 1
        newsletter_creator.create_monthly_newsletter(year, last_month)
    await asyncio.sleep(DATA_COLLECTION_INTERVAL)
    last_weekly_newsletter_day = None
    last_monthly_newsletter_day = None
    interval_minutes = DATA_COLLECTION_INTERVAL // 60
    while True:
        start_time = time.time()
        now = time.localtime()
        # Newsletter-Check ausgelagert
        last_weekly_newsletter_day, last_monthly_newsletter_day = await check_and_publish_newsletter(
            now, interval_minutes, last_weekly_newsletter_day, last_monthly_newsletter_day, newsletter_creator
        )
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
        sleep_time = DATA_COLLECTION_INTERVAL - elapsed_time
        if sleep_time > 0:
            logging.debug(f"Sleeping for {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)

async def main():
    setup_logging()
    database = Database()
    # Starte Webserver für Statistiken
    from web_server import run_webserver
    run_webserver(database)

    data = get_data()
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    discord_client = DiscordClient(intents=intents)
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
