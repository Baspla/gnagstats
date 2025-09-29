import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from requests import exceptions as req_exc
from discord import ActivityType

from data_storage.db import Database
from discord_bot import DiscordClient
from steam_web_api import Steam


class DataCollector:

    def __init__(self, data, discord_client: DiscordClient, database: Database, steam:Steam):
        self.data = data
        self.discord_client = discord_client
        self.db = database
        self.steam = steam

    async def collect_discord_data(self):
        if not self.discord_client.is_ready():
            logging.warning("Discord client is not ready.")
            return
        logging.debug("Collecting Discord data...")
        timestamp = (datetime.now().timestamp()// 300) * 300
        for guild in self.discord_client.guilds:
            logging.debug(f"Guild: {guild.name} (ID: {guild.id})")
            if str(guild.id) in self.data["guild_ids"]:
                for channel in guild.voice_channels:
                    user_count = len(channel.members)
                    tracked_users = 0
                    logging.debug(f"Voice channel {channel.name} has {user_count} users.")
                    for member in channel.members:
                        if str(member.id) in self.data["user_discord_ids"]:  # Nur Eingetragene Leute tracken
                            tracked_users += 1
                            logging.debug(f"Tracking user {member.name} (ID: {member.id}) in channel {channel.name}.")
                            self.db.insert_discord_voice_activity(timestamp,str(member.id), channel.name, str(guild.id))
                            logging.debug(f"User {member.name} is playing {member.activity if member.activity else 'No Activity'}")
                    if user_count > 0:
                        self.db.insert_discord_voice_channel(timestamp,
                            channel.name,
                            str(guild.id),
                            user_count,
                            tracked_users
                        )
                # Separater Loop für Aktivitätstracking aller getrackten Nutzer (unabhängig von Voice-Channel)
                for member in guild.members:
                    if str(member.id) in self.data["user_discord_ids"]:
                        if member.activity:
                            if member.activity.type == ActivityType.playing:
                                logging.debug(f"User {member.name} discord activity details: {member.activity}")
                                self.db.insert_discord_game_activity(timestamp, str(member.id), str(member.activity.name))
                            else:
                                logging.debug(f"User {member.name} activity is not of type 'playing': {member.activity.type}")
        pass

    async def collect_steam_data(self):
        """Collect Steam presence/game activity for configured Steam user IDs.

        Adds defensive error handling around the external HTTP calls so that
        transient SSL / network problems (like handshake failures) don't break
        the whole loop. Implements a small retry for specific network errors.
        """
        logging.debug("Collecting Steam data...")
        timestamp = (datetime.now().timestamp() // 300) * 300

        # Configurable simple retry parameters (could later move to config)
        max_retries = 2
        base_backoff = 1.5  # seconds

        for user_id in self.data.get("user_steam_ids", []):
            logging.debug(f"Collecting data for Steam user {user_id}")
            attempt = 0
            user_details: Optional[Dict[str, Any]] = None
            while attempt <= max_retries:
                try:
                    # The underlying library uses synchronous requests; we just wrap it.
                    user_details = self.steam.users.get_user_details(user_id)
                    break
                except (req_exc.SSLError, req_exc.ConnectionError, req_exc.Timeout) as net_err:
                    # SSL handshake errors typically fall under SSLError
                    logging.warning(
                        f"Steam request network/SSL issue for user {user_id} (attempt {attempt + 1}/{max_retries + 1}): {net_err}"  # noqa: E501
                    )
                    if attempt < max_retries:
                        backoff = base_backoff * (2 ** attempt)
                        await self._async_sleep(backoff)
                        attempt += 1
                        continue
                    else:
                        logging.error(f"Giving up on Steam user {user_id} after {attempt + 1} attempts due to network/SSL errors.")
                        break
                except Exception as e:
                    # Catch-all to prevent loop crash; still surfaced in logs.
                    logging.exception(f"Unexpected error fetching Steam user {user_id}: {e}")
                    break

            if not user_details:
                logging.debug(f"No data returned for Steam user {user_id} (maybe profile private or request failed).")
                continue

            logging.debug(f"User data: {user_details}")
            player = user_details.get("player", {})
            game_name = player.get("gameextrainfo")
            if game_name:
                logging.debug(f"User {user_id} is playing steam game: {game_name}")
                try:
                    self.db.insert_steam_game_activity(timestamp, player.get("steamid", str(user_id)), game_name)
                except Exception as db_err:
                    logging.error(f"DB insert failed for Steam game activity (user {user_id}): {db_err}")
        # explicit return not required

    async def _async_sleep(self, seconds: float):
        """Isolated small awaitable sleep to make retry logic testable/mutable."""
        if seconds > 0:
            # Local import of asyncio to avoid adding a top-level dependency just for a utility
            import asyncio  # noqa: WPS433
            await asyncio.sleep(seconds)
