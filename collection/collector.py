import logging
from datetime import datetime

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
        logging.debug("Collecting Steam data...")
        timestamp = (datetime.now().timestamp()// 300) * 300
        for user in self.data["user_steam_ids"]:
            logging.debug(f"Collecting data for Steam user {user}")
            # Maybe personaname änderungen tracken?
            user = self.steam.users.get_user_details(user)
            if user:
                logging.debug(f"User data: {user}")
                if user["player"].get("gameextrainfo"):
                    logging.debug(f"User is playing steam game: {user["player"]["gameextrainfo"]}")
                    self.db.insert_steam_game_activity(timestamp,user["player"]["steamid"],user["player"]["gameextrainfo"])
            else:
                logging.debug(f"Failed to collect data for Steam user {user}")
        pass
