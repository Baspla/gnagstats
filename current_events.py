import logging
from datetime import datetime

import discord
from steam_web_api import Steam

from discord_bot import DiscordClient


class CurrentEventFetcher:
    def __init__(self, discord_client: DiscordClient, data,steam:Steam):
        self.discord_client = discord_client
        self.data = data
        self.steam = steam

    def get_guild_events(self):
        for guild in self.discord_client.guilds:
            if str(guild.id) in self.data["guild_ids"]:
                return guild.scheduled_events

    def unfiltered(self):
        return [event for event in self.get_guild_events()]

    def get_active_guild_events(self):
        for event in self.get_guild_events():
            logging.debug(f"Event: {event.name} (ID: {event.id})")

        return [event for event in self.get_guild_events() if event.status == discord.EventStatus.active]

    def get_non_active_guild_events_starting_until(self, until: datetime, ends_after: datetime):
        result = []
        for event in self.get_guild_events():
            # Ensure event.start_time and event.end_time are both not None
            if (
                event.status != discord.EventStatus.active and
                event.start_time is not None and
                event.end_time is not None and
                event.start_time <= until.astimezone() and
                event.end_time >= ends_after.astimezone()
            ):
                result.append(event)
        return result


    def get_birthdays(self):
        return self.data["user_birthdays"]

    def get_birthdays_until(self,start: datetime, until: datetime):
        birthdays = []
        for birthday in self.data["user_birthdays"]:
            if start < birthday["birthday"].replace(year=start.year) < until:
                birthday["next_birthday"] = birthday["birthday"].replace(year=start.year)
                birthdays.append(birthday)
            elif start < birthday["birthday"].replace(year=start.year + 1) < until:
                birthday["next_birthday"] = birthday["birthday"].replace(year=start.year + 1)
                birthdays.append(birthday)

        birthdays.sort(key=lambda x: x["next_birthday"])

        return birthdays