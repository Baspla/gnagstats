import logging
from datetime import datetime as dt
import datetime

from db import Database
from jinja2 import Environment, FileSystemLoader
from config import DISCORD_WEBHOOK_URL
import requests


class NewsletterCreator:
    def __init__(self, current_event_fetcher, database: Database):
        self.current_event_fetcher = current_event_fetcher
        self.db = database
        pass

    #TODO Newsletter für bestimmte KW erstellen nicht "letzten 7 Tage und hoffen das es Montag ist"
    def create_weekly_newsletter(self):
        future = dt.now() + datetime.timedelta(days=31)
        past = int(
            (dt.now() - datetime.timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        now = int(dt.now().timestamp())

        int_lone_time = self.db.get_discord_total_lonely_voice_activity(past, now)[0]
        logging.debug(f"Lonely time: {int_lone_time}")
        lone_time = self.db.timesteps_to_human_readable(int_lone_time)

        int_voice_time = self.db.get_discord_total_voice_activity(past, now)[0]
        voice_time = self.db.timesteps_to_human_readable(int_voice_time)

        unique_voice = self.db.get_discord_unique_active_users(past, now)[0]
        if unique_voice is None:
            unique_voice = 0

        busiest_channels = self.db.get_discord_busiest_voice_channels(past, now)
        busiest_channel = busiest_channels[0][0] if busiest_channels else None

        most_played_list = self.db.get_steam_most_played_games(past, now)
        most_played_together_list = self.db.get_steam_most_played_together(past, now)
        most_concurrent_list = self.db.get_steam_most_concurrent_players(past, now)

        most_played = most_played_list[0][0] if most_played_list else None
        most_played_together = most_played_together_list[0][0] if most_played_together_list else None
        most_concurrent = most_concurrent_list[0][0] if most_concurrent_list else None
        calendar_week = str((dt.now() - datetime.timedelta(weeks=1)).isocalendar()[1])
        active_events = self.current_event_fetcher.get_active_guild_events()
        non_active_events = self.current_event_fetcher.get_non_active_guild_events_starting_until(future)
        birthdays = self.current_event_fetcher.get_birthdays_until(future)

        env = Environment(loader=FileSystemLoader('templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)



        def datetime_to_timestamp(value):
            if value is None:
                return 0
            return int(value.timestamp())

        # Add the custom filter to your Jinja environment
        env.filters['datetime_to_timestamp'] = datetime_to_timestamp

        template = env.get_template('newsletter_template.jinja2')
        data = {
            "calendar_week": calendar_week,
            "discord_time_alone": lone_time,
            "discord_time_voice": voice_time,
            "discord_users_voice": unique_voice,
            "discord_busiest_channel": busiest_channel,
            "steam_most_played": most_played,
            "steam_most_played_together": most_played_together,
            "steam_most_concurrent_players": most_concurrent,
            "discord_active_events": active_events,
            "discord_non_active_events": non_active_events,
            "birthdays": birthdays
        }
        # birthdays are a list of dictionaries with keys "name" and "birthday" and "next_birthday"
        # discord events are a list of objects with parameters "name", "start_time", "end_time", "description"
        # discord_time_alone and discord_time_voice are strings in the format "X Tage Y Stunden Z Minuten"

        rendered = template.render(data)
        logging.debug(rendered)
        rendered = rendered.replace("\n", "\\n")
        discord_payload = f"""{{
            "embeds": [
                {{
                    "description": "{rendered}",
                    "timestamp": "{dt.now().isoformat()}",
                    "color": 14924912,
                    "fields": []
                }}
            ],
            "components": []
        }}"""

        logging.debug(discord_payload)
        # Send the payload to the Discord webhook
        logging.info("Sending newsletter to Discord webhook...")

        # Use the requests library to send the payload
        response = requests.post(DISCORD_WEBHOOK_URL, data=discord_payload, headers={"Content-Type": "application/json"})
        if response.status_code == 204:
            logging.info("Newsletter sent successfully.")
        else:
            logging.error(f"Failed to send newsletter: {response.status_code} - {response.text}")
        pass


