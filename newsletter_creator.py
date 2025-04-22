import logging
from datetime import datetime as dt
import datetime

from db import Database
from jinja2 import Environment, FileSystemLoader
from config import DISCORD_WEBHOOK_URL
import requests


def post_to_discord(rendered):
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


def datetime_to_timestamp(value):
    if value is None:
        return 0
    return int(value.timestamp())

class NewsletterCreator:
    def __init__(self, current_event_fetcher, database: Database):
        self.current_event_fetcher = current_event_fetcher
        self.db = database
        pass

    def gather_data(self,past_start:datetime, past_end:datetime, future_start:datetime, future_end:datetime):
        int_lone_time = self.db.get_discord_total_lonely_voice_activity(past_start, past_end)[0]
        logging.debug(f"Lonely time: {int_lone_time}")
        lone_time = self.db.timesteps_to_human_readable(int_lone_time)

        int_voice_time = self.db.get_discord_total_voice_activity(past_start, past_end)[0]
        voice_time = self.db.timesteps_to_human_readable(int_voice_time)

        unique_voice = self.db.get_discord_unique_active_users(past_start, past_end)[0]
        if unique_voice is None:
            unique_voice = 0

        busiest_channels = self.db.get_discord_busiest_voice_channels(past_start, past_end)
        busiest_channel = busiest_channels[0][0] if busiest_channels else None

        most_played_list = self.db.get_steam_most_played_games(past_start, past_end)
        most_played_together_list = self.db.get_steam_most_played_together(past_start, past_end)
        most_concurrent_list = self.db.get_steam_most_concurrent_players(past_start, past_end)
        most_played_list_capped = most_played_list[:5] if most_played_list else None
        most_played_together_list_capped = most_played_together_list[:3] if most_played_together_list else None
        most_concurrent_list_capped = most_concurrent_list[:3] if most_concurrent_list else None


        most_played = most_played_list[0][0] if most_played_list else None
        most_played_together = most_played_together_list[0][0] if most_played_together_list else None
        most_concurrent = most_concurrent_list[0][0] if most_concurrent_list else None
        active_events = self.current_event_fetcher.get_active_guild_events()
        non_active_events = self.current_event_fetcher.get_non_active_guild_events_starting_until(future_end)
        birthdays = self.current_event_fetcher.get_birthdays_until(future_start,future_end)

        data = {
            "discord_time_alone": lone_time, # Wie lange war jemand alleine in einem Voice Channel
            "discord_time_voice": voice_time, # Wie lange waren alle in einem Voice Channel kumuliert
            "discord_users_voice": unique_voice, # Wie viele einzigartige User waren in einem Voice Channel
            "discord_busiest_channel": busiest_channel, # Welcher Channel war am meisten besucht
            "steam_most_played": most_played, # Welches Spiel wurde am meisten gespielt
            "steam_most_played_together": most_played_together, # Welches Spiel wurde am meisten mit zwei oder mehr Leuten gespielt
            "steam_most_concurrent_players": most_concurrent, # Welches Spiel hatte die meisten Spieler gleichzeitig
            "steam_most_played_list": most_played_list, # Liste der Spiele mit den meisten Spielstunden
            "steam_most_played_together_list": most_played_together_list, # Liste der Spiele mit den meisten Spielstunden mit zwei oder mehr Leuten
            "steam_most_concurrent_list": most_concurrent_list, # Liste der Spiele mit den meisten Spielern gleichzeitig
            "discord_busiest_channels_list": busiest_channels, # Liste der Channels mit den meisten Besuchern
            "steam_most_played_list_capped": most_played_list_capped, # Liste der Spiele mit den meisten Spielstunden (max 5)
            "steam_most_played_together_list_capped": most_played_together_list_capped, # Liste der Spiele mit den meisten Spielstunden mit zwei oder mehr Leuten (max 3)
            "steam_most_concurrent_list_capped": most_concurrent_list_capped, # Liste der Spiele mit den meisten Spielern gleichzeitig (max 3)
            "discord_active_events": active_events, # Welche Discord Events sind aktiv
            "discord_non_active_events": non_active_events, # Welche Discord Events sind nicht aktiv
            "birthdays": birthdays # Welche Geburtstage stehen an
        }
        # birthdays are a list of dictionaries with keys "name" and "birthday" and "next_birthday"
        # discord events are a list of objects with parameters "name", "start_time", "end_time", "description"
        # discord_time_alone and discord_time_voice are strings in the format "X Tage Y Stunden Z Minuten"
        return data

    def create_monthly_newsletter(self,year:int,month:int):
        logging.info(f"Creating monthly newsletter for {year}-{month}.")
        month_start = dt(year, month, 1)
        month_end = dt(year, month + 1, 1) if month < 12 else dt(year + 1, 1, 1)

        future = month_end + datetime.timedelta(days=28)
        past_start = month_start
        past_end = month_end

        env = Environment(loader=FileSystemLoader('templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        template = env.get_template('newsletter_template_month.jinja2')

        data = self.gather_data(past_start,past_end,dt.now(),future)
        data["month_name"] = month_start.strftime("%B")

        rendered = template.render(data)
        logging.debug(rendered)
        rendered = rendered.replace("\n", "\\n")
        post_to_discord(rendered)


    def create_weekly_newsletter(self,calendar_date):
        year = calendar_date.year
        calendar_week = calendar_date.week
        logging.info(f"Creating weekly newsletter for week {calendar_week} of year {year}.")
        week_start = dt.fromisocalendar(year, calendar_week, 1)
        week_end = dt.fromisocalendar(year, calendar_week, 7) + datetime.timedelta(days=1) # damit der ganze Sonntag drin ist
        logging.info(f"Week start: {week_start}")
        logging.info(f"Week end: {week_end}")

        future = week_end + datetime.timedelta(days=28)
        past_start = week_start
        past_end = week_end

        env = Environment(loader=FileSystemLoader('templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        template = env.get_template('newsletter_template_week.jinja2')

        data = self.gather_data(past_start,past_end,dt.now(),future)
        data["calendar_week"] = calendar_week

        rendered = template.render(data)
        logging.debug(rendered)
        rendered = rendered.replace("\n", "\\n")
        post_to_discord(rendered)

    # TODO ausführlichere Jahreszusammenfassung
    def create_yearly_newsletter(self,year:int):
        logging.info(f"Creating yearly newsletter for {year}.")
        year_start = dt(year, 1, 1)
        year_end = dt(year + 1, 1, 1)

        future = year_end + datetime.timedelta(days=28)
        past_start = year_start
        past_end = year_end

        env = Environment(loader=FileSystemLoader('templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        template = env.get_template('newsletter_template_year.jinja2')

        data = self.gather_data(past_start,past_end,dt.now(),future)
        data["year"] = year

        rendered = template.render(data)
        logging.debug(rendered)
        rendered = rendered.replace("\n", "\\n")
        post_to_discord(rendered)