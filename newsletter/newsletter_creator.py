import logging
from datetime import datetime as dt
import datetime

from collection.current_events import CurrentEventFetcher
from data_storage.db import Database, timesteps_to_human_readable, seconds_to_human_readable
from jinja2 import Environment, FileSystemLoader, Template
from config import BASE_URL, DISCORD_WEBHOOK_URL, JSON_DATA_PATH
import requests
import locale
import pandas as pd

locale.setlocale(locale.LC_TIME, "de_DE.UTF-8")


def post_to_discord(template: Template, data: dict):
    rendered = template.render(data)
    rendered = rendered.replace("\n", "\\n")
    rendered = rendered.replace('\t', '\\t')
    logging.debug("Rendered newsletter content:")
    logging.debug(rendered)
    # Remove empty lines
    while "\\n\\n" in rendered:
        rendered = rendered.replace("\\n\\n", "\\n")
    while "\\n\\t" in rendered:
        rendered = rendered.replace("\\n\\t", "\\n")
    while "\\n " in rendered: # meine variante eines lstrip
        rendered = rendered.replace("\\n ", "\\n")
    # replace <br> with \n
    rendered = rendered.replace("<br>", "\\n")
    rendered = rendered.replace("\\n<inline>", "")
    rendered = rendered.replace("<nop>", "")
    discord_payload = f"""{{
        "embeds": [
            {{
                "description": "{rendered}",
                "timestamp": "{dt.now(datetime.timezone.utc).isoformat()}",
                "color": 14924912,
                "fields": []
            }}
        ],
        "components": []
    }}"""
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

def calculate_statistics(current_value, past_value):
    """Calculate the change between current and past values.
     Returns a dictionary with absolute and percentage change.
    """
    absolute_change = current_value - past_value
    percentage_change = (absolute_change / past_value * 100) if past_value != 0 else None
    return {
            "current": current_value,
            "past": past_value,
            "change": {
                "absolute": absolute_change,
                "percentage": percentage_change
            }
        }
    
def calculate_list_statistics(current_list, past_list):
    """Calculate statistics for lists of items based on a specific key.
    Returns a dictionary with current_total, past_total, change statistics and entries with:
    name, rank, current_value, past_value_this, past_value_rank with change statistics for each.
    past_value_rank ist the value of the item in the past list at the same rank as in the current list.
    past_value_this is the value of the same item in the past list.
    the list is a list of tuples with name and value.
    """
    current_total = sum(item[1] for item in current_list)
    past_total = sum(item[1] for item in past_list)
    absolute_total_change = current_total - past_total
    percentage_total_change = (absolute_total_change / past_total * 100) if past_total != 0 else None
    
    current_count = len(current_list)
    past_count = len(past_list)
    absolute_count_change = current_count - past_count
    percentage_count_change = (absolute_count_change / past_count * 100) if past_count != 0 else None
    # Create a mapping from identifier to past value for easy lookup
    past_value_map = {item[0]: item[1] for item in past_list}
    entries = []
    for rank, item in enumerate(current_list, start=1):
        current_value = item[1]
        past_value_this = past_value_map.get(item[0], None)
        past_value_rank = past_list[rank - 1][1] if rank - 1 < len(past_list) else None
        
        absolute_change_this = (current_value - past_value_this) if past_value_this is not None else None
        percentage_change_this = (absolute_change_this / past_value_this * 100) if past_value_this != None and past_value_this != 0 else None

        absolute_change_rank = (current_value - past_value_rank) if past_value_rank is not None else None
        percentage_change_rank = (absolute_change_rank / past_value_rank * 100) if past_value_rank != None and past_value_rank != 0 else None

        entry = {
            "name": item[0],
            "rank": rank,
            "current_value": current_value,
            "past_value_this": past_value_this,
            "past_value_rank": past_value_rank,
            "past_rankholder": past_list[rank - 1][0] if rank - 1 < len(past_list) else None,
            "change_this": {
                "absolute": absolute_change_this,
                "percentage": percentage_change_this
            } if past_value_this is not None else None,
            "change_rank": {
                "absolute": absolute_change_rank,
                "percentage": percentage_change_rank
            }
        }
        entries.append(entry)

    return {
        "total": {
            "current": current_total,
            "past": past_total,
            "change": {
                "absolute": absolute_total_change,
                "percentage": percentage_total_change
            },
        },
        "count": {
            "current": current_count,
            "past": past_count,
            "change": {
                "absolute": absolute_count_change,
                "percentage": percentage_count_change
            },
        },
        "entries": entries
    }
    
def calculate_game_session_statistics(current_df:pd.DataFrame, past_df:pd.DataFrame, key:str):
    current_total = current_df[key].sum()
    past_total = past_df[key].sum()
    absolute_total_change = current_total - past_total 
    percentage_total_change = (absolute_total_change / past_total * 100) if past_total != 0 else None

    current_count = len(current_df)
    past_count = len(past_df)
    absolute_count_change = current_count - past_count
    percentage_count_change = (absolute_count_change / past_count * 100) if past_count != 0 else None

    entries = []
    for rank, (_, row) in enumerate(current_df.iterrows(), start=1):
        current_value = row[key]
        past_value_rank = past_df.iloc[rank - 1][key] if rank - 1 < len(past_df) else None

        absolute_change_rank = (current_value - past_value_rank) if past_value_rank is not None else None
        percentage_change_rank = (absolute_change_rank / past_value_rank * 100) if past_value_rank != None and past_value_rank != 0 else None

        entry = {
            "game_name": row.get("game_name", "Unknown"),
            "user_name": row.get("user_name", "Unknown"),
            "source": row.get("source", "Unknown"),
            "rank": rank,
            "past_rankholder": past_df.iloc[rank - 1]["user_name"] if rank - 1 < len(past_df) else None,
            "past_rankholder_game": past_df.iloc[rank - 1]["game_name"] if rank - 1 < len(past_df) else None,
            "current_value": current_value,
            "change_rank": {
                "absolute": absolute_change_rank,
                "percentage": percentage_change_rank
            }
        }
        entries.append(entry)
    return {
        "total": {
            "current": current_total,
            "past": past_total,
            "change": {
                "absolute": absolute_total_change,
                "percentage": percentage_total_change
            }
        },
        "count": {
            "current": current_count,
            "past": past_count,
            "change": {
                "absolute": absolute_count_change,
                "percentage": percentage_count_change
            }
        },
        "entries": entries
    }

def query_value(fun,past_start,past_end,current_start,current_end):
    current = fun(current_start,current_end)
    past = fun(past_start,past_end)
    return calculate_statistics(current,past)

def query_value_df(fun, past_df, current_df):
    current = fun(current_df)
    past = fun(past_df)
    return calculate_statistics(current, past)

def query_list_df(fun, past_df, current_df):
    current = fun(current_df)
    past = fun(past_df)
    return calculate_list_statistics(current, past)

def query_game_sessions_df(fun, past_df, current_df, key):
    current = fun(current_df)
    past = fun(past_df)
    return calculate_game_session_statistics(current, past, key)

class NewsletterCreator:
    def __init__(self, current_event_fetcher: CurrentEventFetcher, database: Database):
        self.current_event_fetcher = current_event_fetcher
        self.db = database
        
    def prepare_template_data(self,past_start:dt, past_end:dt, current_start:dt, current_end:dt, future_start:dt, future_end:dt) -> dict:
        voice_total = query_value(self.db.newsletter_query_get_voice_total,past_start,past_end,current_start,current_end)

        voice_alone = query_value(self.db.newsletter_query_get_voice_alone,past_start,past_end,current_start,current_end)
        
        voice_together = query_value(self.db.newsletter_query_get_voice_together,past_start,past_end,current_start,current_end)
        
        past_game_df = self.db.query_get_game_activity_dataframe(past_start, past_end)
        current_game_df = self.db.query_get_game_activity_dataframe(current_start, current_end)

        gaming_total = query_value_df(self.db.newsletter_query_get_gaming_total,past_game_df,current_game_df)
        
        most_playtime = query_list_df(self.db.newsletter_query_get_playtime,past_game_df,current_game_df)
        
        biggest_groups = query_list_df(self.db.newsletter_query_get_biggest_groups,past_game_df,current_game_df)

        longest_sessions = query_game_sessions_df(self.db.newsletter_query_get_longest_sessions,past_game_df,current_game_df,"duration_seconds")
        
        link = f"{BASE_URL}"

        data = {
            "link": link,
            "events": {
                "active": self.current_event_fetcher.get_active_guild_events(),
                "upcoming": self.current_event_fetcher.get_non_active_guild_events_starting_until(future_end, current_end),
            },
            "birthdays": self.current_event_fetcher.get_birthdays_until(current_end, future_end),
            "title_period": {
                "year": current_start.year,
                "month": current_start.strftime("%B"),
                "calendar_week": current_start.isocalendar().week,
            },
            "timespans": {
                "past": {
                    "start": past_start,
                    "end": past_end,
                },
                "current": {
                    "start": current_start,
                    "end": current_end,
                },
                "future": {
                    "start": future_start,
                    "end": future_end,
                },
            },
            "voice": {
                "total": voice_total,
                "alone": voice_alone,
                "together": voice_together
            },
            "gaming": {
                "total": gaming_total,
                "most_playtime": most_playtime,
                "biggest_groups": biggest_groups,
                "longest_sessions": longest_sessions,
            },
        }
        return data

    def create_monthly_newsletter(self,year:int,month:int):
        logging.info(f"Creating monthly newsletter for {year}-{month}.")
        month_start = dt(year, month, 1)
        month_end = dt(year, month + 1, 1) if month < 12 else dt(year + 1, 1, 1)
        
        previous_month_start = dt(year, month - 1, 1) if month > 1 else dt(year - 1, 12, 1)
        previous_month_end = month_start

        future = month_end + datetime.timedelta(days=28)

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['timesteps_to_human_readable'] = timesteps_to_human_readable
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
        template:Template = env.get_template('newsletter_template_month.jinja2')
        data = self.prepare_template_data(past_start=previous_month_start,past_end=previous_month_end,current_start=month_start,current_end=month_end,future_start=dt.now(),future_end=future)

        post_to_discord(template,data)

    def create_weekly_newsletter(self,calendar_date):
        year = calendar_date.year
        calendar_week = calendar_date.week
        logging.info(f"Creating weekly newsletter for week {calendar_week} of year {year}.")
        week_start = dt.fromisocalendar(year, calendar_week, 1)
        week_end = dt.fromisocalendar(year, calendar_week, 7) + datetime.timedelta(days=1) # damit der ganze Sonntag drin ist
        previous_week_start = week_start - datetime.timedelta(days=7)
        previous_week_end = week_start

        future = week_end + datetime.timedelta(days=28)

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['timesteps_to_human_readable'] = timesteps_to_human_readable
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
        template = env.get_template('newsletter_template_week.jinja2')

        data = self.prepare_template_data(past_start=previous_week_start,past_end=previous_week_end,current_start=week_start,current_end=week_end,future_start=dt.now(),future_end=future)
        # write data to file for debugging
        filename = f"newsletter_week_{year}_{calendar_week}.json"
        with open(filename, "w", encoding="utf-8") as f:
            import json
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        post_to_discord(template,data)

    def create_yearly_newsletter(self,year:int):
        logging.info(f"Creating yearly newsletter for {year}.")
        year_start = dt(year, 1, 1)
        year_end = dt(year + 1, 1, 1)
        previous_year_start = dt(year - 1, 1, 1)
        previous_year_end = dt(year, 1, 1)

        future = year_end + datetime.timedelta(days=28)

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['timesteps_to_human_readable'] = timesteps_to_human_readable
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
        template = env.get_template('newsletter_template_year.jinja2')

        data = self.prepare_template_data(past_start=previous_year_start,past_end=previous_year_end,current_start=year_start,current_end=year_end,future_start=dt.now(),future_end=future)

        post_to_discord(template,data)