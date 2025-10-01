import logging
from datetime import datetime as dt
import datetime

from data_storage.db import Database, timesteps_to_human_readable, seconds_to_human_readable
from jinja2 import Environment, FileSystemLoader
from config import BASE_URL, DISCORD_WEBHOOK_URL, JSON_DATA_PATH
import requests
import locale
import pandas as pd

# Mapping Hilfsfunktionen wie im DataProvider verwenden
from data_storage.json_data import (
    load_json_data,
    get_user_id_to_name_map,
    get_steam_id_to_user_id_map,
    get_discord_id_to_user_id_map,
)

locale.setlocale(locale.LC_TIME, "de_DE.UTF-8")


def post_to_discord(rendered):
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

    def gather_data(self, past_start:datetime, past_end:datetime, future_start:datetime, future_end:datetime):
        """
        Sammelt alle relevanten Statistiken für den Newsletter-Zeitraum und berechnet die Veränderung zum Vorzeitraum.

        Args:
            past_start (datetime): Startzeitpunkt des betrachteten Zeitraums (Vergangenheit).
            past_end (datetime): Endzeitpunkt des betrachteten Zeitraums (Vergangenheit).
            future_start (datetime): Startzeitpunkt für zukünftige Events/Geburtstage.
            future_end (datetime): Endzeitpunkt für zukünftige Events/Geburtstage.

        Returns:
            dict: Enthält alle Kennzahlen und Listen für den Newsletter, inkl. Veränderung zum Vorzeitraum.
                - discord_time_alone: Gesamtzeit, die User alleine im Voice Channel verbracht haben (int)
                - discord_time_alone_change_abs: absolute Veränderung zur Vorperiode (int)
                - discord_time_alone_change_pct: prozentuale Veränderung zur Vorperiode (float)
                - discord_time_voice: Gesamtzeit aller Voice-Aktivität (int)
                - discord_time_voice_change_abs: absolute Veränderung zur Vorperiode (int)
                - discord_time_voice_change_pct: prozentuale Veränderung zur Vorperiode (float)
                - discord_users_voice: Anzahl einzigartiger User im Voice Channel (int)
                - discord_users_voice_change_abs: absolute Veränderung zur Vorperiode (int)
                - discord_users_voice_change_pct: prozentuale Veränderung zur Vorperiode (float)
                - discord_busiest_channel: Name des meistbesuchten Voice Channels (str)
                - steam_most_played: Name des meistgespielten Steam-Spiels (str)
                - steam_most_played_list: Liste der meistgespielten Steam-Spiele [(str, int)]
                - steam_most_played_list_change_abs: absolute Veränderung der Spielstunden zur Vorperiode (int)
                - steam_most_played_list_change_pct: prozentuale Veränderung der Spielstunden zur Vorperiode (float)
                - steam_most_played_together: Name des meistgespielten Spiels mit mehreren Spielern (str)
                - steam_most_played_together_list: Liste der meistgespielten Spiele mit mehreren Spielern [(str, int)]
                - steam_most_played_together_list_change_abs: absolute Veränderung der Spielstunden mit mehreren Spielern (int)
                - steam_most_played_together_list_change_pct: prozentuale Veränderung der Spielstunden mit mehreren Spielern (float)
                - steam_most_concurrent_players: Name des Spiels mit den meisten gleichzeitigen Spielern (str)
                - steam_most_concurrent_list: Liste der Spiele mit den meisten gleichzeitigen Spielern [(str, int)]
                - steam_most_concurrent_list_change_abs: absolute Veränderung der maximalen gleichzeitigen Spieler (int)
                - steam_most_concurrent_list_change_pct: prozentuale Veränderung der maximalen gleichzeitigen Spieler (float)
                - discord_busiest_channels_list: Liste der meistbesuchten Voice Channels [(str, int)]
                - steam_most_played_list_capped: Top 5 meistgespielte Steam-Spiele
                - steam_most_played_together_list_capped: Top 3 meistgespielte Spiele mit mehreren Spielern
                - steam_most_concurrent_list_capped: Top 3 Spiele mit den meisten gleichzeitigen Spielern
                - discord_active_events: Liste der aktiven Discord-Events
                - discord_non_active_events: Liste der nicht aktiven Discord-Events im Zeitraum
                - birthdays: Liste der bevorstehenden Geburtstage
        """
        # Aktueller Zeitraum
        int_lone_time_row = self.db.newsletter_query_get_discord_total_lonely_voice_activity(past_start, past_end)
        int_lone_time = int_lone_time_row["total_voicetime"] if isinstance(int_lone_time_row, dict) and "total_voicetime" in int_lone_time_row else int_lone_time_row[0] if int_lone_time_row else 0
        int_voice_time_row = self.db.newsletter_query_get_discord_total_voice_activity(past_start, past_end)
        int_voice_time = int_voice_time_row["total_voicetime"] if isinstance(int_voice_time_row, dict) and "total_voicetime" in int_voice_time_row else int_voice_time_row[0] if int_voice_time_row else 0
        unique_voice_row = self.db.newsletter_query_get_discord_unique_active_users(past_start, past_end)
        unique_voice = unique_voice_row["unique_users"] if isinstance(unique_voice_row, dict) and "unique_users" in unique_voice_row else unique_voice_row[0] if unique_voice_row else 0
        if unique_voice is None:
            unique_voice = 0
        busiest_channels = self.db.newsletter_query_get_discord_busiest_voice_channels(past_start, past_end)
        busiest_channel = busiest_channels[0]["channel_name"] if busiest_channels and isinstance(busiest_channels[0], dict) and "channel_name" in busiest_channels[0] else busiest_channels[0][0] if busiest_channels else None
        most_played_list = self.db.newsletter_query_get_steam_most_played_games(past_start, past_end)
        most_played_together_list = self.db.newsletter_query_get_steam_most_played_together(past_start, past_end)
        most_concurrent_list = self.db.newsletter_query_get_steam_most_concurrent_players(past_start, past_end)
        most_played_list_capped = most_played_list[:5] if most_played_list else None
        most_played_together_list_capped = most_played_together_list[:3] if most_played_together_list else None
        most_concurrent_list_capped = most_concurrent_list[:3] if most_concurrent_list else None
        most_played = most_played_list[0]["game_name"] if most_played_list and isinstance(most_played_list[0], dict) and "game_name" in most_played_list[0] else most_played_list[0][0] if most_played_list else None
        most_played_together = most_played_together_list[0]["game_name"] if most_played_together_list and isinstance(most_played_together_list[0], dict) and "game_name" in most_played_together_list[0] else most_played_together_list[0][0] if most_played_together_list else None
        most_concurrent = most_concurrent_list[0]["game_name"] if most_concurrent_list and isinstance(most_concurrent_list[0], dict) and "game_name" in most_concurrent_list[0] else most_concurrent_list[0][0] if most_concurrent_list else None
        active_events = self.current_event_fetcher.get_active_guild_events()
        non_active_events = self.current_event_fetcher.get_non_active_guild_events_starting_until(future_end, future_start)
        birthdays = self.current_event_fetcher.get_birthdays_until(future_start,future_end)

        # Vorzeitraum berechnen
        period_length = (past_end - past_start)
        prev_start = past_start - period_length
        prev_end = past_start
        prev_lone_time_row = self.db.newsletter_query_get_discord_total_lonely_voice_activity(prev_start, prev_end)
        prev_lone_time = prev_lone_time_row["total_voicetime"] if isinstance(prev_lone_time_row, dict) and "total_voicetime" in prev_lone_time_row else prev_lone_time_row[0] if prev_lone_time_row else 0
        prev_voice_time_row = self.db.newsletter_query_get_discord_total_voice_activity(prev_start, prev_end)
        prev_voice_time = prev_voice_time_row["total_voicetime"] if isinstance(prev_voice_time_row, dict) and "total_voicetime" in prev_voice_time_row else prev_voice_time_row[0] if prev_voice_time_row else 0
        prev_unique_voice_row = self.db.newsletter_query_get_discord_unique_active_users(prev_start, prev_end)
        prev_unique_voice = prev_unique_voice_row["unique_users"] if isinstance(prev_unique_voice_row, dict) and "unique_users" in prev_unique_voice_row else prev_unique_voice_row[0] if prev_unique_voice_row else 0
        if prev_unique_voice is None:
            prev_unique_voice = 0
        prev_most_played_list = self.db.newsletter_query_get_steam_most_played_games(prev_start, prev_end)
        prev_most_played_together_list = self.db.newsletter_query_get_steam_most_played_together(prev_start, prev_end)
        prev_most_concurrent_list = self.db.newsletter_query_get_steam_most_concurrent_players(prev_start, prev_end)

        # ---------------------------------------------------------------
        # Kombinierte (Steam + Discord) Spielstatistiken (analog DataProvider)
        # ---------------------------------------------------------------
        def _load_raw_game_activity(start_dt: datetime.datetime, end_dt: datetime.datetime):
            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())
            steam_rows = self.db.web_query_get_steam_game_activity(start_ts, end_ts)
            discord_rows = self.db.web_query_get_discord_game_activity(start_ts, end_ts)
            return steam_rows, discord_rows

        def _build_dataframe(steam_rows, discord_rows):
            # steam
            if steam_rows:
                df_steam = pd.DataFrame(steam_rows, columns=["timestamp", "steam_id", "game_name", "collection_interval"])  # noqa: E501
            else:
                df_steam = pd.DataFrame(columns=["timestamp", "steam_id", "game_name", "collection_interval"])
            if discord_rows:
                df_discord = pd.DataFrame(discord_rows, columns=["timestamp", "discord_id", "game_name", "collection_interval"])  # noqa: E501
            else:
                df_discord = pd.DataFrame(columns=["timestamp", "discord_id", "game_name", "collection_interval"])

            if df_steam.empty and df_discord.empty:
                return pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"])

            json_data = load_json_data(JSON_DATA_PATH)
            id_map = get_user_id_to_name_map(json_data) if isinstance(json_data, dict) else {}
            steam_id_map = get_steam_id_to_user_id_map(json_data) if isinstance(json_data, dict) else {}
            discord_id_map = get_discord_id_to_user_id_map(json_data) if isinstance(json_data, dict) else {}

            if not df_steam.empty:
                df_steam["user_id"] = df_steam["steam_id"].astype(str).map(steam_id_map).fillna(df_steam["steam_id"].astype(str)) if steam_id_map else df_steam["steam_id"].astype(str)
                df_steam["user_name"] = df_steam["user_id"].astype(str).map(id_map).fillna(df_steam["user_id"].astype(str)) if id_map else df_steam["user_id"].astype(str)
                df_steam["source"] = "steam"
            if not df_discord.empty:
                df_discord["user_id"] = df_discord["discord_id"].astype(str).map(discord_id_map).fillna(df_discord["discord_id"].astype(str)) if discord_id_map else df_discord["discord_id"].astype(str)
                df_discord["user_name"] = df_discord["user_id"].astype(str).map(id_map).fillna(df_discord["user_id"].astype(str)) if id_map else df_discord["user_id"].astype(str)
                df_discord["source"] = "discord"

            df_all = pd.concat([
                df_steam[["timestamp", "user_name", "game_name", "collection_interval", "source"]] if not df_steam.empty else pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"]),
                df_discord[["timestamp", "user_name", "game_name", "collection_interval", "source"]] if not df_discord.empty else pd.DataFrame(columns=["timestamp", "user_name", "game_name", "collection_interval", "source"]),
            ], ignore_index=True)

            if df_all.empty:
                return df_all

            # Priorität: Steam überschreibt Discord bei gleicher (user_name, timestamp)
            df_all = df_all.sort_values(by=["user_name", "timestamp", "source"], ascending=[True, True, True])
            df_all["key"] = df_all["user_name"].astype(str) + "_" + df_all["timestamp"].astype(str)
            steam_keys = set(df_all[df_all["source"] == "steam"]["key"])  # Keys mit Steam-Eintrag
            df_all = df_all[(df_all["source"] == "steam") | (~df_all["key"].isin(steam_keys))]
            df_all = df_all.drop(columns=["key"])  # Cleanup
            return df_all.reset_index(drop=True)

        def _aggregate_most_played(df_all: pd.DataFrame):
            if df_all.empty:
                return []
            df = df_all.copy()
            df["collection_interval"] = df["collection_interval"].fillna(300)
            grouped = df.groupby("game_name").agg(count=("timestamp", "count"), total_playtime=("collection_interval", "sum")).reset_index()
            grouped = grouped.sort_values(by=["count", "total_playtime"], ascending=[False, False])
            return list(grouped.itertuples(index=False, name=None))  # (game_name, count, total_playtime)

        def _aggregate_most_concurrent(df_all: pd.DataFrame):
            if df_all.empty:
                return []
            df = df_all.copy()
            # Player pro (timestamp, game)
            concurrent = df.groupby(["timestamp", "game_name"])['user_name'].nunique().reset_index(name='player_count')
            concurrent = concurrent.sort_values(by="player_count", ascending=False)
            # Rückgabe analog Steam: (game_name, player_count)
            return list(concurrent[['game_name', 'player_count']].itertuples(index=False, name=None))

        def _aggregate_most_played_together(df_all: pd.DataFrame):
            if df_all.empty:
                return []
            df = df_all.copy()
            df["collection_interval"] = df["collection_interval"].fillna(300)
            # Gruppiere Snapshot-Ebene
            snap = df.groupby(["timestamp", "game_name"]).agg(
                player_count=("user_name", "nunique"),
                total_playtime=("collection_interval", "sum"),
            ).reset_index()
            snap = snap[snap["player_count"] >= 2]
            if snap.empty:
                return []
            agg = snap.groupby("game_name").agg(
                occurrences=("timestamp", "count"),
                total_playtime=("total_playtime", "sum"),
            ).reset_index()
            agg = agg.sort_values(by=["occurrences", "total_playtime"], ascending=[False, False])
            # (game_name, occurrences, total_playtime)
            return list(agg.itertuples(index=False, name=None))

        # Aktueller Zeitraum kombiniertes DF
        steam_rows_curr, discord_rows_curr = _load_raw_game_activity(past_start, past_end)
        df_combined_curr = _build_dataframe(steam_rows_curr, discord_rows_curr)
        combined_most_played_list = _aggregate_most_played(df_combined_curr)
        combined_most_concurrent_list = _aggregate_most_concurrent(df_combined_curr)
        combined_most_played_together_list = _aggregate_most_played_together(df_combined_curr)

        combined_most_played_list_capped = combined_most_played_list[:5] if combined_most_played_list else None
        combined_most_played_together_list_capped = combined_most_played_together_list[:3] if combined_most_played_together_list else None
        combined_most_concurrent_list_capped = combined_most_concurrent_list[:3] if combined_most_concurrent_list else None
        combined_most_played = combined_most_played_list[0][0] if combined_most_played_list else None
        combined_most_played_together = combined_most_played_together_list[0][0] if combined_most_played_together_list else None
        combined_most_concurrent = combined_most_concurrent_list[0][0] if combined_most_concurrent_list else None

        # Vorzeitraum kombiniert
        steam_rows_prev, discord_rows_prev = _load_raw_game_activity(prev_start, prev_end)
        df_combined_prev = _build_dataframe(steam_rows_prev, discord_rows_prev)
        prev_combined_most_played_list = _aggregate_most_played(df_combined_prev)
        prev_combined_most_concurrent_list = _aggregate_most_concurrent(df_combined_prev)
        prev_combined_most_played_together_list = _aggregate_most_played_together(df_combined_prev)

        # Hilfsfunktionen für Prozent und Absolut
        def calc_change(current, previous):
            abs_change = current - previous
            pct_change = ((current - previous) / previous * 100) if previous else 0
            return abs_change, pct_change

        # Summen für Listen berechnen
        def sum_list(lst):
            if not lst:
                return 0
            return sum([x[1] for x in lst if len(x) > 1 and isinstance(x[1], (int, float))])

        lone_abs, lone_pct = calc_change(int_lone_time or 0, prev_lone_time or 0)
        voice_abs, voice_pct = calc_change(int_voice_time or 0, prev_voice_time or 0)
        unique_abs, unique_pct = calc_change(unique_voice or 0, prev_unique_voice or 0)
        played_sum = sum_list(most_played_list)
        prev_played_sum = sum_list(prev_most_played_list)
        played_abs, played_pct = calc_change(played_sum, prev_played_sum)
        played_together_sum = sum_list(most_played_together_list)
        prev_played_together_sum = sum_list(prev_most_played_together_list)
        played_together_abs, played_together_pct = calc_change(played_together_sum, prev_played_together_sum)
        concurrent_sum = sum_list(most_concurrent_list)
        prev_concurrent_sum = sum_list(prev_most_concurrent_list)
        concurrent_abs, concurrent_pct = calc_change(concurrent_sum, prev_concurrent_sum)

        # Combined Veränderung berechnen
        combined_played_sum = sum_list(combined_most_played_list)
        prev_combined_played_sum = sum_list(prev_combined_most_played_list)
        combined_played_abs, combined_played_pct = calc_change(combined_played_sum, prev_combined_played_sum)

        combined_played_together_sum = sum_list(combined_most_played_together_list)
        prev_combined_played_together_sum = sum_list(prev_combined_most_played_together_list)
        combined_played_together_abs, combined_played_together_pct = calc_change(combined_played_together_sum, prev_combined_played_together_sum)

        combined_concurrent_sum = sum_list(combined_most_concurrent_list)
        prev_combined_concurrent_sum = sum_list(prev_combined_most_concurrent_list)
        combined_concurrent_abs, combined_concurrent_pct = calc_change(combined_concurrent_sum, prev_combined_concurrent_sum)

        link = f"{BASE_URL}?start_date={past_start.date()}&end_date={ past_end.date()}"

        data = {
            "discord_time_alone": int_lone_time,
            "discord_time_alone_change_abs": lone_abs,
            "discord_time_alone_change_pct": lone_pct,
            "discord_time_voice": int_voice_time,
            "discord_time_voice_change_abs": voice_abs,
            "discord_time_voice_change_pct": voice_pct,
            "discord_users_voice": unique_voice,
            "discord_users_voice_change_abs": unique_abs,
            "discord_users_voice_change_pct": unique_pct,
            "discord_busiest_channel": busiest_channel,
            "steam_most_played": most_played,
            "steam_most_played_list": most_played_list,
            "steam_most_played_list_change_abs": played_abs,
            "steam_most_played_list_change_pct": played_pct,
            "steam_most_played_together": most_played_together,
            "steam_most_played_together_list": most_played_together_list,
            "steam_most_played_together_list_change_abs": played_together_abs,
            "steam_most_played_together_list_change_pct": played_together_pct,
            "steam_most_concurrent_players": most_concurrent,
            "steam_most_concurrent_list": most_concurrent_list,
            "steam_most_concurrent_list_change_abs": concurrent_abs,
            "steam_most_concurrent_list_change_pct": concurrent_pct,
            "discord_busiest_channels_list": busiest_channels,
            "steam_most_played_list_capped": most_played_list_capped,
            "steam_most_played_together_list_capped": most_played_together_list_capped,
            "steam_most_concurrent_list_capped": most_concurrent_list_capped,
            # Kombinierte Statistiken
            "combined_most_played": combined_most_played,
            "combined_most_played_list": combined_most_played_list,
            "combined_most_played_list_change_abs": combined_played_abs,
            "combined_most_played_list_change_pct": combined_played_pct,
            "combined_most_played_together": combined_most_played_together,
            "combined_most_played_together_list": combined_most_played_together_list,
            "combined_most_played_together_list_change_abs": combined_played_together_abs,
            "combined_most_played_together_list_change_pct": combined_played_together_pct,
            "combined_most_concurrent_players": combined_most_concurrent,
            "combined_most_concurrent_list": combined_most_concurrent_list,
            "combined_most_concurrent_list_change_abs": combined_concurrent_abs,
            "combined_most_concurrent_list_change_pct": combined_concurrent_pct,
            "combined_most_played_list_capped": combined_most_played_list_capped,
            "combined_most_played_together_list_capped": combined_most_played_together_list_capped,
            "combined_most_concurrent_list_capped": combined_most_concurrent_list_capped,
            "discord_active_events": active_events,
            "discord_non_active_events": non_active_events,
            "birthdays": birthdays,
            "link": link
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

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['timesteps_to_human_readable'] = timesteps_to_human_readable
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
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

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)

        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['timesteps_to_human_readable'] = timesteps_to_human_readable
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
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

        env = Environment(loader=FileSystemLoader('newsletter/templates'),
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