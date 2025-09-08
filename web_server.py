import logging
from flask import Flask, render_template_string
import threading

def create_app(database):
    from flask import render_template
    import os
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))

    @app.route("/")
    def stats():
        import time
        from jinja2 import Environment, FileSystemLoader
        from newsletter_creator import datetime_to_timestamp, timesteps_to_human_readable, seconds_to_human_readable
        now = int(time.time())
        week_ago = now - 7*24*3600
        steam_most_played = database.get_steam_most_played_games(week_ago, now)
        discord_busiest_channels = database.get_discord_busiest_voice_channels(week_ago, now)

        env = Environment(loader=FileSystemLoader('templates'),
                          autoescape=True,
                          trim_blocks=True,
                          lstrip_blocks=True)
        env.filters['datetime_to_timestamp'] = datetime_to_timestamp
        env.filters['seconds_to_human_readable'] = seconds_to_human_readable
        template = env.get_template('stats_overview.jinja2')

        rendered = template.render(
            steam_most_played=steam_most_played,
            discord_busiest_channels=discord_busiest_channels
        )
        return rendered

    return app

def run_webserver(database, host="127.0.0.1", port=5000):
    app = create_app(database)
    logging.info(f"Starting production web server at http://{host}:{port}/ using Waitress.")
    from waitress import serve
    threading.Thread(target=serve, args=(app,), kwargs={"host": host, "port": port}, daemon=True).start()
