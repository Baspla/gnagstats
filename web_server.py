from flask import Flask, render_template_string
import threading

def create_app(database):
    from flask import render_template
    import os
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))

    @app.route("/stats")
    def stats():
        import time
        now = int(time.time())
        week_ago = now - 7*24*3600
        # Steam: meistgespielte Spiele
        steam_most_played = database.get_steam_most_played_games(week_ago, now)
        # Discord: aktivste Voice-Channels
        discord_busiest_channels = database.get_discord_busiest_voice_channels(week_ago, now)
        return render_template("stats_overview.jinja2",
                              steam_most_played=steam_most_played,
                              discord_busiest_channels=discord_busiest_channels)

    return app

def run_webserver(database, host="127.0.0.1", port=5000):
    app = create_app(database)
    threading.Thread(target=app.run, kwargs={"host": host, "port": port, "debug": False}, daemon=True).start()
