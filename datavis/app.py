from streamlit.web import cli as stcli
from streamlit import runtime
import sys
import streamlit as st
import sys
import os
import datetime
import time
from streamlit_autorefresh import st_autorefresh

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datavis.data_provider import DataProvider
from data_storage.db import Database
from datavis.plots import build_figures, build_calendar_heatmap

st.set_page_config(layout="wide", page_title="Gnag Stats Dashboard")

@st.cache_data(ttl=300)
def get_dashboard_data():
    db = Database()
    provider = DataProvider(db)

    figures = build_figures(provider)
    user_stats = provider.get_user_stats_7d()
    return figures, user_stats


@st.cache_data(ttl=3600)
def get_alltime_stats():
    """Cache the expensive historical aggregation independently.

    All-time values change much less often than the live dashboard data, so
    they should not force a full historical scan every five-minute refresh.
    """
    provider = DataProvider(Database())
    return provider.get_user_stats_alltime()


def load_dashboard_data_with_timing():
    """Load both datasets and measure how long the loading call takes."""
    start = time.perf_counter()
    figures, user_stats = get_dashboard_data()
    alltime_stats = get_alltime_stats()
    elapsed = time.perf_counter() - start

    return figures, user_stats, alltime_stats, elapsed

def main():
    try:
        st_autorefresh(interval=5 * 60 * 1000, key="data_refresher")
        
        (
            figures,
            user_stats,
            alltime_stats,
            load_seconds,
        ) = load_dashboard_data_with_timing()
        last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        st.title(f"Gnag Stats Dashboard")
        st.caption(f"Innerhalb von {load_seconds:.2f} Sekunden geladen. Letzte Aktualisierung: {last_updated}")
        
        voice_fig = figures.get('voice')
        game_fig = figures.get('game')


        st.header("Yap Yap Yap")
        if voice_fig:
            st.plotly_chart(voice_fig, width="stretch")
        else:
            st.warning("No voice activity data available.")

        st.header("The Gaming")
        if game_fig:
            st.plotly_chart(game_fig, width="stretch")
        else:
            st.warning("No game activity data available.")

        # ──── Benutzerspezifische Statistiken (Tabs) ────────────────
        st.header("Leude Leude Leude")
        if user_stats:
            user_names = list(user_stats.keys())
            tabs = st.tabs(user_names)
            for idx, name in enumerate(user_names):
                with tabs[idx]:
                    st.subheader(f"{name}s Stats")
                    stats = user_stats[name]
                    k1, k2 = st.columns(2)
                    with k1:
                        st.metric("Spielzeit (7 Tage)", f"{stats['game_hours']:.2f} Stunden")
                    with k2:
                        st.metric("Sprechzeit (7 Tage)", f"{stats['voice_hours']:.2f} Stunden")
                        
                    year_now = datetime.datetime.now().year
                    game_fig = build_calendar_heatmap(
                        stats.get("daily_game_minutes", {}),
                        year_now,
                        "Spielzeit (täglich)",
                        "Greens",
                        stats.get("daily_top_games", {}),
                    )
                    st.plotly_chart(game_fig, use_container_width=True)
                    voice_fig = build_calendar_heatmap(
                        stats.get("daily_voice_minutes", {}),
                        year_now,
                        "Sprechzeit (täglich)",
                        "Blues",
                    )
                    st.plotly_chart(voice_fig, use_container_width=True)

                    # ──── All‑time statistics ───────────────────────────────────
                    st.markdown("##### Some of the stats of all time")
                    at = alltime_stats.get(name, {})
                    ca, cb, cc = st.columns(3)
                    with ca:
                        st.metric("Sprechzeit (gesamt)", f"{at.get('voice_hours', 0):.2f} Stunden")
                        st.metric("Spielzeit (gesamt)", f"{at.get('game_hours', 0):.2f} Stunden")
                    with cb:
                        st.metric("Zeit gemutet",          f"{at.get('mute_hours', 0):.2f} Stunden")
                        st.metric("Zeit deafened",           f"{at.get('deaf_hours', 0):.2f} Stunden")
                    with cc:
                        st.metric("Zeit gestreamt",         f"{at.get('stream_hours', 0):.2f} Stunden")
                        st.metric("Zeit mit Kamera",          f"{at.get('video_hours', 0):.2f} Stunden")

                    
                    st.markdown("##### Die zuletzt gespielten Spiele")
                    games = stats["games"]
                    if games:
                        now_ts = int(datetime.datetime.now().timestamp())
                        table_rows = []
                        for g in games:
                            secs_ago = now_ts - g["last_played_ts"]
                            hrs_ago = secs_ago / 3600
                            if hrs_ago < 1:
                                ago_label = "<1 Std."
                            elif hrs_ago < 24:
                                ago_label = f"vor {int(hrs_ago)} Std."
                            else:
                                days_ago = hrs_ago / 24
                                ago_label = f"vor {int(days_ago)} Tagen"
                            table_rows.append({
                                "Spiel": g["game_name"],
                                "Zuletzt gespielt": ago_label,
                            })
                        st.dataframe(table_rows, use_container_width=True, hide_index=True)
                    else:
                        st.info("Keine Spiele gespielt.")
        else:
            st.info("Keine Benutzerstatistiken verfügbar.")
            
    except Exception as e:
        st.error(f"An error occurred: {e}")
        import traceback
        st.code(traceback.format_exc())
        
if __name__ == "__main__":
    main()
