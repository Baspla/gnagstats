from streamlit.web import cli as stcli
from streamlit import runtime
import sys
import streamlit as st
import sys
import os
import datetime
from streamlit_autorefresh import st_autorefresh

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datavis.data_provider import DataProvider
from data_storage.db import Database
from datavis.plots import build_figures

st.set_page_config(layout="wide", page_title="Gnag Stats Dashboard")

@st.cache_resource(ttl=300)
def get_global_data():
    db = Database()
    provider = DataProvider(db)
    
    figures = build_figures(provider)
    
    return figures, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    try:
        st_autorefresh(interval=5 * 60 * 1000, key="data_refresher")
        
        figures, last_updated = get_global_data()
        
        st.title(f"Gnag Stats Dashboard")
        
        voice_fig = figures.get('voice')
        game_fig = figures.get('game')


        st.header("Yap Yap Yap")
        if voice_fig:
            st.plotly_chart(voice_fig, use_container_width=True)
        else:
            st.warning("No voice activity data available.")

        st.header("The Gaming")
        if game_fig:
            st.plotly_chart(game_fig, use_container_width=True)
        else:
            st.warning("No game activity data available.")
            
    except Exception as e:
        st.error(f"An error occurred: {e}")
        import traceback
        st.code(traceback.format_exc())
        
if __name__ == "__main__":
    main()
