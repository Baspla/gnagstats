from dash import Input, Output
import plotly.express as px
import pandas as pd

from webserver.data_provider import DataProvider, Params

def register_callbacks(app, data_provider: DataProvider):
	@app.callback(
		Output('graph-playtime-pie', 'figure'),
		[Input('reload-btn', 'n_clicks'),
		 Input('timerange-picker', 'start_date'),
		 Input('timerange-picker', 'end_date')]
	)
	def update_playtime_pie(n_clicks, start_date, end_date):
		# Konvertiere die Datumswerte in UNIX-Timestamps (Sekunden)
		import pandas as pd
		start_ts = int(pd.Timestamp(start_date).timestamp()) if start_date else None
		end_ts = int(pd.Timestamp(end_date).timestamp()+pd.Timedelta(days=1)) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_combined = bundle["combined"]
		if df_combined.empty:
			return px.pie(names=['Keine Daten'], values=[1], title='Spielzeit pro Spiel')
		playtime = df_combined.groupby('game_name')['minutes_per_snapshot'].sum().reset_index()
		fig = px.pie(playtime, names='game_name', values='minutes_per_snapshot', title='Spielzeit pro Spiel')
		return fig
	
	@app.callback(
		Output('graph-24h-voice-activity', 'figure'),
		[Input('reload-btn', 'n_clicks')]
	)
	def update_voice_activity(n_clicks):
		# Gantt-Diagramm der Sprachaktivität über 24 Stunden mit df_voice_intervals
		params = Params(start=int(pd.Timestamp.now().timestamp()) - 24 * 60 * 60, end=int(pd.Timestamp.now().timestamp()))
		bundle = data_provider.load_all(params)
		df_voice_intervals = bundle["voice_intervals"]
		if df_voice_intervals.empty:
			return px.timeline(pd.DataFrame(columns=['user_name', 'start_dt', 'end_dt', 'channel_name']), x_start='start_dt', x_end='end_dt', y='user_name', color='channel_name', title='Sprachaktivität der letzten 24 Stunden')
		df_voice_intervals['start_dt'] = pd.to_datetime(df_voice_intervals['start_ts'], unit='s')
		df_voice_intervals['end_dt'] = pd.to_datetime(df_voice_intervals['end_ts'], unit='s')
		fig = px.timeline(df_voice_intervals, x_start='start_dt', x_end='end_dt', y='user_name', color='channel_name', title='Sprachaktivität der letzten 24 Stunden')
		fig.update_yaxes(autorange="reversed") # Damit die neuesten Einträge oben sind
		return fig
