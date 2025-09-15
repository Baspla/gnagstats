from dash import Input, Output
import plotly.express as px

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
		end_ts = int(pd.Timestamp(end_date).timestamp()) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_combined = bundle[2]
		if df_combined.empty:
			return px.pie(names=['Keine Daten'], values=[1], title='Spielzeit pro Spiel')
		playtime = df_combined.groupby('game_name')['minutes_per_snapshot'].sum().reset_index()
		fig = px.pie(playtime, names='game_name', values='minutes_per_snapshot', title='Spielzeit pro Spiel')
		return fig
