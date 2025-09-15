from dash import Input, Output
import plotly.express as px
import pandas as pd

from data_storage.db import minutes_to_human_readable
from webserver.data_provider import DataProvider, Params

def register_callbacks(app, data_provider: DataProvider):
	@app.callback(
		Output('graph-playtime-pie', 'figure'),
		[Input('reload-btn', 'n_clicks'),
		 Input('timerange-picker', 'start_date'),
		 Input('timerange-picker', 'end_date')]
	)
	def update_playtime_pie(n_clicks, start_date, end_date):

		start_ts = int(pd.Timestamp(start_date).timestamp()) if start_date else None
		end_ts = int((pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp()) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_combined = bundle["combined"]
		if df_combined.empty:
			return px.pie(names=['Keine Daten'], values=[1], title='Spielzeit pro Spiel')

		playtime = df_combined.groupby('game_name')['minutes_per_snapshot'].sum().reset_index()
		playtime = playtime.sort_values('minutes_per_snapshot', ascending=False).head(20)
		playtime['spielzeit'] = playtime['minutes_per_snapshot'].apply(minutes_to_human_readable)
		fig = px.pie(
			playtime,
			names='game_name',
			values='minutes_per_snapshot',
			title='Spielzeit pro Spiel (Top 20)',
			labels={
				'game_name': 'Spiel',
				'spielzeit': 'Spielzeit'
			},
			hover_data={'game_name': True, 'spielzeit': True}
		)
		fig.update_traces(text=playtime['spielzeit'], textinfo='label+percent')
		return fig
	
	@app.callback(
		Output('graph-24h-voice-activity', 'figure'),
		[Input('reload-btn', 'n_clicks')]
	)
	def update_voice_activity(n_clicks):
		params = Params(start=int(pd.Timestamp.now().timestamp()) - 24 * 60 * 60, end=int(pd.Timestamp.now().timestamp()))
		bundle = data_provider.load_all(params)
		df_voice_intervals = bundle["voice_intervals"]
		if df_voice_intervals.empty:
			return px.timeline(
				pd.DataFrame(columns=['user_name', 'start_dt', 'end_dt', 'channel_name', 'duration_minutes']),
				x_start='start_dt',
				x_end='end_dt',
				y='user_name',
				color='channel_name',
				title='Sprachaktivität der letzten 24 Stunden',
				labels={
					'user_name': 'Benutzer',
					'start_dt': 'Startzeit',
					'end_dt': 'Endzeit',
					'channel_name': 'Sprachkanal',
					'duration_minutes': 'Dauer'
				},
				hover_data={
					'user_name': True,
					'channel_name': True,
					'start_dt': True,
					'end_dt': True,
					'duration_minutes': True
				}
			)
		df_voice_intervals['start_dt'] = pd.to_datetime(df_voice_intervals['start_ts'], unit='s')
		df_voice_intervals['end_dt'] = pd.to_datetime(df_voice_intervals['end_ts'], unit='s')
		df_voice_intervals['dauer'] = df_voice_intervals['duration_minutes'].apply(minutes_to_human_readable)
		fig = px.timeline(
			df_voice_intervals,
			x_start='start_dt',
			x_end='end_dt',
			y='user_name',
			color='channel_name',
			title='Sprachaktivität der letzten 24 Stunden',
			labels={
				'user_name': 'Benutzer',
				'start_dt': 'Startzeit',
				'end_dt': 'Endzeit',
				'channel_name': 'Sprachkanal',
				'dauer': 'Dauer'
			},
			hover_data={
				'user_name': True,
				'channel_name': True,
				'start_dt': True,
				'end_dt': True,
				'dauer': True
			}
		)
		fig.update_yaxes(title_text='Benutzer', autorange="reversed")
		fig.update_xaxes(title_text='Zeit')
		fig.update_layout(legend_title_text='Sprachkanal')
		return fig


	@app.callback(
		Output('graph-24h-game-activity', 'figure'),
		[Input('reload-btn', 'n_clicks')]
	)
	def update_game_activity(n_clicks):
		params = Params(start=int(pd.Timestamp.now().timestamp()) - 24 * 60 * 60, end=int(pd.Timestamp.now().timestamp()))
		bundle = data_provider.load_all(params)
		df_game_intervals = bundle["game_intervals"]
		if df_game_intervals.empty:
			return px.timeline(
				pd.DataFrame(columns=['user_name', 'start_dt', 'end_dt', 'game_name', 'duration_minutes']),
				x_start='start_dt',
				x_end='end_dt',
				y='user_name',
				color='game_name',
				title='Spielaktivität der letzten 24 Stunden',
				labels={
					'user_name': 'Benutzer',
					'start_dt': 'Startzeit',
					'end_dt': 'Endzeit',
					'game_name': 'Spiel',
					'duration_minutes': 'Dauer'
				},
				hover_data={
					'user_name': True,
					'game_name': True,
					'start_dt': True,
					'end_dt': True,
					'duration_minutes': True
				}
			)
		df_game_intervals['start_dt'] = pd.to_datetime(df_game_intervals['start_ts'], unit='s')
		df_game_intervals['end_dt'] = pd.to_datetime(df_game_intervals['end_ts'], unit='s')
		df_game_intervals['dauer'] = df_game_intervals['duration_minutes'].apply(minutes_to_human_readable)
		fig = px.timeline(
			df_game_intervals,
			x_start='start_dt',
			x_end='end_dt',
			y='user_name',
			color='game_name',
			title='Spielaktivität der letzten 24 Stunden',
			labels={
				'user_name': 'Benutzer',
				'start_dt': 'Startzeit',
				'end_dt': 'Endzeit',
				'game_name': 'Spiel',
				'dauer': 'Dauer'
			},
			hover_data={
				'user_name': True,
				'game_name': True,
				'start_dt': True,
				'end_dt': True,
				'dauer': True
			}
		)
		fig.update_yaxes(title_text='Benutzer', autorange="reversed")
		fig.update_xaxes(title_text='Zeit')
		fig.update_layout(legend_title_text='Spiel')
		return fig