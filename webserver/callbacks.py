from dash import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import Counter

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
		df_games = bundle["games"]
		if df_games.empty:
			return px.pie(names=['Keine Daten'], values=[1], title='Spielzeit pro Spiel')

		playtime = df_games.groupby('game_name')['minutes_per_snapshot'].sum().reset_index()
		playtime = playtime.sort_values('minutes_per_snapshot', ascending=False)
		top_n = 10
		top_games = playtime.head(top_n)
		other_games = playtime.iloc[top_n:]
		if not other_games.empty:
			others_row = pd.DataFrame({
				'game_name': ['Others'],
				'minutes_per_snapshot': [other_games['minutes_per_snapshot'].sum()]
			})
			playtime_final = pd.concat([top_games, others_row], ignore_index=True)
		else:
			playtime_final = top_games

		playtime_final['spielzeit'] = playtime_final['minutes_per_snapshot'].apply(minutes_to_human_readable)
		fig = px.pie(
			playtime_final,
			names='game_name',
			values='minutes_per_snapshot',
			title='Spielzeit pro Spiel',
			labels={
				'game_name': 'Spiel',
				'spielzeit': 'Spielzeit'
			},
			hover_name='game_name'
		)
		fig.update_traces(
			text=playtime_final['spielzeit'], 
			textinfo='label+percent',
			hovertemplate='<b>%{label}</b><br>Spielzeit: %{customdata}<br>Prozent: %{percent}<extra></extra>',
			customdata=playtime_final['spielzeit']
		)
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
	
	@app.callback(
		Output('network-voice-activity', 'figure'),
		[Input('reload-btn', 'n_clicks'),
		 Input('timerange-picker', 'start_date'),
		 Input('timerange-picker', 'end_date')]
	)
	def update_voice_network(n_clicks, start_date, end_date):
		start_ts = int(pd.Timestamp(start_date).timestamp()) if start_date else None
		end_ts = int((pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp()) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_voice = bundle["voice"]
		
		if df_voice.empty or df_voice["user_name"].nunique() < 2:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="Voice User Network (keine Daten)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Berechne gemeinsame Voice-Zeit zwischen Usern
		pair_minutes = Counter()
		for (timestamp, channel), group in df_voice.groupby(["timestamp", "channel_name"], sort=False):
			users = sorted(group["user_name"].dropna().unique().tolist())
			if len(users) < 2:
				continue
			minutes_slot = group["minutes_per_snapshot"].median()
			for u1, u2 in combinations(users, 2):
				if u1 == u2:
					continue
				key = tuple(sorted((u1, u2)))
				pair_minutes[key] += minutes_slot
		
		if not pair_minutes:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="Voice User Network (keine gemeinsamen Zeiten)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Erstelle Netzwerk-Graph
		G = nx.Graph()
		user_total_minutes = df_voice.groupby("user_name")["minutes_per_snapshot"].sum()
		
		for user, minutes in user_total_minutes.items():
			G.add_node(user, total_hours=minutes / 60.0)
		
		for (u1, u2), minutes in pair_minutes.items():
			if minutes >= 30:  # Mindestens 30 Minuten gemeinsame Zeit
				G.add_edge(u1, u2, weight=minutes / 60.0)
		
		if G.number_of_edges() == 0:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="Voice User Network (keine Verbindungen)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Layout berechnen
		pos = nx.spring_layout(G, k=0.6, iterations=200, seed=42, weight="weight")
		
		# Node-Größen basierend auf gesamter Voice-Zeit
		hours_vals = [G.nodes[n].get("total_hours", 0.0) for n in G.nodes()]
		h_min, h_max = min(hours_vals), max(hours_vals)
		
		def scale_node_size(hours):
			if h_max > h_min:
				return 15 + 45 * ((hours - h_min) / (h_max - h_min))
			return 30
		
		node_sizes = [scale_node_size(G.nodes[n].get("total_hours", 0.0)) for n in G.nodes()]
		
		# Edge-Breiten basierend auf gemeinsamer Zeit
		edge_weights = [d.get("weight", 0.0) for _, _, d in G.edges(data=True)]
		ew_min, ew_max = min(edge_weights), max(edge_weights)
		
		def scale_edge_width(weight):
			if ew_max > ew_min:
				return 0.5 + 7.5 * ((weight - ew_min) / (ew_max - ew_min))
			return 4
		
		# Erstelle Edge-Traces
		edge_traces = []
		for u, v, d in G.edges(data=True):
			weight = d.get("weight", 0.0)
			width = scale_edge_width(weight)
			x0, y0 = pos[u]
			x1, y1 = pos[v]
			edge_traces.append(go.Scatter(
				x=[x0, x1], y=[y0, y1],
				mode="lines",
				line=dict(width=width, color="rgba(120,120,120,0.5)"),
				hoverinfo="text",
				showlegend=False
			))
		
		# Erstelle Node-Trace
		users = list(G.nodes())
		node_x = [pos[u][0] for u in users]
		node_y = [pos[u][1] for u in users]
		node_hover = [f"{u}<br>Gesamt Voice-Zeit: {G.nodes[u].get('total_hours', 0):.1f}h" for u in users]
		
		# Farbpalette für Benutzer
		colors = px.colors.qualitative.Set3[:len(users)]
		if len(users) > len(colors):
			colors = colors * ((len(users) // len(colors)) + 1)
		
		node_trace = go.Scatter(
			x=node_x, y=node_y,
			mode="markers+text",
			hoverinfo="text",
			hovertext=node_hover,
			text=users,
			textposition="top center",
			marker=dict(
				size=node_sizes,
				color=colors[:len(users)],
				line=dict(width=1.5, color="#1f1f1f")
			),
			showlegend=False
		)
		
		# Erstelle Figure
		fig = go.Figure(
			data=[*edge_traces, node_trace],
			layout=go.Layout(
				title="Voice User Network<br><span style='font-size:12px'>Kantenbreite = gemeinsame Voice-Zeit, Node-Größe = gesamte Voice-Zeit</span>",
				title_x=0.5,
				margin=dict(l=10, r=10, t=60, b=10),
				hovermode="closest",
				showlegend=False,
				xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
				yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
			)
		)
		
		return fig
	
	@app.callback(
		Output('network-game-activity', 'figure'),
		[Input('reload-btn', 'n_clicks'),
		 Input('timerange-picker', 'start_date'),
		 Input('timerange-picker', 'end_date')]
	)
	def update_game_network(n_clicks, start_date, end_date):
		start_ts = int(pd.Timestamp(start_date).timestamp()) if start_date else None
		end_ts = int((pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp()) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_games = bundle["games"]
		
		if df_games.empty:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Network (keine Daten)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Berechne Spielzeit pro User-Game-Paar
		user_game_hours = df_games.groupby(["user_name", "game_name"])["minutes_per_snapshot"].sum().reset_index()
		user_game_hours["hours"] = user_game_hours["minutes_per_snapshot"] / 60.0
		
		# Filtere Spiele mit mindestens 20 Stunden Gesamtspielzeit
		game_totals = user_game_hours.groupby("game_name")["hours"].sum()
		keep_games = set(game_totals[game_totals >= 20.0].index)
		
		if not keep_games:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Network (keine Spiele mit genug Spielzeit)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		filtered_data = user_game_hours[user_game_hours["game_name"].isin(keep_games)]
		
		# Erstelle bipartiten Graph
		G = nx.Graph()
		
		# Füge Game-Nodes hinzu
		for game in keep_games:
			total_hours = game_totals[game]
			G.add_node(("game", game), kind="game", label=game, total_hours=total_hours)
		
		# Füge User-Nodes hinzu
		users = filtered_data["user_name"].unique()
		for user in users:
			G.add_node(("user", user), kind="user", label=user)
		
		# Füge Edges hinzu
		for _, row in filtered_data.iterrows():
			G.add_edge(("user", row["user_name"]), ("game", row["game_name"]), weight=row["hours"])
		
		if G.number_of_edges() == 0:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Network (keine Verbindungen)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Layout berechnen
		pos = nx.spring_layout(G, weight="weight", k=None, iterations=200, seed=42)
		
		# Edge-Traces
		edge_weights = [d.get("weight", 1.0) for _, _, d in G.edges(data=True)]
		w_max = max(edge_weights) if edge_weights else 1.0
		
		edge_traces = []
		for u, v, d in G.edges(data=True):
			weight = d.get("weight", 1.0)
			width = 1.0 + 7.0 * (weight / w_max) if w_max > 0 else 4.0
			x0, y0 = pos[u]
			x1, y1 = pos[v]
			edge_traces.append(go.Scatter(
				x=[x0, x1], y=[y0, y1],
				mode="lines",
				line=dict(width=width, color="#aaaaaa"),
				opacity=0.55,
				showlegend=False,
				hoverinfo="skip"  # Remove any hovertext from the edges
			))
		
		# Node-Traces (getrennt für User und Games)
		user_x, user_y, user_text = [], [], []
		game_x, game_y, game_text, game_sizes = [], [], [], []
		
		# Game-Node-Größen basierend auf Gesamtspielzeit
		g_hours_vals = [G.nodes[n].get("total_hours", 0) for n, attrs in G.nodes(data=True) if attrs.get("kind") == "game"]
		g_min, g_max = (min(g_hours_vals), max(g_hours_vals)) if g_hours_vals else (0, 1)
		
		for n, attrs in G.nodes(data=True):
			x, y = pos[n]
			if attrs.get("kind") == "user":
				user_x.append(x)
				user_y.append(y)
				user_text.append(attrs.get("label", ""))
			else:
				game_x.append(x)
				game_y.append(y)
				game_text.append(f"{attrs.get('label','')}<br>{attrs.get('total_hours',0):.1f}h total")
				hours = attrs.get("total_hours", 0.0)
				if g_max > g_min:
					size = 12 + 24 * ((hours - g_min) / (g_max - g_min))
				else:
					size = 24
				game_sizes.append(size)
		
		user_trace = go.Scatter(
			x=user_x, y=user_y,
			mode="markers",
			hoverinfo="text",
			text=user_text,
			marker=dict(size=12, color="#1f77b4", line=dict(width=1, color="#ffffff")),
			name="User"
		)
		
		game_trace = go.Scatter(
			x=game_x, y=game_y,
			mode="markers",
			hoverinfo="text",
			text=game_text,
			marker=dict(size=game_sizes, color="#ff7f0e", line=dict(width=1, color="#333333")),
			name="Game"
		)
		
		# Erstelle Figure
		fig = go.Figure(
			data=[*edge_traces, user_trace, game_trace],
			layout=go.Layout(
				title="User-Game Network<br><span style='font-size:12px'>Kantenbreite = Spielzeit, Game-Node-Größe = Gesamtspielzeit</span>",
				title_x=0.5,
				showlegend=True,
				legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
				hovermode="closest",
				margin=dict(l=10, r=10, t=60, b=10),
				xaxis=dict(visible=False),
				yaxis=dict(visible=False)
			)
		)
		
		return fig
	
	@app.callback(
		Output('heatmap-user-game-activity', 'figure'),
		[Input('reload-btn', 'n_clicks'),
		 Input('timerange-picker', 'start_date'),
		 Input('timerange-picker', 'end_date')]
	)
	def update_user_game_heatmap(n_clicks, start_date, end_date):
		start_ts = int(pd.Timestamp(start_date).timestamp()) if start_date else None
		end_ts = int((pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp()) if end_date else None
		params = Params(start=start_ts, end=end_ts)
		bundle = data_provider.load_all(params)
		df_games = bundle["games"]
		
		if df_games.empty:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Heatmap (keine Daten)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		# Berechne Spielzeit pro User-Game-Paar
		game_totals = df_games.groupby("game_name")["minutes_per_snapshot"].sum().sort_values(ascending=False)
		top_games = game_totals.head(20).index.tolist()
		user_game_hours = df_games[df_games["game_name"].isin(top_games)].groupby(["user_name", "game_name"])["minutes_per_snapshot"].sum().reset_index()
		user_game_hours["hours"] = user_game_hours["minutes_per_snapshot"] / 60.0
		
		if user_game_hours.empty:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Heatmap (keine Verbindungen)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		pivot_table = user_game_hours.pivot(index="user_name", columns="game_name", values="hours").fillna(0)
		
		if pivot_table.empty:
			return go.Figure(layout=dict(
				xaxis=dict(visible=False), 
				yaxis=dict(visible=False), 
				title="User-Game Heatmap (keine Verbindungen)",
				margin=dict(l=0, r=0, t=30, b=0)
			))
		
		fig = px.imshow(
			pivot_table,
			labels=dict(x="Spiel", y="Benutzer", color="Stunden"),
			x=pivot_table.columns,
			y=pivot_table.index,
			color_continuous_scale='Viridis',
			title="User-Game Aktivitäts-Heatmap"
		)
		
		fig.update_xaxes(tickangle=-45)
		fig.update_layout(margin=dict(l=40, r=40, t=60, b=100))
		return fig