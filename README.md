# gnagstats

gnagstats is a Python project for analyzing and visualizing gaming statistics.

## Features

- Import and process game data
- Generate insightful statistics
- Visualize results with charts and graphs

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py
```

### Serverseitiges Refresh der Dashboard-Grafiken

Die im Web-Dashboard angezeigten Plotly-Grafiken werden serverseitig periodisch neu berechnet.
Standard: alle 10 Minuten. Das Intervall kann über die Umgebungsvariable `WEB_FIGURE_REFRESH_MINUTES`
oder in `config.py` geändert werden.

Beispiel (PowerShell):
```powershell
$env:WEB_FIGURE_REFRESH_MINUTES=10; python main.py
```

Der Refresh erfolgt vollständig serverseitig; Browser-Clients müssen die Seite nur neu laden,
um die aktualisierten Daten zu sehen. Ein expliziter Client-Polling-Mechanismus ist nicht nötig.

### JSON-API für zuletzt gespielte Spiele

Der Prozess startet zusätzlich einen kleinen Flask-Webserver auf `WEB_SERVER_PORT` (Standard: `8080`).
Der Endpunkt `GET /api/recent` liefert ein JSON-Objekt mit den zuletzt in den letzten 30 Tagen
gespielten Spielen pro Nutzer, zum Beispiel:

```json
{"userid":["Game A","Game B"],"userid2":["Game C"]}
```

Wenn du den Port ändern willst, setze `WEB_SERVER_PORT` in der Umgebung.

## Useful Commands

### Update Requirements

```bash
pip freeze > requirements.txt
```

## Contributing

Contributions are welcome! Please open issues or submit pull requests.