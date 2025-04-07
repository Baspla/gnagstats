import logging
import os
DB_PATH : str                   = os.getenv("DB_PATH","collected_data.db")
DISCORD_API_TOKEN : str         = os.getenv("DISCORD_API_TOKEN", "")
DISCORD_WEBHOOK_URL : str         = os.getenv("DISCORD_WEBHOOK_URL", "")
STEAM_API_KEY : str             = os.getenv("STEAM_API_KEY", "")
DATA_COLLECTION_INTERVAL : int  = int(os.getenv("DATA_COLLECTION_INTERVAL", 5))  # Intervall in Minuten für die Datensammlung
DISCORD_STATS_ENABLED : bool    = os.getenv("DISCORD_STATS_ENABLED", "True").lower() == "true"  # Aktiviert/Deaktiviert das Sammeln von Discord-Daten
STEAM_STATS_ENABLED : bool      = os.getenv("STEAM_STATS_ENABLED", "True").lower() == "true"  # Aktiviert/Deaktiviert das Sammeln von Steam-Daten
LOGGING_LEVEL : str             = os.getenv("LOGGING_LEVEL", "INFO").upper()  # Setzt das Logging-Level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
WEB_SERVER_PORT : int           = int(os.getenv("WEB_SERVER_PORT", 8080))  # Port für den Webserver
JSON_DATA_PATH : str         = os.getenv("JSON_DATA_PATH", "data.json")  # Pfad zur JSON-Datei für die Datenspeicherung
DEBUG_MODE : bool             = os.getenv("DEBUG_MODE", "False").lower() == "true"  # Aktiviert/Deaktiviert den Debug-Modus

if DISCORD_API_TOKEN == "":
    logging.warning("DISCORD_API_TOKEN is empty. Discord stats collection will be disabled.")
    DISCORD_STATS_ENABLED = False

if STEAM_API_KEY == "":
    logging.warning("STEAM_API_KEY is empty. Steam stats collection will be disabled.")
    STEAM_STATS_ENABLED = False