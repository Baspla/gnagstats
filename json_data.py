# This class loads data from a JSON file and provides methods to access it.
# This data is for example: discord guild ids, people with steam id and discord id and birthday, etc.

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

from config import JSON_DATA_PATH


def load_json_data(file_path: str) -> Dict[str, Any]:
    """
    Load JSON data from a file.

    :param file_path: Path to the JSON file.
    :return: Parsed JSON data as a dictionary.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            logging.info(f"Loaded JSON data from {file_path}.")
            return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {file_path}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error while loading JSON data from {file_path}: {e}")
        return {}
    finally:
        logging.debug(f"Finished loading JSON data from {file_path}.")

def get_guild_ids(data: Dict[str, Any]) -> List[str]:
    """
    Get guild IDs from the JSON data.

    :param data: Parsed JSON data.
    :return: List of guild IDs.
    """
    guild_ids = data.get("guildIds", [])
    logging.debug(f"Extracted guild IDs: {guild_ids}")
    return guild_ids

def get_user_steam_ids(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get user Steam IDs from the JSON data.

    :param data: Parsed JSON data.
    :return: List of dictionaries containing Steam IDs.
    """
    people = data.get("people", [])
    steam_ids = [person["steamId"] for person in people if "steamId" in person]
    logging.debug(f"Extracted user Steam IDs: {steam_ids}")
    return steam_ids

def get_user_discord_ids(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get user Discord IDs from the JSON data.

    :param data: Parsed JSON data.
    :return: List of dictionaries containing user Discord IDs.
    """
    people = data.get("people", [])
    discord_ids = [person["discordId"] for person in people if "discordId" in person]
    logging.debug(f"Extracted user Discord IDs: {discord_ids}")
    return discord_ids

def get_user_birthdays(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get user birthdays from the JSON data.

    :param data: Parsed JSON data.
    :return: List of dictionaries containing user IDs and birthdays.
    """
    people = data.get("people", [])
    birthdays = [{"name": person["name"], "birthday": datetime.strptime(person["birthday"], "%d-%m-%Y")} for person in people if "birthday" in person]
    logging.debug(f"Extracted user birthdays: {birthdays}")
    return birthdays

def get_user_data(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get user data from the JSON data.

    :param data: Parsed JSON data.
    :return: List of dictionaries containing user IDs and other data.
    """
    people = data.get("people", [])
    user_data = [{"id": person["id"], "name": person["name"]} for person in people]
    logging.debug(f"Extracted user data: {user_data}")
    return user_data

def get_data():
    """
    Get all data from the JSON file.

    :return: Dictionary containing all data.
    """
    json_data = load_json_data(JSON_DATA_PATH)
    return {
        "guild_ids": get_guild_ids(json_data),
        "user_steam_ids": get_user_steam_ids(json_data),
        "user_discord_ids": get_user_discord_ids(json_data),
        "user_birthdays": get_user_birthdays(json_data),
        "user_data": get_user_data(json_data)
    }
