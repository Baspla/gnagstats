import asyncio

import discord
import logging

from config import DISCORD_API_TOKEN

class DiscordClient(discord.Client):
    async def on_ready(self):
        logging.info(f"Logged in as {self.user}")

    async def on_message(self, message):
        if message.author == self.user:
            return
        logging.info(f"Message from {message.author}: {message.content}")