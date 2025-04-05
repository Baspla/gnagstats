import discord
import logging

class DiscordClient(discord.Client):
    async def on_ready(self):
        logging.info(f"Logged in as {self.user}")