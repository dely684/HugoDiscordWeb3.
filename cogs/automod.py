import discord
from discord.ext import commands
import re
import datetime
from database import get_server_config, add_log, add_warn

class AutoMod(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.link_regex = re.compile(r"https?://\S+|www\.\S+")
        self.message_cache = {} # {user_id: [timestamps]}

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot or not message.guild:
            return

        # Skip if user is staff (manage_messages permission)
        if message.author.guild_permissions.manage_messages:
            return

        config = await get_server_config(message.guild.id)
        if not config:
            return

        content = message.content.lower()
        deleted = False

        # 1. BANNED WORDS
        if config.get('automod_words'):
            banned_words = [w.strip() for w in config['automod_words'].split(",") if w.strip()]
            for word in banned_words:
                if word.lower() in content:
                    await self.punish(message, "Yasaklı Kelime", word)
                    deleted = True
                    break

        # 2. ANTI-LINK
        if not deleted and config.get('automod_links'):
            if self.link_regex.search(content):
                await self.punish(message, "Reklam/Link Engeli", "Link tespiti")
                deleted = True

        # 3. ANTI-SPAM
        if not deleted and config.get('automod_spam'):
            user_id = message.author.id
            now = datetime.datetime.now()
            
            if user_id not in self.message_cache:
                self.message_cache[user_id] = []
            
            # Keep only last 10 seconds of messages
            self.message_cache[user_id] = [t for t in self.message_cache[user_id] if (now - t).total_seconds() < 10]
            self.message_cache[user_id].append(now)
            
            if len(self.message_cache[user_id]) > 5: # More than 5 messages in 10 seconds
                await self.punish(message, "Spam Engeli", "Hızlı mesaj gönderimi")
                deleted = True

    async def punish(self, message, reason, detail):
        try:
            await message.delete()
        except:
            pass
            
        # Log to DB
        await add_log("automod-log", message.author.id, message.author.name, f"{reason}: {detail}", message.guild.id)
        
        # Add a warning record
        await add_warn(message.author.id, message.guild.id, self.bot.user.id, f"Auto-Mod: {reason}")
        
        # Notify user (temporary)
        try:
            warn_msg = await message.channel.send(f"⚠️ {message.author.mention}, {reason} nedeniyle mesajın silindi ve uyarı kaydedildi.")
            await warn_msg.delete(delay=5)
        except:
            pass

async def setup(bot):
    await bot.add_cog(AutoMod(bot))
