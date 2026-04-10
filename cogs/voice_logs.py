import discord
from discord.ext import commands
from database import add_log

class VoiceLogs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_voice_state_update(self, member, before, after):
        if member.bot:
            return

        guild_id = member.guild.id
        username = member.name
        user_id = member.id

        # 1. Join
        if before.channel is None and after.channel is not None:
            await add_log('voice-log', user_id, username, f"🔉 Sese katıldı: {after.channel.name}", guild_id)

        # 2. Leave
        elif before.channel is not None and after.channel is None:
            await add_log('voice-log', user_id, username, f"🔇 Sesten ayrıldı: {before.channel.name}", guild_id)

        # 3. Move
        elif before.channel is not None and after.channel is not None and before.channel.id != after.channel.id:
            await add_log('voice-log', user_id, username, f"🔄 Ses değiştirdi: {before.channel.name} -> {after.channel.name}", guild_id)

        # 4. Mute/Deafen (Optional but useful for Roleplay)
        if before.self_mute != after.self_mute:
            status = "Susturdu" if after.self_mute else "Açtı"
            await add_log('voice-log', user_id, username, f"🎤 Mikrofon {status}", guild_id)
            
        if before.self_deaf != after.self_deaf:
            status = "Sağırlaştırdı" if after.self_deaf else "Açtı"
            await add_log('voice-log', user_id, username, f"🎧 Kulaklık {status}", guild_id)

async def setup(bot):
    await bot.add_cog(VoiceLogs(bot))
