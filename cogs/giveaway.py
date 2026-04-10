import discord
from discord.ext import commands, tasks
import asyncio
import random
import datetime
from database import add_giveaway, get_active_giveaways, update_giveaway_status, add_log

class Giveaway(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_giveaways.start()

    def cog_unload(self):
        self.check_giveaways.cancel()

    @tasks.loop(seconds=30)
    async def check_giveaways(self):
        """Çekilişlerin bitiş süresini kontrol eden döngü."""
        active_giveaways = await get_active_giveaways()
        now = datetime.datetime.now()

        for g in active_giveaways:
            end_time = datetime.datetime.strptime(g['end_time'], "%Y-%m-%d %H:%M:%S")
            if now >= end_time:
                await self.end_giveaway(g)

    @commands.hybrid_command(description="Çekiliş başlatır. Örn: /gstart duration:60s winners:1 prize:Hediye")
    @commands.has_permissions(manage_guild=True)
    async def gstart(self, ctx, duration: str, winners: int, *, prize: str):
        # Süreyi saniyeye çevir
        seconds = 0
        if duration.endswith("s"): seconds = int(duration[:-1])
        elif duration.endswith("m"): seconds = int(duration[:-1]) * 60
        elif duration.endswith("h"): seconds = int(duration[:-1]) * 3600
        else: return await ctx.send("❌ Hata! Süreyi s/m/h formatında girin (Örn: 60s, 5m)")

        success, msg = await self.start_giveaway(ctx.guild.id, ctx.channel.id, duration, winners, prize, ctx.author.id, ctx.author.name)
        if not success:
            await ctx.send(f"❌ Hata: {msg}")

    async def start_giveaway(self, guild_id, channel_id, duration, winners, prize, author_id, author_name):
        """Çekiliş başlatma mantığı (API ve Komutlar tarafından ortak kullanılır)."""
        guild = self.bot.get_guild(int(guild_id))
        channel = self.bot.get_channel(int(channel_id))
        if not channel: return False, "Kanal bulunamadı."

        seconds = 0
        if duration.endswith("s"): seconds = int(duration[:-1])
        elif duration.endswith("m"): seconds = int(duration[:-1]) * 60
        elif duration.endswith("h"): seconds = int(duration[:-1]) * 3600
        else: return False, "Geçersiz süre formatı."

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)

        embed = discord.Embed(
            title="🎉 ÇEKİLİŞ BAŞLADI 🎉",
            description=f"**Ödül:** {prize}\n**Bitiş:** <t:{int(end_time.timestamp())}:R>\n**Kazanan Sayısı:** {winners}",
            color=discord.Color.blue(),
            timestamp=datetime.datetime.now()
        )
        embed.set_footer(text="Katılmak için aşağıdaki 🎉 emojisine bas!")
        
        try:
            giveaway_msg = await channel.send(embed=embed)
            await giveaway_msg.add_reaction("🎉")
            
            # Veritabanına kaydet
            await add_giveaway(guild_id, channel_id, giveaway_msg.id, prize, winners, end_time)
            await add_log('giveaway-log', author_id, author_name, f"Yeni çekiliş başlatıldı: {prize}", guild_id)
            return True, "Çekiliş başlatıldı."
        except Exception as e:
            return False, str(e)
        
    async def end_giveaway(self, g_data):
        """Çekilişi bitirir ve kazananları seçer."""
        channel = self.bot.get_channel(int(g_data['channel_id']))
        if not channel:
            await update_giveaway_status(g_data['id'], 'cancelled')
            return

        try:
            msg = await channel.fetch_message(int(g_data['message_id']))
        except:
            await update_giveaway_status(g_data['id'], 'error')
            return

        users = [user async for user in msg.reactions[0].users() if not user.bot]

        if len(users) == 0:
            await channel.send(f"❌ **{g_data['prize']}** çekilişine kimse katılmadığı için kazanan yok.")
            await update_giveaway_status(g_data['id'], 'ended')
            return

        winners_list = random.sample(users, min(len(users), g_data['winners']))
        winners_mentions = ", ".join([u.mention for u in winners_list])

        win_embed = discord.Embed(
            title="🎊 ÇEKİLİŞ SONUÇLANDI 🎊",
            description=f"**Ödül:** {g_data['prize']}\n**Kazananlar:** {winners_mentions}",
            color=discord.Color.gold()
        )
        await channel.send(embed=win_embed)
        await channel.send(f"Tebrikler {winners_mentions}! **{g_data['prize']}** kazandınız!")
        
        await update_giveaway_status(g_data['id'], 'ended')

async def setup(bot):
    await bot.add_cog(Giveaway(bot))
