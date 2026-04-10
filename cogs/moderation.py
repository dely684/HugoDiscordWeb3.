import discord
from discord import app_commands
from discord.ext import commands
import asyncio
from database import add_log, add_warn, get_warns

class Moderation(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def is_staff():
        async def predicate(ctx):
            # Yönetici veya mesaj yönetme yetkisi olanlar
            return ctx.author.guild_permissions.manage_messages or ctx.author.guild_permissions.administrator
        return commands.check(predicate)

    @commands.hybrid_command(description="Botun gecikmesini ölçer.")
    async def ping(self, ctx):
        await ctx.send(f"🏓 Pong! Gecikme: {round(self.bot.latency * 1000)}ms")

    @commands.hybrid_command(description="Kullanıcıyı sunucudan atar.")
    @commands.has_permissions(kick_members=True)
    @app_commands.describe(member="Atılacak üye", reason="Atılma sebebi")
    async def kick(self, ctx, member: discord.Member, *, reason: str = "Belirtilmedi"):
        await member.kick(reason=reason)
        await ctx.send(f"✅ {member.name} sunucudan atıldı. Sebep: {reason}")
        await add_log("moderation-log", ctx.author.id, ctx.author.name, f"KICK: {member.name} | Sebep: {reason}", ctx.guild.id)

    @commands.hybrid_command(description="Kullanıcıyı yasaklar.")
    @commands.has_permissions(ban_members=True)
    @app_commands.describe(member="Yasaklanacak üye", reason="Yasaklanma sebebi")
    async def ban(self, ctx, member: discord.Member, *, reason: str = "Belirtilmedi"):
        await member.ban(reason=reason)
        await ctx.send(f"🚫 {member.name} yasaklandı. Sebep: {reason}")
        await add_log("moderation-log", ctx.author.id, ctx.author.name, f"BAN: {member.name} | Sebep: {reason}", ctx.guild.id)

    @commands.hybrid_command(description="Kullanıcıyı uyarır.")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(member="Uyarılacak üye", reason="Uyarı sebebi")
    async def warn(self, ctx, member: discord.Member, *, reason: str = "Belirtilmedi"):
        await add_warn(member.id, ctx.guild.id, ctx.author.id, reason)
        warns = await get_warns(member.id, ctx.guild.id)
        await ctx.send(f"⚠️ {member.mention} uyarıldı! Toplam Uyarı: {len(warns)}\nSebep: {reason}")
        await add_log("moderation-log", ctx.author.id, ctx.author.name, f"WARN: {member.name} | Sebep: {reason}", ctx.guild.id)

    @commands.hybrid_command(description="Kullanıcının uyarılarını gösterir.")
    @app_commands.describe(member="Uyarılarına bakılacak üye")
    async def warns(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        warn_list = await get_warns(member.id, ctx.guild.id)
        if not warn_list:
            return await ctx.send(f"✨ {member.name} kullanıcısının hiç uyarısı yok.")
        
        embed = discord.Embed(title=f"⚠️ {member.name} - Uyarı Listesi", color=discord.Color.orange())
        for i, warn in enumerate(warn_list, 1):
            embed.add_field(name=f"Uyarı #{i}", value=f"**Sebep:** {warn['reason']}\n**Mod:** <@{warn['moderator_id']}>\n**Tarih:** {warn['timestamp']}", inline=False)
        await ctx.send(embed=embed)

    @commands.hybrid_command(description="Belirtilen sayıda mesajı siler.")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(amount="Silinecek mesaj sayısı")
    async def purge(self, ctx, amount: int):
        if amount > 100:
            return await ctx.send("❌ Tek seferde en fazla 100 mesaj silebilirsiniz.")
        deleted = await ctx.channel.purge(limit=amount + 1)
        await ctx.send(f"🧹 {len(deleted)-1} mesaj silindi.", delete_after=3)

    @commands.hybrid_command(description="Kanalı mesaj gönderimine kapatır.")
    @commands.has_permissions(manage_channels=True)
    async def lock(self, ctx):
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
        await ctx.send("🔒 Kanal başarıyla kilitlendi.")

    @commands.hybrid_command(description="Kanalın kilidini açar.")
    @commands.has_permissions(manage_channels=True)
    async def unlock(self, ctx):
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=True)
        await ctx.send("🔓 Kanalın kilidi açıldı.")

    @commands.hybrid_command(description="Kanalı silip tekrar oluşturur (Temizlik).")
    @commands.has_permissions(administrator=True)
    async def nuke(self, ctx):
        channel_info = [ctx.channel.name, ctx.channel.category, ctx.channel.position, ctx.channel.overwrites]
        await ctx.channel.delete()
        new_channel = await ctx.guild.create_text_channel(
            name=channel_info[0],
            category=channel_info[1],
            position=channel_info[2],
            overwrites=channel_info[3],
            reason="NUKE Komutu"
        )
        await new_channel.send("☢️ Kanal patlatıldı ve temizlendi!")

async def setup(bot):
    await bot.add_cog(Moderation(bot))
