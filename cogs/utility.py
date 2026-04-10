import discord
from discord.ext import commands
import datetime
import psutil
import os

class Utility(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_command(description="Sunucu hakkında detaylı bilgi verir.")
    async def serverinfo(self, ctx):
        guild = ctx.guild
        embed = discord.Embed(title=f"📊 {guild.name} - Sunucu Bilgisi", color=discord.Color.blue())
        embed.set_thumbnail(url=guild.icon.url if guild.icon else None)
        
        embed.add_field(name="Sunucu Sahibi", value=guild.owner.mention, inline=True)
        embed.add_field(name="Üye Sayısı", value=str(guild.member_count), inline=True)
        embed.add_field(name="Kanal Sayısı", value=str(len(guild.channels)), inline=True)
        embed.add_field(name="Rol Sayısı", value=str(len(guild.roles)), inline=True)
        embed.add_field(name="Oluşturulma", value=guild.created_at.strftime("%d/%m/%Y"), inline=True)
        embed.add_field(name="ID", value=str(guild.id), inline=True)
        
        await ctx.send(embed=embed)

    @commands.hybrid_command(description="Kullanıcı hakkında bilgi verir.")
    async def userinfo(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        embed = discord.Embed(title=f"👤 {member.name} - Kullanıcı Bilgisi", color=member.color)
        embed.set_thumbnail(url=member.display_avatar.url)
        
        embed.add_field(name="Kullanıcı Adı", value=member.name, inline=True)
        embed.add_field(name="ID", value=str(member.id), inline=True)
        embed.add_field(name="Hesap Tarihi", value=member.created_at.strftime("%d/%m/%Y"), inline=True)
        embed.add_field(name="Katılma Tarihi", value=member.joined_at.strftime("%d/%m/%Y") if member.joined_at else "Bilinmiyor", inline=True)
        embed.add_field(name="En Yüksek Rol", value=member.top_role.mention, inline=True)
        embed.add_field(name="Bot mu?", value="Evet" if member.bot else "Hayır", inline=True)
        
        await ctx.send(embed=embed)

    @commands.hybrid_command(description="Kullanıcının profil fotoğrafını gösterir.")
    async def avatar(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        embed = discord.Embed(title=f"🖼️ {member.name} - Avatar", color=discord.Color.random())
        embed.set_image(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.hybrid_command(description="Botun genel durumunu ve istatistiklerini gösterir.")
    async def botinfo(self, ctx):
        embed = discord.Embed(title=f"🤖 {self.bot.user.name} - Bot Bilgisi", color=discord.Color.green())
        
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        uptime = str(datetime.datetime.now() - datetime.datetime.fromtimestamp(psutil.boot_time())).split('.')[0]
        
        embed.add_field(name="Sunucu Sayısı", value=str(len(self.bot.guilds)), inline=True)
        embed.add_field(name="Toplam Kullanıcı", value=str(sum(g.member_count for g in self.bot.guilds)), inline=True)
        embed.add_field(name="CPU Kullanımı", value=f"%{cpu}", inline=True)
        embed.add_field(name="RAM Kullanımı", value=f"%{ram}", inline=True)
        embed.add_field(name="Komut Prefiksi", value=f"`{self.bot.command_prefix}`", inline=True)
        embed.add_field(name="Çalışma Süresi", value=uptime, inline=True)
        
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(Utility(bot))
