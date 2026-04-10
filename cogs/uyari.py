import discord
from discord import app_commands
from discord.ext import commands
import database
import io

class Uyari(commands.Cog, name="Uyarı Sistemi"):
    """Gelişmiş uyarı verme ve takip etme sistemi."""
    
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_command(name="uyar", description="Bir kullanıcıya uyarı verir.")
    @app_commands.describe(
        kullanici="Uyarılacak kullanıcı", 
        sebep="Uyarılma sebebi"
    )
    async def uyar(self, ctx, kullanici: discord.Member, sebep: str):
        config = await database.get_server_config(ctx.guild.id)
        
        has_permission = False
        if config and config.get('uyari_staff_role'):
            role = ctx.guild.get_role(int(config['uyari_staff_role']))
            if role and role in ctx.author.roles:
                has_permission = True
        
        if ctx.author.guild_permissions.administrator:
            has_permission = True
 
        if not has_permission:
            return await ctx.send("❌ Bu komutu kullanmak için gerekli yetkiye sahip değilsiniz!", ephemeral=True)
 
        await database.add_warn(kullanici.id, ctx.guild.id, ctx.author.id, sebep)
        
        # Calculate new total warnings
        all_warnings = await database.get_warns(kullanici.id, ctx.guild.id)
        yeni_uyari_sayisi = len(all_warnings)
        
        embed = discord.Embed(
            title="⚠️ KULLANICI UYARISI",
            description=f"{kullanici.mention} adlı kullanıcıya bir uyarı verildi.",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )
        
        embed.add_field(name="📋 Yetkili", value=ctx.author.mention, inline=True)
        embed.add_field(name="👤 Uyarılan Kişi", value=kullanici.mention, inline=True)
        embed.add_field(name="💬 Sebep", value=sebep, inline=False)
        embed.add_field(name="📈 Toplam Uyarı Sayısı", value=f"**{yeni_uyari_sayisi}**", inline=False)
        
        embed.set_footer(text=f"Kullanıcı ID: {kullanici.id}")
        if kullanici.display_avatar:
            embed.set_thumbnail(url=kullanici.display_avatar.url)
 
        await ctx.send(embed=embed)
        
        await database.add_log("moderation-log", ctx.author.id, ctx.author.name, f"UYARI: {kullanici.name} ({kullanici.id}) | Sebep: {sebep}", ctx.guild.id)
        
        # Eğer log kanalı ayarlıysa oraya da logla
        if config and config.get('uyari_log_channel'):
            log_channel = ctx.guild.get_channel(int(config['uyari_log_channel']))
            if log_channel:
                try:
                    await log_channel.send(embed=embed)
                except: pass
 
    @commands.hybrid_command(name="uyari-listesi", description="Tüm uyarı geçmişini detaylı bir metin dosyası olarak verir.")
    async def uyari_listesi(self, ctx):
        config = await database.get_server_config(ctx.guild.id)
        
        has_permission = False
        if config and config.get('uyari_staff_role'):
            role = ctx.guild.get_role(int(config['uyari_staff_role']))
            if role and role in ctx.author.roles:
                has_permission = True
                
        if ctx.author.guild_permissions.administrator:
            has_permission = True
 
        if not has_permission:
            return await ctx.send("❌ Bu komutu kullanmak için gerekli yetkiye sahip değilsiniz!", ephemeral=True)
 
        if config and config.get('uyari_log_channel'):
            if str(ctx.channel.id) != config['uyari_log_channel']:
                return await ctx.send(f"❌ Bu komut sadece <#{config['uyari_log_channel']}> kanalında kullanılabilir!", ephemeral=True)
 
        logs = await database.get_all_warns(ctx.guild.id)
        
        if not logs:
            return await ctx.send("📭 Henüz kayıtlı bir uyarı bulunmuyor.", ephemeral=True)
 
        content = "=== DETAYLI UYARI GEÇMİŞİ ===\n"
        content += "Format: [Tarih] | Uyarılan ID | Yetkili ID | Sebep\n"
        content += "-" * 70 + "\n\n"
        
        for log in logs:
            ts = log['timestamp']
            u_id = log['user_id']
            s_id = log['moderator_id']
            rs = log['reason']
            
            uyarilan_user = ctx.guild.get_member(int(u_id))
            yetkili_user = ctx.guild.get_member(int(s_id))
            
            u_name = uyarilan_user.display_name if uyarilan_user else "Bilinmeyen Kullanıcı"
            s_name = yetkili_user.display_name if yetkili_user else "Bilinmeyen Yetkili"
            
            content += f"[{ts}] | {u_name} ({u_id}) | Yetkili: {s_name} ({s_id}) | Sebep: {rs}\n"
 
        file_bytes = io.BytesIO(content.encode('utf-8'))
        discord_file = discord.File(file_bytes, filename="uyari_gecmisi.txt")
 
        await ctx.send("📄 Tüm uyarı geçmişi hazırlandı ve gönderiliyor...", ephemeral=True)
        await ctx.send(file=discord_file)

async def setup(bot):
    await bot.add_cog(Uyari(bot))
