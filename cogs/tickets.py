import discord
from discord.ext import commands
from discord import ui, app_commands
import asyncio
import io
from database import get_server_config, add_log

DEFAULT_LOGO_URL = "https://cdn.discordapp.com/attachments/1491592592993947848/1491815965975908513/image.png?ex=69d91162&is=69d7bfe2&hm=250f5448f986cc40399dde27a38189623eed45581df740e7ff536425c26a0733"

class TicketControls(ui.View):
    """Ticket kanalındaki kontrol menüsü (Dropdown)"""
    def __init__(self):
        super().__init__(timeout=None)

    @ui.select(
        placeholder="Bilet İşlemleri Seçin...",
        options=[
            discord.SelectOption(label="Ticket Sil", value="delete", description="Talebi kalıcı olarak siler.", emoji="🗑️"),
            discord.SelectOption(label="Ticket Kapat", value="close", description="Talebi kapatır ve yetkililere bildirir.", emoji="🔒"),
            discord.SelectOption(label="Yetkili Üstlen", value="claim", description="Talebi üzerinize alır.", emoji="🙋‍♂️"),
            discord.SelectOption(label="Yedek Al", value="transcript", description="Mesaj geçmişini yedekler.", emoji="📑")
        ],
        custom_id="ticket_actions"
    )
    async def select_callback(self, interaction: discord.Interaction, select: ui.Select):
        config = await get_server_config(interaction.guild_id)
        log_channel_id = int(config.get('ticket_log_channel', 0) or 0) if config else 0

        if select.values[0] == "delete":
            await interaction.response.send_message("⚠️ Ticket 5 saniye içinde siliniyor...", ephemeral=True)
            await asyncio.sleep(5)
            await interaction.channel.delete()
            await add_log("ticket-log", interaction.user.id, interaction.user.name, f"TICKET DELETED: {interaction.channel.name}", interaction.guild_id)
        
        elif select.values[0] == "close":
            # Kanalı salt okunur yap
            overwrites = interaction.channel.overwrites
            for target in overwrites:
                if isinstance(target, discord.Member):
                    overwrites[target].send_messages = False
            
            await interaction.channel.edit(overwrites=overwrites)
            embed = discord.Embed(description="🔒 Bu ticket kapatıldı. Sadece yetkililer işlem yapabilir.", color=discord.Color.red())
            await interaction.response.send_message(embed=embed)
            await add_log("ticket-log", interaction.user.id, interaction.user.name, f"TICKET CLOSED: {interaction.channel.name}", interaction.guild_id)

        elif select.values[0] == "claim":
            embed = discord.Embed(description=f"✅ Ticket {interaction.user.mention} tarafından üstlenildi!", color=discord.Color.green())
            await interaction.response.send_message(embed=embed)
            try:
                await interaction.channel.edit(name=f"yöneticide-{interaction.user.name}")
            except: pass
            await add_log("ticket-log", interaction.user.id, interaction.user.name, f"TICKET CLAIMED: {interaction.channel.name}", interaction.guild_id)

        elif select.values[0] == "transcript":
            await interaction.response.defer(ephemeral=True)
            
            messages = [message async for message in interaction.channel.history(limit=None, oldest_first=True)]
            
            transcript_content = f"--- TICKET YEDEGI: {interaction.channel.name} ---\n"
            transcript_content += f"Talep Sahibi: {interaction.channel.name}\n"
            transcript_content += f"Yedek Alan: {interaction.user.name} ({interaction.user.id})\n"
            transcript_content += f"Zaman: {discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n\n"
            
            for msg in messages:
                timestamp = msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
                content = msg.content if msg.content else "[Resim/Dosya veya Boş Mesaj]"
                transcript_content += f"[{timestamp}] {msg.author}: {content}\n"
            
            file_data = io.BytesIO(transcript_content.encode('utf-8'))
            file_name = f"transcript-{interaction.channel.name}.txt"
            
            log_channel = interaction.guild.get_channel(log_channel_id) if log_channel_id else None
            
            if log_channel:
                await log_channel.send(
                    content=f"📑 **Ticket Yedegi**\nKanal: `#{interaction.channel.name}`\nYedek Alan: {interaction.user.mention}",
                    file=discord.File(file_data, filename=file_name)
                )
                await interaction.followup.send(content=f"✅ Yedek başarıyla {log_channel.mention} kanalına gönderildi!", ephemeral=True)
            else:
                file_data.seek(0)
                await interaction.followup.send(
                    content="⚠️ Log kanalı bulunamadı! Yedek direkt buraya gönderiliyor:",
                    file=discord.File(file_data, filename=file_name),
                    ephemeral=True
                )
            # Log the full transcript content to the database
            await add_log("ticket-log", interaction.user.id, interaction.user.name, transcript_content, interaction.guild_id)

class TicketView(ui.View):
    """Başlangıç mesajındaki Ticket açma butonları"""
    def __init__(self):
        super().__init__(timeout=None)

    async def create_ticket(self, interaction: discord.Interaction, ticket_type: str):
        guild = interaction.guild
        config = await get_server_config(guild.id)
        
        if not config:
            return await interaction.response.send_message("Hata: Sunucu ayarları bulunamadı!", ephemeral=True)
            
        category_id = int(config.get('ticket_category', 0) or 0)
        staff_role_id = int(config.get('ticket_staff_role', 0) or 0)
        logo_url = config.get('ticket_logo_url') or DEFAULT_LOGO_URL
        
        category = guild.get_channel(category_id)
        if not category:
            return await interaction.response.send_message("Hata: Ticket kategorisi bulunamadı! Lütfen web panelden ayarlayın.", ephemeral=True)

        # İzinleri ayarla
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }

        staff_role = guild.get_role(staff_role_id) if staff_role_id else None
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

        channel = await guild.create_text_channel(
            name=f"{ticket_type}-{interaction.user.name}",
            category=category,
            overwrites=overwrites
        )

        embed = discord.Embed(
            title="🎫 Ticket Kontrol - Destek",
            description=f"Merhaba {interaction.user.mention}, destek talebiniz başarıyla açıldı.\nEn kısa sürede bir yetkili sizinle ilgilenecektir.",
            color=0x3498db
        )
        embed.add_field(name="Konu Bilgisi", value=f"Destek Tipi: `{ticket_type.upper()}`\nDestek Talebi Açan: {interaction.user.mention}", inline=False)
        if logo_url:
            embed.set_image(url=logo_url)
        embed.set_footer(text="Gelişmiş Destek Sistemi")

        await channel.send(embed=embed, view=TicketControls())
        await interaction.response.send_message(f"✅ Ticket kanalınız oluşturuldu: {channel.mention}", ephemeral=True)
        await add_log("ticket-log", interaction.user.id, interaction.user.name, f"TICKET OPENED: {channel.name}", guild.id)

    @ui.button(label="🎮 Oyun İçi Destek", style=discord.ButtonStyle.success, custom_id="ticket_game")
    async def game_support(self, interaction: discord.Interaction, button: ui.Button):
        await self.create_ticket(interaction, "oyun-ici")

    @ui.button(label="🤝 Genel Destek", style=discord.ButtonStyle.primary, custom_id="ticket_general")
    async def general_support(self, interaction: discord.Interaction, button: ui.Button):
        await self.create_ticket(interaction, "oyuncu-disi")

    @ui.button(label="🚩 Hata Bildirimi", style=discord.ButtonStyle.danger, custom_id="ticket_bug")
    async def bug_report(self, interaction: discord.Interaction, button: ui.Button):
        await self.create_ticket(interaction, "hata-bildirim")

class Tickets(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_group(description="Ticket sistemini yönetir.")
    @commands.has_permissions(administrator=True)
    async def ticket(self, ctx):
        if ctx.invoked_subcommand is None:
            await ctx.send("❓ Kullanım: `/ticket setup` veya `!ticket setup`")

    @ticket.command(description="Ticket sistemini başlatır.")
    async def setup(self, ctx):
        config = await get_server_config(ctx.guild.id)
        logo_url = config.get('ticket_logo_url') if config and config.get('ticket_logo_url') else DEFAULT_LOGO_URL

        embed = discord.Embed(
            title="Ticket Sistemi - Destek",
            description=(
                "Lütfen sorununuza en yakın **başlığı seçerek** destek talebi açınız.\n\n"
                "🛡️ **Kurallara Uygunluk:** Gereksiz ticket açmak yasaktır.\n"
                "⏳ **Yanıt Süresi:** Yetkililerimiz en kısa sürede size dönüş yapacaktır.\n"
                "📂 **Kategoriler:** Oyun içi yardımlar, genel sorular ve hata bildirimleri için farklı butonlar mevcuttur."
            ),
            color=0x2f3136
        )
        if logo_url:
            embed.set_image(url=logo_url)
        embed.set_footer(text="Gelişmiş Destek Sistemi")
        
        await ctx.send(embed=embed, view=TicketView())
        await ctx.message.delete()

async def setup(bot):
    bot.add_view(TicketView())
    bot.add_view(TicketControls())
    await bot.add_cog(Tickets(bot))
