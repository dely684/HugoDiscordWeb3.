import discord
from discord.ext import commands
from discord import app_commands
import database

class StreamModal(discord.ui.Modal, title="Yayın Duyurusu"):
    stream_link = discord.ui.TextInput(
        label="Yayın Linkin",
        placeholder="https://www.twitch.tv/kullanıcıadı",
        required=True,
        min_length=5
    )
    
    announcement_msg = discord.ui.TextInput(
        label="Duyuru Mesajın (İsteğe Bağlı)",
        placeholder="Yayındayım, hepinizi bekliyorum!",
        style=discord.TextStyle.paragraph,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        config = await database.get_server_config(interaction.guild.id)
        if not config or not config.get('yayinci_channel'):
            return await interaction.response.send_message("❌ Sistem henüz kurulmamış! Yayıncı kanalı eksik.", ephemeral=True)

        channel = interaction.guild.get_channel(int(config['yayinci_channel']))
        if not channel:
            return await interaction.response.send_message("❌ Duyuru kanalı bulunamadı!", ephemeral=True)

        role = None
        if config.get('yayinci_role'):
            role = interaction.guild.get_role(int(config['yayinci_role']))
        
        # Use provided message or fallback to database/default
        db_msg = await database.get_yayinci_message(interaction.user.id)
        final_msg = self.announcement_msg.value if self.announcement_msg.value else db_msg

        embed = discord.Embed(
            title=f"🔴 {interaction.user.display_name} Yayında!",
            description=f"{final_msg}",
            url=self.stream_link.value,
            color=discord.Color.purple()
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url if interaction.user.display_avatar else None)
        embed.add_field(name="Yayın Linki", value=f"[Buraya Tıkla]({self.stream_link.value})")
        
        if interaction.user.display_avatar:
            embed.set_thumbnail(url=interaction.user.display_avatar.url)
        
        mention = role.mention if role else "@everyone"
        await channel.send(content=f"Hey! {mention}, {interaction.user.mention} yayına girdi!", embed=embed)
        
        await database.add_log("yayinci-log", interaction.user.id, interaction.user.name, f"YAYIN DUYURUSU: {self.stream_link.value}", interaction.guild.id)
        
        await interaction.response.send_message("✅ Duyuru başarıyla gönderildi!", ephemeral=True)

class StreamButton(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Persistent view

    @discord.ui.button(label="Yayına Girdim! 🔴", style=discord.ButtonStyle.danger, custom_id="stream_announcement_btn")
    async def stream_button_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = await database.get_server_config(interaction.guild.id)
        if config and config.get('yayinci_role'):
            role = interaction.guild.get_role(int(config['yayinci_role']))
            if role and role not in interaction.user.roles:
                return await interaction.response.send_message(f"❌ Bu butonu kullanmak için {role.mention} rolüne sahip olmalısın!", ephemeral=True)

        modal = StreamModal()
        db_msg = await database.get_yayinci_message(interaction.user.id)
        modal.announcement_msg.default = db_msg
        await interaction.response.send_modal(modal)

class Yayinci(commands.Cog, name="Yayıncı Sistemi"):
    """Yayıncıların yayın duyurularını yapması için otomasyon."""

    def __init__(self, bot):
        self.bot = bot
        self.bot.add_view(StreamButton())

    @commands.hybrid_command(name="mesaj_ayarla", description="Duyuru mesajını kaydeder/günceller.")
    @app_commands.describe(mesaj="Duyuru metni")
    async def register_message(self, ctx, mesaj: str):
        config = await database.get_server_config(ctx.guild.id)
        if config and config.get('yayinci_role'):
            role = ctx.guild.get_role(int(config['yayinci_role']))
            if role and role not in ctx.author.roles:
                 return await ctx.send(f"❌ Bu komutu kullanmak için {role.mention} rolüne sahip olmalısın!", ephemeral=True)
        
        await database.set_yayinci_message(ctx.author.id, mesaj)
        await ctx.send(f"✅ Duyuru mesajın kaydedildi: \n`{mesaj}`", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Yayinci(bot))
