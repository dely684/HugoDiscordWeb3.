import discord
from discord import app_commands
from discord.ext import commands
import database

class EkipKapatView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Ekibi Kapat", style=discord.ButtonStyle.danger, custom_id="ekip_kapat_btn")
    async def kapat(self, interaction: discord.Interaction, button: discord.ui.Button):
        team_data = await database.get_ekip_team_by_channel(interaction.channel.id)
        if not team_data:
            return await interaction.response.send_message("Bu kanal geçerli bir ekip kanalı değil.", ephemeral=True)
            
        config = await database.get_server_config(interaction.guild.id)
        is_staff = False
        if config and config.get('ekip_staff_role'):
            staff_role = interaction.guild.get_role(int(config['ekip_staff_role']))
            if staff_role and staff_role in interaction.user.roles:
                is_staff = True
                
        is_founder = (str(interaction.user.id) == team_data['leader_id'])
        
        if not (is_founder or is_staff or interaction.user.guild_permissions.administrator):
            return await interaction.response.send_message("Bu ekibi kapatmak için ekip kurucusu veya yetkili olmalısınız!", ephemeral=True)

        await interaction.response.send_message("Ekip kapatılıyor, roller ve kanal silinecek...", ephemeral=True)
        guild = interaction.guild
        
        try:
            boss = guild.get_role(int(team_data['boss_role_id']))
            if boss: await boss.delete()
            og = guild.get_role(int(team_data['og_role_id']))
            if og: await og.delete()
            normal = guild.get_role(int(team_data['normal_role_id']))
            if normal: await normal.delete()
        except Exception:
            pass
            
        await database.delete_ekip_team(team_data['id'])
        await database.add_log("ekip-log", interaction.user.id, interaction.user.name, f"EKİP KAPATILDI: {team_data['ekip_ismi']}", interaction.guild.id)
        
        try:
            await interaction.channel.delete()
        except:
            pass

class EkipModal(discord.ui.Modal, title='Ekip Oluşturma Formu'):
    ekip_ismi = discord.ui.TextInput(label='Ekip İsmi', placeholder='Ekibinizin adını giriniz...', required=True)
    kac_kisi = discord.ui.TextInput(label='Kaç Kişi', placeholder='Ekibiniz kaç kişiden oluşuyor?', required=True)
    aciklama = discord.ui.TextInput(label='Açıklama (Max 50)', placeholder='Kısa bir açıklama giriniz...', max_length=50, required=True)
    ekip_rengi = discord.ui.TextInput(label='Ekip Rengi (Hex Kodu)', placeholder='#FF0000 gibi...', min_length=7, max_length=7, required=True)

    async def on_submit(self, interaction: discord.Interaction):
        config = await database.get_server_config(interaction.guild.id)
        if not config or not config.get('ekip_log_channel'):
            return await interaction.response.send_message("Sistem henüz yapılandırılmadı. Ekip başvuru kanalı eksik.", ephemeral=True)
            
        staff_channel = interaction.guild.get_channel(int(config['ekip_log_channel']))
        if not staff_channel:
            return await interaction.response.send_message("Yetkili başvuru kanalı bulunamadı!", ephemeral=True)

        embed = discord.Embed(title="Yeni Ekip Başvurusu", color=discord.Color.yellow())
        embed.add_field(name="Başvuran", value=interaction.user.mention, inline=False)
        embed.add_field(name="Ekip İsmi", value=self.ekip_ismi.value, inline=True)
        embed.add_field(name="Kişi Sayısı", value=self.kac_kisi.value, inline=True)
        embed.add_field(name="Ekip Rengi", value=self.ekip_rengi.value, inline=True)
        embed.add_field(name="Açıklama", value=self.aciklama.value, inline=False)

        view = OnayView(
            applicant_id=interaction.user.id,
            ekip_ismi=self.ekip_ismi.value,
            aciklama=self.aciklama.value,
            ekip_rengi=self.ekip_rengi.value
        )
        
        await staff_channel.send(embed=embed, view=view)
        await interaction.response.send_message("Başvurunuz yetkililere iletildi!", ephemeral=True)

class OnayView(discord.ui.View):
    def __init__(self, applicant_id, ekip_ismi, aciklama, ekip_rengi):
        super().__init__(timeout=None)
        self.applicant_id = applicant_id
        self.ekip_ismi = ekip_ismi
        self.aciklama = aciklama
        self.ekip_rengi = ekip_rengi

    @discord.ui.button(label="Onayla", style=discord.ButtonStyle.green, custom_id="ekip_onay_btn")
    async def onayla(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = await database.get_server_config(interaction.guild.id)
        is_staff = interaction.user.guild_permissions.administrator
        
        if config and config.get('ekip_staff_role'):
            is_staff = is_staff or any(str(role.id) == config['ekip_staff_role'] for role in interaction.user.roles)

        if not is_staff:
            return await interaction.response.send_message("Bu işlemi sadece yetkililer yapabilir!", ephemeral=True)

        guild = interaction.guild
        applicant = guild.get_member(self.applicant_id)
        if not applicant:
            return await interaction.response.send_message("Başvuran kişi sunucuda bulunamadı!", ephemeral=True)

        await interaction.response.defer()

        try:
            try:
                hex_color = self.ekip_rengi.replace("#", "")
                final_color = discord.Color(int(hex_color, 16))
            except:
                final_color = discord.Color.light_grey()

            boss_role = await guild.create_role(name=f"{self.ekip_ismi} BOSS", color=final_color, reason="Ekip Sistemi")
            og_role = await guild.create_role(name=f"{self.ekip_ismi} OG", color=final_color, reason="Ekip Sistemi")
            normal_role = await guild.create_role(name=self.ekip_ismi, color=final_color, reason="Ekip Sistemi")

            await applicant.add_roles(boss_role)

            category = None
            if config and config.get('ekip_category'):
                category = discord.utils.get(guild.categories, id=int(config['ekip_category']))

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                boss_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                og_role: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                normal_role: discord.PermissionOverwrite(read_messages=True, send_messages=True)
            }
            
            if config and config.get('ekip_staff_role'):
                staff_role = guild.get_role(int(config['ekip_staff_role']))
                if staff_role:
                    overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

            channel = await guild.create_text_channel(
                name=self.ekip_ismi.lower().replace(" ", "-"),
                category=category,
                overwrites=overwrites
            )

            await database.add_ekip_team(
                guild.id, self.ekip_ismi, boss_role.id, og_role.id, normal_role.id, channel.id, applicant.id
            )
            
            await database.add_log("ekip-log", interaction.user.id, interaction.user.name, f"EKİP ONAYLANDI: {self.ekip_ismi} | Başvuran: {applicant.name}", interaction.guild.id)

            embed = interaction.message.embeds[0]
            embed.color = discord.Color.green()
            embed.title = "Ekip Başvurusu Onaylandı"
            embed.set_footer(text=f"Onaylayan: {interaction.user.display_name}")
            
            await interaction.message.edit(embed=embed, view=None)
            
            close_embed = discord.Embed(
                title=f"🎉 {self.ekip_ismi} Ekibi Kuruldu!",
                description="Ekibiniz başarıyla onaylandı ve rolleriniz teslim edildi.\nBu ekibi lağvetmek (kapatmak) ve tüm kanalları/rolleri kaldırmak isterseniz aşağıdaki butonu kullanabilirsiniz.",
                color=discord.Color.blue()
            )
            await channel.send(f"Hoş geldiniz {applicant.mention}!", embed=close_embed, view=EkipKapatView())
            
        except Exception as e:
            await interaction.followup.send(f"Bir hata oluştu: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Reddet", style=discord.ButtonStyle.red, custom_id="ekip_red_btn")
    async def reddet(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = await database.get_server_config(interaction.guild.id)
        is_staff = interaction.user.guild_permissions.administrator
        
        if config and config.get('ekip_staff_role'):
            is_staff = is_staff or any(str(role.id) == config['ekip_staff_role'] for role in interaction.user.roles)

        if not is_staff:
            return await interaction.response.send_message("Bu işlemi sadece yetkililer yapabilir!", ephemeral=True)

        embed = interaction.message.embeds[0]
        embed.color = discord.Color.red()
        embed.title = "Ekip Başvurusu Reddedildi"
        embed.set_footer(text=f"Reddeden: {interaction.user.display_name}")
        
        await database.add_log("ekip-log", interaction.user.id, interaction.user.name, f"EKİP REDDEDİLDİ: {self.ekip_ismi} | Başvuran ID: {self.applicant_id}", interaction.guild.id)
        
        await interaction.response.edit_message(embed=embed, view=None)

class EkipGirisView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Ekip Oluştur", style=discord.ButtonStyle.blurple, custom_id="ekip_olustur_btn")
    async def setup_ekip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EkipModal())


class EkipBasvur(commands.Cog, name="Ekip Sistemi"):
    """Sunucu ekip oluşturma ve yönetme sistemi."""
    
    def __init__(self, bot):
        self.bot = bot
        self.bot.add_view(EkipGirisView()) # Persistent View Event Catching
        self.bot.add_view(EkipKapatView()) # Ekip Kapatma butonu için kalıcı kayıt

    @commands.hybrid_command(name="toplam-ekip", description="Sunucudaki toplam ekip sayısını gösterir.")
    async def toplam_ekip(self, ctx):
        teams = await database.get_all_teams(ctx.guild.id)
        total = len(teams)
        await ctx.send(f"Şu ana kadar toplam **{total}** ekip oluşturuldu!", ephemeral=False)

    @commands.hybrid_command(name="ekip-bilgi", description="Belirli bir ekibin üyelerini ve bilgilerini gösterir.")
    @app_commands.describe(ekip_ismi="Bilgisini almak istediğiniz ekibin tam adını yazın.")
    async def ekip_bilgi(self, ctx, ekip_ismi: str):
        team_data = await database.get_team(ctx.guild.id, ekip_ismi)
        if not team_data:
            return await ctx.send("Böyle bir ekip bulunamadı!", ephemeral=True)

        guild = ctx.guild
        roles = [
            guild.get_role(int(team_data["boss_role_id"])) if team_data["boss_role_id"] else None,
            guild.get_role(int(team_data["og_role_id"])) if team_data["og_role_id"] else None,
            guild.get_role(int(team_data["normal_role_id"])) if team_data["normal_role_id"] else None
        ]
        
        members_list = []
        seen_ids = set()
        
        for role in roles:
            if role:
                for member in role.members:
                    if member.id not in seen_ids:
                        members_list.append(f"{member.mention} (ID: {member.id})")
                        seen_ids.add(member.id)

        if not members_list:
            members_str = "Bu ekipte şu an kimse bulunmuyor."
        else:
            members_str = "\n".join(members_list)

        embed = discord.Embed(title=f"Ekip Bilgilendirmesi: {ekip_ismi}", color=discord.Color.green())
        embed.add_field(name="Üye Listesi", value=members_str, inline=False)
        embed.add_field(name="Toplam Üye Sayısı", value=str(len(members_list)), inline=True)
        
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="ekip-kapat-buton", description="Bulunduğunuz ekip kanalına kapatma butonunu tekrar gönderir.")
    async def ekip_kapat_buton(self, ctx):
        team_data = await database.get_ekip_team_by_channel(ctx.channel.id)
        if not team_data:
            return await ctx.send("Bu komutu sadece bir ekip kanalında kullanabilirsiniz!", ephemeral=True)

        config = await database.get_server_config(ctx.guild.id)
        is_staff = False
        if config and config.get('ekip_staff_role'):
            staff_role = ctx.guild.get_role(int(config['ekip_staff_role']))
            if staff_role and staff_role in ctx.author.roles:
                is_staff = True
                
        is_founder = (str(ctx.author.id) == team_data['leader_id'])
        
        if not (is_founder or is_staff or ctx.author.guild_permissions.administrator):
            return await ctx.send("Bu butonu göndermek için ekip kurucusu veya yetkili olmalısınız!", ephemeral=True)

        embed = discord.Embed(
            title="Ekip Yönetimi",
            description="Ekibi lağvetmek ve tüm rolleri/kanalları kaldırmak için aşağıdaki butonu kullanabilirsiniz.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed, view=EkipKapatView())

async def setup(bot):
    await bot.add_cog(EkipBasvur(bot))
