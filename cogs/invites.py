import discord
from discord.ext import commands
from database import update_invite_count, add_log

class Invites(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.invites = {} # {guild_id: {invite_code: uses}}

    @commands.Cog.listener()
    async def on_ready(self):
        # Bot yüklendiğinde tüm davetleri önbelleğe al
        for guild in self.bot.guilds:
            try:
                self.invites[guild.id] = {invite.code: invite.uses for invite in await guild.invites()}
            except:
                pass

    @commands.Cog.listener()
    async def on_invite_create(self, invite):
        if invite.guild.id not in self.invites:
            self.invites[invite.guild.id] = {}
        self.invites[invite.guild.id][invite.code] = invite.uses

    @commands.Cog.listener()
    async def on_invite_delete(self, invite):
        if invite.guild.id in self.invites:
            self.invites[invite.guild.id].pop(invite.code, None)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        guild_id = member.guild.id
        if guild_id not in self.invites:
            return

        try:
            # Yeni davet listesini çek ve karşılaştır
            new_invites = {invite.code: invite.uses for invite in await member.guild.invites()}
            
            for code, uses in new_invites.items():
                if code in self.invites[guild_id]:
                    if uses > self.invites[guild_id][code]:
                        # Bu davet kodu kullanıldı!
                        invite_obj = discord.utils.get(await member.guild.invites(), code=code)
                        if invite_obj and invite_obj.inviter:
                            inviter = invite_obj.inviter
                            await update_invite_count(guild_id, inviter.id, 1)
                            await add_log('invite-log', inviter.id, inviter.name, f"{member.name} sunucuya {code} koduyla katıldı.", guild_id)
                            print(f"✅ {member.name} katıldı. Davet eden: {inviter.name}")
                
            # Önbelleği güncelle
            self.invites[guild_id] = new_invites
        except Exception as e:
            print(f"❌ Davet takip hatası: {e}")

async def setup(bot):
    await bot.add_cog(Invites(bot))
