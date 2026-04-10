import discord
from discord import app_commands
from discord.ext import commands
import random
import asyncio
from database import get_balance, update_wallet

class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.hybrid_command(description="Mevcut Coin bakiyenizi gösterir.")
    @app_commands.describe(member="Bakiyesine bakılacak üye")
    async def balance(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        balance = await get_balance(member.id, ctx.guild.id)
        
        embed = discord.Embed(
            title=f"💰 {member.name} - Bakiye",
            description=f"**Cüzdan:** `{balance['wallet']} Coin`\n**Banka:** `{balance['bank']} Coin`",
            color=discord.Color.gold()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        await ctx.send(embed=embed)

    @commands.hybrid_command(description="Günlük Coin ödülünüzü alırsınız.")
    @commands.cooldown(1, 86400, commands.BucketType.user) # 24 saat
    async def daily(self, ctx):
        amount = random.randint(50, 200)
        await update_wallet(ctx.author.id, ctx.guild.id, amount)
        await ctx.send(f"🎁 Günlük ödülünü aldın! Hesabına **{amount} Coin** eklendi.")

    @commands.hybrid_command(description="Çalışarak Coin kazanırsınız.")
    @commands.cooldown(1, 3600, commands.BucketType.user) # 1 saat
    async def work(self, ctx):
        jobs = ["Yazılım Geliştirici", "Discord Modu", "Tasarımcı", "Garson", "Influencer", "Maden İşçisi"]
        job = random.choice(jobs)
        amount = random.randint(20, 100)
        await update_wallet(ctx.author.id, ctx.guild.id, amount)
        await ctx.send(f"💼 **{job}** olarak çalıştın ve **{amount} Coin** kazandın!")

    @commands.hybrid_command(description="Kumar oynayarak Coin kazanmaya (veya kaybetmeye) çalışırsınız.")
    @app_commands.describe(amount="Oynanacak miktar")
    async def slots(self, ctx, amount: int):
        if amount <= 0:
            return await ctx.send("❌ Lütfen geçerli bir miktar girin.")
        
        balance = await get_balance(ctx.author.id, ctx.guild.id)
        if balance['wallet'] < amount:
            return await ctx.send("❌ Cüzdanında yeterli Coin yok!")

        emojis = ["🍎", "🍊", "🍇", "🍒", "💎"]
        a = random.choice(emojis)
        b = random.choice(emojis)
        c = random.choice(emojis)

        slot_machine = f"[ {a} | {b} | {c} ]"
        
        if a == b == c:
            win = amount * 5
            await update_wallet(ctx.author.id, ctx.guild.id, win)
            msg = f"🎰 {slot_machine}\n\n**JACKPOT!** 🎉 **{win} Coin** kazandın!"
        elif a == b or b == c or a == c:
            win = amount * 2
            await update_wallet(ctx.author.id, ctx.guild.id, win)
            msg = f"🎰 {slot_machine}\n\n**Tebrikler!** 💰 **{win} Coin** kazandın!"
        else:
            await update_wallet(ctx.author.id, ctx.guild.id, -amount)
            msg = f"🎰 {slot_machine}\n\n**Kaybettin...** 💸 **{amount} Coin** cüzdanından uçtu."
        
        await ctx.send(msg)

    @commands.hybrid_command(description="Başka bir kullanıcıya Coin gönderirsiniz.")
    @app_commands.describe(member="Gönderilecek üye", amount="Gönderilecek miktar")
    async def send(self, ctx, member: discord.Member, amount: int):
        if amount <= 0 or member.id == ctx.author.id:
            return await ctx.send("❌ Geçersiz işlem.")
        
        balance = await get_balance(ctx.author.id, ctx.guild.id)
        if balance['wallet'] < amount:
            return await ctx.send("❌ Yeterli bakiyen yok.")

        await update_wallet(ctx.author.id, ctx.guild.id, -amount)
        await update_wallet(member.id, ctx.guild.id, amount)
        await ctx.send(f"💸 {ctx.author.mention}, {member.mention} kullanıcısına **{amount} Coin** gönderdi.")

    @daily.error
    @work.error
    async def cooldown_error(self, ctx, error):
        if isinstance(error, commands.CommandOnCooldown):
            hours = int(error.retry_after // 3600)
            minutes = int((error.retry_after % 3600) // 60)
            seconds = int(error.retry_after % 60)
            time_str = f"{hours}s {minutes}dk {seconds}sn" if hours > 0 else f"{minutes}dk {seconds}sn"
            await ctx.send(f"⏳ Sakin ol dostum! Bu komutu tekrar kullanmak için **{time_str}** beklemelisin.", delete_after=5)

async def setup(bot):
    await bot.add_cog(Economy(bot))
