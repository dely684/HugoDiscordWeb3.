import os
import asyncio
import discord
import psutil
from discord.ext import commands, tasks
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import uvicorn
from database import init_db, add_log, get_logs, add_rule, get_rules, add_suggestion, update_suggestion_status, add_application, update_server_channels, get_server_config, get_suggestion_list, get_application_list, update_application_status, get_application_by_id, get_all_teams, delete_ekip_team, add_token, get_all_tokens, delete_token, validate_token, add_auto_responder, get_auto_responders, delete_auto_responder, find_auto_response, increment_stat, get_analytics_data
import secrets
import sys
from datetime import timedelta

DEFAULT_LOGO_URL = "https://cdn.discordapp.com/attachments/1491592592993947848/1491815965975908513/image.png?ex=69d91162&is=69d7bfe2&hm=250f5448f986cc40399dde27a38189623eed45581df740e7ff536425c26a0733"

# Load config
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
PORT = int(os.getenv("PORT", 8000))

# --- DISCORD BOT SETUP ---
intents = discord.Intents.default()
intents.message_content = True
intents.members = True # Üye listesini çekebilmek için gerekli
intents.moderation = True # Denetim kaydı (Audit Log) için gerekli
bot = commands.Bot(command_prefix="!", intents=intents)

# Kayıtların tekrar etmemesi için son işlenen Audit Log ID'sini tutuyoruz
last_audit_id = None

# Local Config Store (In a real app, use a database)
bot_config = {
    "prefix": "!",
    "activity": "Web Panel ile Kontrol Ediliyor",
    "commands_status": {} # {'ping': True, 'yardim': False, ...}
}

# Global Check: Komutun aktif olup olmadığını kontrol et
@bot.check
async def check_commands_status(ctx):
    cmd_name = ctx.command.name
    status = bot_config["commands_status"].get(cmd_name, True)
    if not status:
        await ctx.send(f"❌ `{cmd_name}` komutu şu anda web panel üzerinden devre dışı bırakılmış.", delete_after=5)
        return False
    return True

@bot.event
async def on_ready():
    print(f"Bot giriş yaptı: {bot.user.name}")
    await bot.change_presence(activity=discord.Game(name=bot_config["activity"]))
    
    # --- MODÜLER COĞ YÜKLEYİCİ ---
    print("📦 Modüller yükleniyor...")
    for filename in os.listdir('./cogs'):
        if filename.endswith('.py'):
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                print(f"✅ Yüklendi: {filename}")
            except Exception as e:
                print(f"❌ Hata ({filename}): {e}")

    # Tüm komutları başlangıçta aktif olarak işaretle (Dashboard kontrolü için)
    for cmd in bot.commands:
        if cmd.name not in bot_config["commands_status"]:
            bot_config["commands_status"][cmd.name] = True

    # Slash Komutlarını Senkronize Et
    try:
        synced = await bot.tree.sync()
        print(f"✅ {len(synced)} Slash komutu senkronize edildi.")
    except Exception as e:
        print(f"❌ Slash senkronizasyon hatası: {e}")

    # Örnek Log (Eğer boşsa test amaçlı)
    logs = await get_logs(limit=1)
    if not logs:
        await add_log("ticket-log", "0", "Sistem", "Veritabanı başarıyla başlatıldı.")
        await add_log("ticket-log", "123", "DenemeKullanici", "Ticket #001 açıldı.")

    # İstatistik Güncelleme Döngüsünü Başlat
    update_live_stats.start()

@tasks.loop(minutes=1)
async def update_live_stats():
    """Dashboard için canlı istatistikleri günceller."""
    if not bot.is_ready(): return
    
    for guild in bot.guilds:
        # Bu verileri bir cache'e veya DB'ye yazabiliriz. 
        # API her çağrıldığında hesaplamak büyük sunucularda yavaş olabilir.
        online_members = len([m for m in guild.members if m.status != discord.Status.offline])
        voice_members = sum(len(c.members) for c in guild.voice_channels)
        total_members = guild.member_count
        
        # Statları increment_stat benzeri bir yapıya kaydedebiliriz veya direkt API'den hesaplatırız.
        # Şimdilik direkt API'den hesaplatacağız ama bu döngü ilerde "Grafik" verileri için kullanılabilir.
        pass




# --- DEV COMMANDS ---
@bot.command()
@commands.is_owner()
async def sync(ctx):
    """Slash komutlarını manuel olarak senkronize eder."""
    try:
        synced = await bot.tree.sync()
        await ctx.send(f"✅ {len(synced)} slash komutu senkronize edildi!")
    except Exception as e:
        await ctx.send(f"❌ Hata: {e}")

# --- BOT EVENTS ---

@bot.event
async def on_member_join(member):
    await increment_stat(member.guild.id, 'joins')
    await add_log('member-join', f'Yeni üye katıldı: {member.name}', guild_id=member.guild.id)

@bot.event
async def on_member_remove(member):
    await increment_stat(member.guild.id, 'leaves')
    await add_log('member-leave', f'Üye ayrıldı: {member.name}', guild_id=member.guild.id)

@bot.event
async def on_message(message):
    # Eğer botun kendi mesajıysa işlem yapma
    if message.author.id == bot.user.id:
        return

    # İstatistiği artır (Eğer sunucu mesajıysa)
    if message.guild:
        await increment_stat(message.guild.id, 'messages')

    # Sunucu Ayarlarını Al
    if message.guild:
        config = await get_server_config(message.guild.id)
        if config:
            # 1. Öneri Kanalı Kontrolü
            if str(message.channel.id) == config.get('suggestions_channel'):
                await add_suggestion(message.guild.id, message.author.id, message.content, message.id)

                print(f"📩 Yeni Öneri Kaydedildi: {message.author.name}")

            # 2. Başvuru Kanalı Kontrolü
            elif str(message.channel.id) == config.get('applications_channel'):
                await add_application(message.guild.id, message.author.id, message.content, "staff", message.id)
                await message.add_reaction("📑")
                print(f"📑 Yeni Başvuru Kaydedildi: {message.author.name}")

    # Eğer mesaj log kanalına geldiyse (Ticket logları vb.)
    log_channel_id = config.get('ticket_log_channel') if config else None
    if log_channel_id and str(message.channel.id) == str(log_channel_id):
        print(f"\n[LOG RECEIVED] Kanal: {message.channel.name} | Gönderen: {message.author.name}")
        
        full_content = message.content if message.content else ""
        
        # 1. Embed Desteği (Renkli kutuların içini oku)
        if message.embeds:
            print(f"- Mesajda {len(message.embeds)} adet Embed bulundu. Okunuyor...")
            for embed in message.embeds:
                if embed.title:
                    full_content += f"\n**Başlık:** {embed.title}"
                if embed.description:
                    full_content += f"\n**Açıklama:** {embed.description}"
                for field in embed.fields:
                    full_content += f"\n**{field.name}:** {field.value}"
        
        # 2. TXT Dosya Desteği
        if message.attachments:
            print(f"- Mesajda {len(message.attachments)} adet dosya bulundu.")
            for attachment in message.attachments:
                if attachment.filename.endswith('.txt'):
                    try:
                        file_content = await attachment.read()
                        full_content += f"\n\n[DOSYA İÇERİĞİ ({attachment.filename})]:\n{file_content.decode('utf-8')}"
                        print(f"- {attachment.filename} içeriği okundu.")
                    except Exception as e:
                        print(f"- Dosya okuma hatası: {e}")

        # Eğer hala içerik boşsa (sadece resim falan olabilir)
        if not full_content.strip():
            full_content = "[İçerik bulunamadı / Sadece görsel]"

        # 3. Veritabanına Kaydet
        try:
            await add_log(
                log_type="ticket-log",
                user_id=message.author.id,
                username=message.author.name,
                content=full_content,
                guild_id=message.guild.id if message.guild else None
            )
            print("✅ Log başarıyla veritabanına ve web sitesine kaydedildi.")
        except Exception as e:
            print(f"❌ Veritabanı kayıt hatası: {e}")

    # 3. Otomatik Cevap Kontrolü
    if message.guild:
        auto_reply = await find_auto_response(message.guild.id, message.content.lower())
        if auto_reply:
            await message.reply(auto_reply)
            return # Komut işlemeye devam etmesin (opsiyonel, genelde daha iyidir)

    await bot.process_commands(message)

# --- FASTAPI SETUP ---
app = FastAPI()


# Data models
class MessageRequest(BaseModel):
    channel_id: str
    content: str

class ChannelCreateRequest(BaseModel):
    name: str

class StatusRequest(BaseModel):
    status: str

class SettingsRequest(BaseModel):
    prefix: str
    activity: str

class CommandToggleRequest(BaseModel):
    name: str
    status: bool

class RuleRequest(BaseModel):
    title: str
    content: str

class ConfigUpdateRequest(BaseModel):
    rules_channel: str = None
    suggestions_channel: str = None
    applications_channel: str = None
    ticket_category: str = None
    ticket_log_channel: str = None
    ticket_staff_role: str = None
    ticket_logo_url: str = None
    ekip_category: str = None
    ekip_staff_role: str = None
    ekip_log_channel: str = None
    yayinci_channel: str = None
    yayinci_role: str = None
    uyari_log_channel: str = None
    uyari_staff_role: str = None
    automod_links: int = None
    automod_spam: int = None
    automod_words: str = None

class GiveawayRequest(BaseModel):
    prize: str
    winners: int
    duration: str # "1h", "10m" etc.
    channel_id: str

class TokenGenerateRequest(BaseModel):
    role: str

class LoginRequest(BaseModel):
    token: str

class AutoResponderRequest(BaseModel):
    keyword: str
    response: str

class EmbedRequest(BaseModel):
    channel_id: str
    title: str = ""
    description: str = ""
    color: str = "#5865f2"
    image_url: str = ""
    thumbnail_url: str = ""
    author_name: str = ""
    footer_text: str = ""

# Üye Yönetimi Modelleri
class MemberActionRequest(BaseModel):
    reason: str = "Belirtilmedi"
    duration: int = None # Timeout için (dakika)
    roles: list = [] # Rol güncelleme için

class BotStatusRequest(BaseModel):
    status: str # online, dnd, idle, invisible
    activity_type: str # playing, streaming, listening, watching
    activity_name: str

# API Endpoints
@app.get("/api/stats")
async def get_stats():
    # Bot ve Sistem verilerini topla
    cpu_usage = psutil.cpu_percent()
    ram_usage = psutil.virtual_memory().percent
    
    return {
        "online": bot.is_ready(),
        "guild_count": len(bot.guilds) if bot.is_ready() else 0,
        "user_count": sum(g.member_count for g in bot.guilds) if bot.is_ready() else 0,
        "latency": round(bot.latency * 1000, 1) if bot.is_ready() else 0,
        "bot_name": bot.user.name if bot.is_ready() else "Bilinmiyor",
        "cpu": cpu_usage,
        "ram": ram_usage
    }

@app.get("/api/guild/{guild_id}/analytics")
async def get_analytics(guild_id: str, days: int = 7):
    data = await get_analytics_data(guild_id, days)
    return data

@app.get("/api/bot/console")
async def get_console_logs():
    # Son 50 logu çek
    logs = await get_logs()
    # Logların okunabilirliğini artırmak için formatlayabiliriz (isteğe bağlı)
    return logs[:50]

@app.get("/api/commands")
async def get_commands():
    cmds = []
    for command in bot.commands:
        cmds.append({
            "name": command.name,
            "description": command.help or "Açıklama yok.",
            "status": bot_config["commands_status"].get(command.name, True),
            "category": command.cog_name if command.cog else "Genel"
        })
    return cmds

@app.post("/api/commands/toggle")
async def toggle_command(req: CommandToggleRequest):
    # Komutun varlığını kontrol et
    if req.name in bot_config["commands_status"] or any(c.name == req.name for c in bot.commands):
        bot_config["commands_status"][req.name] = req.status
        print(f"⚙️ Komut Durumu Değişti: {req.name} -> {'Aktif' if req.status else 'Pasif'}")
        return {"status": "success", "new_status": req.status}
    raise HTTPException(status_code=404, detail="Komut bulunamadı.")

@app.get("/api/servers")
async def get_servers():
    if not bot.is_ready():
        return []
    servers = []
    for guild in bot.guilds:
        servers.append({
            "id": str(guild.id),
            "name": guild.name,
            "member_count": guild.member_count,
            "icon": str(guild.icon.url) if guild.icon else None
        })
    return servers

@app.get("/api/guild/{guild_id}/roles")
async def get_roles(guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild: return []
    return [{"id": str(r.id), "name": r.name} for r in guild.roles if not r.managed and r.name != "@everyone"]

@app.get("/api/guild/{guild_id}/categories")
async def get_categories(guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild: return []
    return [{"id": str(c.id), "name": c.name} for c in guild.categories]

# Replaced by main config endpoint

# --- Üye Yönetimi Endpoints ---

@app.get("/api/guild/{guild_id}/members")
async def get_guild_members(guild_id: int, search: str = ""):
    guild = bot.get_guild(guild_id)
    if not guild:
        raise HTTPException(status_code=404, detail="Sunucu bulunamadı.")
    
    members_data = []
    count = 0
    search_query = search.lower()
    
    for member in guild.members:
        if search_query and search_query not in member.name.lower() and search_query not in (member.nick or "").lower():
            continue
            
        members_data.append({
            "id": str(member.id),
            "name": member.name,
            "display_name": member.display_name,
            "avatar": str(member.display_avatar.url),
            "top_role": member.top_role.name,
            "roles": [{"id": str(r.id), "name": r.name} for r in member.roles[1:][:5]], 
            "is_bot": member.bot,
            "joined_at": member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "Bilinmiyor"
        })
        count += 1
        if count >= 300: break 
        
    return members_data

@app.post("/api/member/{guild_id}/{user_id}/kick")
async def kick_member(guild_id: int, user_id: int, req: MemberActionRequest):
    guild = bot.get_guild(guild_id)
    member = guild.get_member(user_id)
    if not member: raise HTTPException(status_code=404, detail="Üye bulunamadı.")
    
    try:
        await member.kick(reason=req.reason)
        await add_log("moderation-log", 0, "Web Dashboard", f"KICK: {member.name} | Sebep: {req.reason}", guild_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/member/{guild_id}/{user_id}/ban")
async def ban_member(guild_id: int, user_id: int, req: MemberActionRequest):
    guild = bot.get_guild(guild_id)
    member = guild.get_member(user_id)
    if not member: raise HTTPException(status_code=404, detail="Üye bulunamadı.")
    
    try:
        await member.ban(reason=req.reason)
        await add_log("moderation-log", 0, "Web Dashboard", f"BAN: {member.name} | Sebep: {req.reason}", guild_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/member/{guild_id}/{user_id}/timeout")
async def timeout_member(guild_id: int, user_id: int, req: MemberActionRequest):
    guild = bot.get_guild(guild_id)
    member = guild.get_member(user_id)
    if not member: raise HTTPException(status_code=404, detail="Üye bulunamadı.")
    
    if not req.duration: raise HTTPException(status_code=400, detail="Süre belirtilmedi.")
    
    try:
        duration_delta = timedelta(minutes=req.duration)
        await member.timeout(duration_delta, reason=req.reason)
        await add_log("moderation-log", 0, "Web Dashboard", f"TIMEOUT: {member.name} ({req.duration} dk) | Sebep: {req.reason}", guild_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Bot Kontrol Endpoints ---

@app.post("/api/bot/status")
async def update_bot_status(req: BotStatusRequest):
    if not bot.is_ready(): raise HTTPException(status_code=503, detail="Bot hazır değil.")
    
    status_map = {
        "online": discord.Status.online,
        "dnd": discord.Status.dnd,
        "idle": discord.Status.idle,
        "invisible": discord.Status.invisible
    }
    
    activity = None
    if req.activity_name:
        if req.activity_type == "playing":
            activity = discord.Game(name=req.activity_name)
        elif req.activity_type == "streaming":
            activity = discord.Streaming(name=req.activity_name, url="https://twitch.tv/discord")
        elif req.activity_type == "listening":
            activity = discord.Activity(type=discord.ActivityType.listening, name=req.activity_name)
        elif req.activity_type == "watching":
            activity = discord.Activity(type=discord.ActivityType.watching, name=req.activity_name)

    try:
        await bot.change_presence(status=status_map.get(req.status, discord.Status.online), activity=activity)
        bot_config["activity"] = req.activity_name 
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/bot/restart")
async def restart_bot():
    print("🔄 Bot web panel üzerinden yeniden başlatılıyor...")
    asyncio.create_task(perform_restart())
    return {"status": "restarting"}

async def perform_restart():
    await asyncio.sleep(1) 
    await bot.close()
    os.execv(sys.executable, ['python'] + sys.argv)

@app.post("/api/guild/{guild_id}/send_ticket_setup")
async def send_ticket_setup(guild_id: int, channel_id: int):
    ctx_channel = bot.get_channel(channel_id)
    if not ctx_channel:
        raise HTTPException(status_code=404, detail="Kanal bulunamadı.")
    
    from cogs.tickets import TicketView
    config = await get_server_config(guild_id)
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
    
    await ctx_channel.send(embed=embed, view=TicketView())
    return {"status": "success"}

@app.post("/api/guild/{guild_id}/send_ekip_setup")
async def send_ekip_setup(guild_id: int, channel_id: int):
    ctx_channel = bot.get_channel(channel_id)
    if not ctx_channel:
        raise HTTPException(status_code=404, detail="Kanal bulunamadı.")
    
    from cogs.ekip_basvuru import EkipGirisView
    embed = discord.Embed(
        title="Ekip Oluşturma Sistemi",
        description="Ekibinizi kurmak ve sunucuda kendinize özel kanala sahip olmak için aşağıdaki butona basabilirsiniz.",
        color=discord.Color.blue()
    )
    embed.add_field(name="Gereksinimler", value="- Ekip ismi\n- Kişi sayısı\n- Kısa açıklama (Max 50 karakter)", inline=False)
    
    await ctx_channel.send(embed=embed, view=EkipGirisView())
    return {"status": "success"}

@app.post("/api/guild/{guild_id}/send_yayinci_setup")
async def send_yayinci_setup(guild_id: int, channel_id: int):
    ctx_channel = bot.get_channel(channel_id)
    if not ctx_channel:
        raise HTTPException(status_code=404, detail="Kanal bulunamadı.")
    
    from cogs.yayinci import StreamButton
    embed = discord.Embed(
        title="📢 Yayıncı Kontrol Paneli",
        description="Yayına girdiğinde aşağıdaki butona basarak otomatik duyuru yapabilirsin.\n\n👉 **Tüyo:** Butona bastığında açılan pencereye yayın linkini yapıştırman yeterlidir!",
        color=discord.Color.blue()
    )
    await ctx_channel.send(embed=embed, view=StreamButton())
    return {"status": "success"}

@app.get("/api/guild/{guild_id}/active-teams")
async def fetch_active_teams(guild_id: int):
    teams = await get_all_teams(guild_id)
    return teams

@app.post("/api/guild/{guild_id}/delete_team")
async def delete_team_api(guild_id: int, team_id: int):
    teams = await get_all_teams(guild_id)
    team_data = next((t for t in teams if t['id'] == team_id), None)
    
    if not team_data:
        raise HTTPException(status_code=404, detail="Ekip bulunamadı.")
    
    guild = bot.get_guild(guild_id)
    if not guild:
         raise HTTPException(status_code=404, detail="Sunucu bulunamadı.")

    # Delete Roles
    try:
        for role_key in ['boss_role_id', 'og_role_id', 'normal_role_id']:
            if team_data.get(role_key):
                role = guild.get_role(int(team_data[role_key]))
                if role: await role.delete()
    except Exception as e:
        print(f"Role deletion error: {e}")

    # Delete Channel
    try:
        if team_data.get('channel_id'):
            channel = guild.get_channel(int(team_data['channel_id']))
            if channel: await channel.delete()
    except Exception as e:
        print(f"Channel deletion error: {e}")

    # Delete from DB
    await delete_ekip_team(team_id)
    
    # Log the action
    await add_log("ekip-log", "SYSTEM", "Web Panel", f"EKİP SİLİNDİ (Panel): {team_data['ekip_ismi']}", guild_id)
    
    return {"status": "success"}


# --- AUTH & TOKEN SYSTEM ---

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    print(f"🔑 GİRİŞ DENEMESİ: Token='{req.token}'")
    token_data = await validate_token(req.token)
    if token_data:
        print(f"✅ GİRİŞ BAŞARILI: Role={token_data['role']}")
        return {"status": "success", "role": token_data['role'], "token": req.token}
    print(f"❌ GİRİŞ BAŞARISIZ: Geçersiz token!")
    raise HTTPException(status_code=401, detail="Geçersiz token.")

@app.get("/api/auth/me")
async def check_auth(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Yetkisiz erişim.")
    
    token = auth_header.split(" ")[1]
    token_data = await validate_token(token)
    if token_data:
        return {"role": token_data['role']}
    raise HTTPException(status_code=401, detail="Geçersiz veya süresi dolmuş token.")

# --- LIVE STATS API ---
@app.get("/api/guild/{guild_id}/live-stats")
async def get_live_stats(guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild: return {"online": 0, "voice": 0, "total": 0}
    
    online = len([m for m in guild.members if m.status != discord.Status.offline])
    voice = sum(len(c.members) for c in guild.voice_channels)
    
    return {
        "online": online,
        "voice": voice,
        "total": guild.member_count
    }

# --- AUTOMOD API ---
@app.post("/api/guild/{guild_id}/automod")
async def set_automod(guild_id: int, req: ConfigUpdateRequest):
    from database import update_automod_config
    await update_automod_config(
        guild_id, 
        links=req.automod_links, 
        spam=req.automod_spam, 
        words=req.automod_words
    )
    return {"status": "success"}

# --- INVITE API ---
@app.get("/api/guild/{guild_id}/invites")
async def fetch_invite_leaderboard(guild_id: int):
    from database import get_invite_leaderboard
    return await get_invite_leaderboard(guild_id)

# --- GIVEAWAY API ---
@app.get("/api/guild/{guild_id}/giveaways")
async def fetch_active_giveaways_api(guild_id: int):
    from database import get_active_giveaways
    all_active = await get_active_giveaways()
    # Sadece bu sunucuya ait olanları filtrele
    return [g for g in all_active if str(g['guild_id']) == str(guild_id)]

@app.post("/api/guild/{guild_id}/giveaways")
async def start_giveaway_api(guild_id: int, req: GiveawayRequest):
    cog = bot.get_cog("Giveaway")
    if not cog: raise HTTPException(status_code=500, detail="Giveaway modülü yüklü değil.")
    
    success, msg = await cog.start_giveaway(guild_id, req.channel_id, req.duration, req.winners, req.prize, 0, "Web Dashboard")
    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"status": "success"}

@app.get("/api/tokens")
async def list_tokens(request: Request):
    # Basit bir check: Header'dan gelen token owner mı?
    auth_header = request.headers.get("Authorization")
    if not auth_header: raise HTTPException(status_code=401)
    token = auth_header.split(" ")[1]
    token_data = await validate_token(token)
    
    if not token_data or token_data['role'] != 'owner':
        raise HTTPException(status_code=403, detail="Yalnızca kurucu yetkisi gerekir.")
    
    return await get_all_tokens()

@app.post("/api/tokens/generate")
async def generate_token_api(req: TokenGenerateRequest, request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header: raise HTTPException(status_code=401)
    token = auth_header.split(" ")[1]
    token_data = await validate_token(token)
    
    if not token_data or token_data['role'] != 'owner':
        raise HTTPException(status_code=403, detail="Yalnızca kurucu yetkisi gerekir.")
    
    new_token = f"{req.role}-{secrets.token_hex(8)}"
    await add_token(new_token, req.role)
    return {"status": "success", "token": new_token}

@app.delete("/api/tokens/{token_to_delete}")
async def delete_token_api(token_to_delete: str, request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header: raise HTTPException(status_code=401)
    token = auth_header.split(" ")[1]
    token_data = await validate_token(token)
    
    if not token_data or token_data['role'] != 'owner':
        raise HTTPException(status_code=403, detail="Yalnızca kurucu yetkisi gerekir.")
    
    if token == token_to_delete:
        raise HTTPException(status_code=400, detail="Kendi tokeninizi silemezsiniz.")
        
    await delete_token(token_to_delete)
    return {"status": "success"}



@app.get("/api/guild/{guild_id}/members")

async def get_members(guild_id: int):
    if not bot.is_ready():
        return []
    guild = bot.get_guild(guild_id)
    if not guild:
         raise HTTPException(status_code=404, detail="Sunucu bulunamadı.")
    
    # Not: Büyük sunucularda limitli çekmek gerekebilir.
    members = []
    try:
        # Sunucudaki tüm üyeleri çekiyoruz
        async for member in guild.fetch_members(limit=1000):
            members.append({
                "id": str(member.id),
                "name": f"{member.name}#{member.discriminator}" if member.discriminator != "0" else member.name,
                "avatar": str(member.display_avatar.url),
                "joined_at": member.joined_at.isoformat() if member.joined_at else None,
                "top_role": member.top_role.name
            })
    except Exception as e:
        print(f"Member fetch error: {e}")
        raise HTTPException(status_code=500, detail="Üyeler çekilemedi. Intent açık mı?")
    
    return members

@app.post("/api/guild/{guild_id}/kick")
async def kick_member(guild_id: int, user_id: int):
    guild = bot.get_guild(guild_id)
    if not guild:
        raise HTTPException(status_code=404, detail="Sunucu bulunamadı.")
    
    try:
        member = await guild.fetch_member(user_id)
        await member.kick(reason="Web Panel üzerinden atıldı.")
        
        return {"status": "success"}
    except Exception as e:

        raise HTTPException(status_code=403, detail=f"Yetki hatası: {str(e)}")

@app.post("/api/guild/{guild_id}/ban")
async def ban_member(guild_id: int, user_id: int):
    guild = bot.get_guild(guild_id)
    if not guild:
        raise HTTPException(status_code=404, detail="Sunucu bulunamadı.")
    
    try:
        # Ban işlemi için kullanıcı ismini çekmeye çalışalım
        username = "Bilinmeyen Kullanıcı"
        try:
            user = await bot.fetch_user(user_id)
            username = user.name
        except: pass

        await guild.ban(discord.Object(id=user_id), reason="Web Panel üzerinden yasaklandı.")
        
        return {"status": "success"}
    except Exception as e:

        raise HTTPException(status_code=403, detail=f"Yetki hatası: {str(e)}")

@app.get("/api/settings")

async def get_settings():
    return bot_config

@app.get("/api/logs")
async def get_bot_logs(type: str = None):
    return await get_logs(log_type=type)

@app.get("/api/log/{log_id}")
async def get_single_log(log_id: int):
    logs = await get_logs() # Simple way, but I should probably optimize
    for log in logs:
        if log['id'] == log_id:
            return log
    raise HTTPException(status_code=404, detail="Log bulunamadı.")


@app.get("/api/channel/{channel_id}/messages")
async def get_channel_messages(channel_id: int):
    if not bot.is_ready():
        raise HTTPException(status_code=503, detail="Bot hazır değil.")
    
    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            channel = await bot.fetch_channel(channel_id)
        
        messages = []
        async for msg in channel.history(limit=100, oldest_first=True):
            # Yetkili tespiti: manage_messages yetkisi olanları yetkili sayıyoruz
            is_staff = False
            if isinstance(msg.author, discord.Member):
                is_staff = msg.author.guild_permissions.manage_messages
            elif msg.author.id == bot.user.id:
                 is_staff = True # Botun kendisi de yetkili sayılabilir loglarda

            messages.append({
                "id": str(msg.id),
                "author": msg.author.display_name,
                "avatar": str(msg.author.display_avatar.url),
                "content": msg.content,
                "timestamp": msg.created_at.strftime("%H:%M"),
                "is_bot": msg.author.bot,
                "is_staff": is_staff
            })
        return messages
    except Exception as e:
        raise HTTPException(status_code=403, detail=f"Kanal okunamadı: {str(e)}")

@app.post("/api/settings")
async def update_settings(req: SettingsRequest):
    bot_config["prefix"] = req.prefix
    bot_config["activity"] = req.activity
    bot.command_prefix = req.prefix
    if bot.is_ready():
        await bot.change_presence(activity=discord.Game(name=req.activity))
    return {"status": "success"}

# --- SERVER CONFIG & CHANNELS ---
@app.get("/api/guild/{guild_id}/channels")
async def get_guild_channels(guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild: return []
    return [{"id": str(c.id), "name": c.name} for c in guild.text_channels]

@app.get("/api/guild/{guild_id}/config")
async def get_config(guild_id: int):
    config = await get_server_config(guild_id)
    return config or {}

@app.post("/api/guild/{guild_id}/config")
async def update_config(guild_id: int, req: ConfigUpdateRequest):
    await update_server_channels(
        guild_id, 
        req.rules_channel, 
        req.suggestions_channel, 
        req.applications_channel,
        req.ticket_category,
        req.ticket_log_channel,
        req.ticket_staff_role,
        req.ticket_logo_url,
        req.ekip_category,
        req.ekip_staff_role,
        req.ekip_log_channel,
        req.yayinci_role,
        req.uyari_log_channel,
        req.uyari_staff_role,
        req.automod_links,
        req.automod_spam,
        req.automod_words
    )
    return {"status": "success"}

# --- RULES INTEGRATION ---
@app.get("/api/guild/{guild_id}/rules")
async def fetch_rules(guild_id: int):
    return await get_rules(guild_id)

@app.post("/api/guild/{guild_id}/rules")
async def post_rule(guild_id: int, req: RuleRequest):
    await add_rule(guild_id, req.title, req.content)
    
    # Discord'a Gönder
    config = await get_server_config(guild_id)
    if config and config['rules_channel']:
        channel = bot.get_channel(int(config['rules_channel']))
        if channel:
            embed = discord.Embed(
                title=f"⚖️ {req.title.upper()}",
                description=f"\n{req.content}\n\n",
                color=0x2f3136 # Koyu Gri / Modern Discord Teması
            )
            embed.set_author(name="Hüküm RolePlay - Sunucu Kuralları", icon_url=bot.user.display_avatar.url)
            embed.set_footer(text="Huzurlu bir ortam için lütfen kurallara uyunuz.", icon_url=bot.user.display_avatar.url)
            
            # Kuralın önemini vurgulayan küçük bir not
            embed.add_field(name="⚠️ Unutmayın", value="Kurallara uymamak ağır yaptırımlara neden olabilir.", inline=False)
            
            await channel.send(embed=embed)
    
    return {"status": "success"}

# --- SUGGESTIONS INTEGRATION ---
@app.get("/api/guild/{guild_id}/suggestions")
async def fetch_suggestions(guild_id: int):
    results = await get_suggestion_list(guild_id)
    return results

@app.get("/api/guild/{guild_id}/applications")
async def fetch_applications(guild_id: int):
    results = await get_application_list(guild_id)
    return results

@app.post("/api/suggestion/{suggestion_id}/approve")
async def approve_suggestion(suggestion_id: int):
    await update_suggestion_status(suggestion_id, 'approved')
    return {"status": "success"}

@app.post("/api/suggestion/{suggestion_id}/reject")
async def reject_suggestion(suggestion_id: int):
    await update_suggestion_status(suggestion_id, 'rejected')
    return {"status": "success"}

# --- APPLICATION MODAL ---
class ApplyModal(discord.ui.Modal, title='Yetkili Başvuru Formu'):
    name = discord.ui.TextInput(label='Adınız ve Yaşınız', placeholder='Örn: Ahmet, 18', min_length=2, max_length=50)
    fivem_knowledge = discord.ui.TextInput(label='Fivem Bilgin', style=discord.TextStyle.paragraph, placeholder='Bize Fivem Bilginden bahset...', min_length=5, max_length=1000)
    reason = discord.ui.TextInput(label='Neden Yetkili Olmak İstiyorsunuz?', style=discord.TextStyle.paragraph, placeholder='Tecrübelerinizden bahsedin...', min_length=10, max_length=1000)

    async def on_submit(self, interaction: discord.Interaction):
        content = f"**Ad/Yaş:** {self.name.value}\n**Fivem Bilgi:** {self.fivem_knowledge.value}\n**Neden:** {self.reason.value}"
        await add_application(interaction.guild_id, interaction.user.id, content, "staff", interaction.id)
        
        await interaction.response.send_message(f'✅ Başvurunuz başarıyla alındı, {interaction.user.mention}!', ephemeral=True)

class ApplyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Butonun kalıcı olması için

    @discord.ui.button(label='Başvuru Yap', style=discord.ButtonStyle.success, custom_id='apply_button', emoji='📑')
    async def apply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ApplyModal())

@app.post("/api/guild/{guild_id}/send_apply")
async def send_apply_form(guild_id: int):
    config = await get_server_config(guild_id)
    if not config or not config.get('applications_channel'):
        raise HTTPException(status_code=400, detail="Önce başvuru kanalı seçilmelidir.")
    
    channel = bot.get_channel(int(config['applications_channel']))
    if not channel:
        raise HTTPException(status_code=404, detail="Kanal bulunamadı.")
    
    embed = discord.Embed(
        title="✨ AİLEMİZE KATILMAK İSTER MİSİN? ✨",
        description=(
            "🚀 **Profesyonel ve Enerjik Bir Ekip Seni Bekliyor!**\n\n"
            "Sunucumuzu daha ileriye taşımak için yeni yetkililer arıyoruz. Eğer sen de bu heyecana ortak olmak istiyorsan doğru yerdesin!\n\n"
            "🛡️ **Neler Bekliyoruz?**\n"
            "• Sorumluluk bilinci ve adil yaklaşım\n"
            "• Aktif katılım ve pozitif iletişim\n"
            "• Kurallara tam bağlılık\n\n"
            "💡 **Nasıl Başvurulur?**\n"
            "Aşağıdaki **Başvuru Yap** butonuna basarak kısa formumuzu doldurabilirsin. Ekibimiz en kısa sürede seninle iletişime geçecektir."
        ),
        color=0x5865f2 # Discord Blurple
    )
    embed.set_image(url="https://i.imgur.com/vH9v5Zt.png") # Opsiyonel: Şık bir banner
    embed.set_footer(text="CodeX Academy • Başvuru Sistemi", icon_url=bot.user.display_avatar.url)
    
    await channel.send(embed=embed, view=ApplyView())
    return {"status": "success"}

# --- APPLICATION ACTIONS ---
@app.post("/api/application/{app_id}/approve")
async def approve_app(app_id: int):
    app = await get_application_by_id(app_id)
    if not app: raise HTTPException(status_code=404, detail="Başvuru bulunamadı.")
    
    await update_application_status(app_id, 'approved')
    
    # Kullanıcıya DM Gönder
    try:
        user = await bot.fetch_user(int(app['user_id']))
        embed = discord.Embed(
            title="✅ Başvurunuz Onaylandı!",
            description="Tebrikler! Yapmış olduğunuz yetkili başvurusu yönetim ekibimiz tarafından **onaylanmıştır**.\n\nEn kısa sürede yetkileriniz tanımlanacaktır.",
            color=discord.Color.green()
        )
        await user.send(embed=embed)
    except Exception as e: print(f"DM Error: {e}")
    
    return {"status": "success"}

@app.post("/api/application/{app_id}/reject")
async def reject_app(app_id: int):
    app = await get_application_by_id(app_id)
    if not app: raise HTTPException(status_code=404, detail="Başvuru bulunamadı.")
    
    await update_application_status(app_id, 'rejected')
    
    # Kullanıcıya DM Gönder
    try:
        user = await bot.fetch_user(int(app['user_id']))
        embed = discord.Embed(
            title="❌ Başvurunuz Reddedildi",
            description="Üzgünüz, yapmış olduğunuz yetkili başvurusu şu an için **reddedilmiştir**.\n\nDaha sonra tekrar başvurmayı deneyebilirsiniz.",
            color=discord.Color.red()
        )
        await user.send(embed=embed)
    except Exception as e: print(f"DM Error: {e}")
    
    return {"status": "success"}

@app.post("/api/send_message")
async def send_message(req: MessageRequest):
    if not bot.is_ready():
        raise HTTPException(status_code=503, detail="Bot henüz hazır değil.")
    
    try:
        channel = bot.get_channel(int(req.channel_id))
        if not channel:
            channel = await bot.fetch_channel(int(req.channel_id))
        
        await channel.send(req.content)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/update_status")
async def update_status(req: StatusRequest):
    if not bot.is_ready():
        raise HTTPException(status_code=503, detail="Bot henüz hazır değil.")
    
    status_map = {
        "online": discord.Status.online,
        "idle": discord.Status.idle,
        "dnd": discord.Status.dnd,
        "invisible": discord.Status.invisible
    }
    
    try:
        await bot.change_presence(status=status_map.get(req.status, discord.Status.online))
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- AUTO RESPONDERS ---
@app.get("/api/guild/{guild_id}/auto-responders")
async def fetch_auto_responders(guild_id: int):
    return await get_auto_responders(guild_id)

@app.post("/api/guild/{guild_id}/auto-responders")
async def create_auto_responder(guild_id: int, req: AutoResponderRequest):
    if not req.keyword or not req.response:
        raise HTTPException(status_code=400, detail="Kelime ve cevap boş olamaz.")
    await add_auto_responder(guild_id, req.keyword, req.response)
    return {"status": "success"}

@app.delete("/api/auto-responders/{responder_id}")
async def remove_auto_responder(responder_id: int):
    await delete_auto_responder(responder_id)
    return {"status": "success"}

@app.post("/api/guild/{guild_id}/send_embed")
async def send_embed_api(guild_id: int, req: EmbedRequest):
    if not bot.is_ready():
        raise HTTPException(status_code=503, detail="Bot hazır değil.")
    
    try:
        channel = bot.get_channel(int(req.channel_id))
        if not channel:
            channel = await bot.fetch_channel(int(req.channel_id))
        
        # Color parsing
        color_hex = req.color.lstrip('#')
        color_int = int(color_hex, 16)
        
        embed = discord.Embed(
            title=req.title if req.title else None,
            description=req.description if req.description else None,
            color=color_int
        )
        
        if req.image_url: embed.set_image(url=req.image_url)
        if req.thumbnail_url: embed.set_thumbnail(url=req.thumbnail_url)
        if req.author_name: embed.set_author(name=req.author_name)
        if req.footer_text: embed.set_footer(text=req.footer_text)
        
        await channel.send(embed=embed)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Static files and Frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")

# --- CONCURRENT RUNNER ---
async def run_app():
    await init_db() # Uygulama başlamadan önce veritabanını kesin olarak kur
    
    # Bootstrap: Eğer hiç token yoksa bir owner token oluştur
    tokens = await get_all_tokens()
    if not tokens:
        bootstrap_token = "owner-codex-admin"
        await add_token(bootstrap_token, "owner")
        print(f"\n" + "="*50)
        print(f"🚀 BOOTSTRAP: İlk Kurucu Tokeni Oluşturuldu!")
        print(f"Token: {bootstrap_token}")
        print(f"Lütfen bu tokeni not edin, siteye giriş için kullanacaksınız.")
        print("="*50 + "\n")

    # Token ve Port temizliği
    CLEAN_TOKEN = TOKEN.strip() if TOKEN else None
    if not CLEAN_TOKEN:
        print("❌ HATA: DISCORD_TOKEN bulunamadı! Lütfen deployment (Render vb.) panelinden Environment Variables kısmına DISCORD_TOKEN ekleyin.")
        sys.exit(1)

    # 0.0.0.0 allows external connections (Render, Heroku vb. platformlarda çalışması için 0.0.0.0 olmalıdır)
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info", loop="asyncio")
    server = uvicorn.Server(config)
    
    # Run both concurrently
    print("🚀 Bot ve Web Sunucusu başlatılıyor (Selector Loop)...")
    await asyncio.gather(
        server.serve(),
        bot.start(CLEAN_TOKEN)
    )

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # Selector loop DNS hatalarını ve WinError 10014 hatasını önlemek için tekrar aktifleştirildi
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        pass



