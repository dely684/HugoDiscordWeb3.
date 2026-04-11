import asyncpg
import datetime
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Global pool
pool = None

async def init_db():
    global pool
    if not pool:
        try:
            pool = await asyncpg.create_pool(DATABASE_URL)
            print("✅ PostgreSQL Bağlantı Havuzu Oluşturuldu.")
        except Exception as e:
            print(f"❌ PostgreSQL Bağlantı Hatası: {e}")
            return

    async with pool.acquire() as conn:
        # Loglar Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                timestamp TEXT,
                type TEXT,
                user_id TEXT,
                username TEXT,
                content TEXT,
                guild_id TEXT
            )
        """)
        # Ekonomi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS economy (
                user_id TEXT,
                guild_id TEXT,
                wallet INTEGER DEFAULT 0,
                bank INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, guild_id)
            )
        """)
        # Uyarı Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS warnings (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                guild_id TEXT,
                moderator_id TEXT,
                reason TEXT,
                timestamp TEXT
            )
        """)
        # Sunucu Ayarları Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_config (
                guild_id TEXT PRIMARY KEY,
                log_channel TEXT,
                rules_channel TEXT,
                suggestions_channel TEXT,
                suggestions_log_channel TEXT,
                applications_channel TEXT,
                ticket_category TEXT,
                ticket_log_channel TEXT,
                ticket_staff_role TEXT,
                ui_update_channel TEXT,
                ticket_logo_url TEXT,
                ekip_category TEXT,
                ekip_staff_role TEXT,
                ekip_log_channel TEXT,
                yayinci_channel TEXT,
                yayinci_role TEXT,
                uyari_log_channel TEXT,
                uyari_staff_role TEXT,
                automod_links INTEGER DEFAULT 0,
                automod_spam INTEGER DEFAULT 0,
                automod_words TEXT
            )
        """)
        # Kurallar Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                title TEXT,
                content TEXT
            )
        """)
        # Öneriler Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS suggestions (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                user_id TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                message_id TEXT
            )
        """)
        # Başvurular Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                user_id TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                type TEXT,
                message_id TEXT
            )
        """)
        # Ekip Sistemi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ekip_teams (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                ekip_ismi TEXT,
                boss_role_id TEXT,
                og_role_id TEXT,
                normal_role_id TEXT,
                channel_id TEXT,
                leader_id TEXT
            )
        """)
        # Yayıncı Sistemi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS yayinci (
                user_id TEXT PRIMARY KEY,
                custom_message TEXT
            )
        """)
        # Token Sistemi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                role TEXT,
                created_at TEXT,
                used_by TEXT DEFAULT NULL
            )
        """)
        # Otomatik Cevaplayıcı Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auto_responders (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                keyword TEXT,
                response TEXT
            )
        """)
        # Davet Sistemi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS invites (
                guild_id TEXT,
                inviter_id TEXT,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, inviter_id)
            )
        """)
        # Çekiliş Sistemi Tablosu
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                guild_id TEXT,
                channel_id TEXT,
                message_id TEXT,
                prize TEXT,
                winners INTEGER,
                end_time TEXT,
                status TEXT DEFAULT 'active'
            )
        """)

        # İstatistik Tablosu (Analizler için)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT,
                guild_id TEXT,
                joins INTEGER DEFAULT 0,
                leaves INTEGER DEFAULT 0,
                messages INTEGER DEFAULT 0,
                PRIMARY KEY (date, guild_id)
            )
        """)
        print("✅ Veritabanı tabloları kontrol edildi/oluşturuldu.")

# --- HELPERS ---
async def get_conn():
    global pool
    if not pool:
        await init_db()
    return pool

# --- LOGGING ---
async def add_log(log_type, user_id, username, content, guild_id=None):
    pool = await get_conn()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO logs (timestamp, type, user_id, username, content, guild_id) VALUES ($1, $2, $3, $4, $5, $6)",
            timestamp, log_type, str(user_id), username, content, str(guild_id) if guild_id else None
        )

async def get_logs(log_type=None, limit=100):
    pool = await get_conn()
    async with pool.acquire() as conn:
        if log_type:
            rows = await conn.fetch(
                "SELECT * FROM logs WHERE type = $1 ORDER BY id DESC LIMIT $2",
                log_type, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM logs ORDER BY id DESC LIMIT $1",
                limit
            )
        return [dict(row) for row in rows]

# --- ECONOMY ---
async def get_balance(user_id, guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT wallet, bank FROM economy WHERE user_id = $1 AND guild_id = $2",
            str(user_id), str(guild_id)
        )
        if row: return dict(row)
        
        # İlk kez ise oluştur
        await conn.execute(
            "INSERT INTO economy (user_id, guild_id, wallet, bank) VALUES ($1, $2, $3, $4)",
            str(user_id), str(guild_id), 100, 0
        )
        return {"wallet": 100, "bank": 0}

async def update_wallet(user_id, guild_id, amount):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await get_balance(user_id, guild_id) # Varlığı garanti et
        await conn.execute(
            "UPDATE economy SET wallet = wallet + $1 WHERE user_id = $2 AND guild_id = $3",
            amount, str(user_id), str(guild_id)
        )

# --- MODERATION ---
async def add_warn(user_id, guild_id, mod_id, reason):
    pool = await get_conn()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO warnings (user_id, guild_id, moderator_id, reason, timestamp) VALUES ($1, $2, $3, $4, $5)",
            str(user_id), str(guild_id), str(mod_id), reason, timestamp
        )

async def get_warns(user_id, guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM warnings WHERE user_id = $1 AND guild_id = $2",
            str(user_id), str(guild_id)
        )
        return [dict(row) for row in rows]

async def get_all_warns(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM warnings WHERE guild_id = $1 ORDER BY timestamp DESC", str(guild_id))
        return [dict(row) for row in rows]

# --- RULES ---
async def add_rule(guild_id, title, content):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO rules (guild_id, title, content) VALUES ($1, $2, $3)",
            str(guild_id), title, content
        )

async def get_rules(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rules WHERE guild_id = $1", str(guild_id))
        return [dict(row) for row in rows]

# --- SUGGESTIONS & APPLICATIONS ---
async def add_suggestion(guild_id, user_id, content, message_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO suggestions (guild_id, user_id, content, message_id) VALUES ($1, $2, $3, $4)",
            str(guild_id), str(user_id), content, str(message_id)
        )

async def update_suggestion_status(suggestion_id, status):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE suggestions SET status = $1 WHERE id = $2", status, int(suggestion_id))

async def add_application(guild_id, user_id, content, app_type, message_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO applications (guild_id, user_id, content, type, message_id) VALUES ($1, $2, $3, $4, $5)",
            str(guild_id), str(user_id), content, app_type, str(message_id)
        )

async def get_suggestion_list(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM suggestions WHERE guild_id = $1 ORDER BY id DESC", str(guild_id))
        return [dict(row) for row in rows]

async def get_application_list(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM applications WHERE guild_id = $1 ORDER BY id DESC", str(guild_id))
        return [dict(row) for row in rows]

async def update_application_status(app_id, status):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE applications SET status = $1 WHERE id = $2", status, int(app_id))

async def get_application_by_id(app_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM applications WHERE id = $1", int(app_id))
        return dict(row) if row else None

# --- EKİP SİSTEMİ ---
async def add_ekip_team(guild_id, ekip_ismi, boss_role_id, og_role_id, normal_role_id, channel_id, leader_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ekip_teams (guild_id, ekip_ismi, boss_role_id, og_role_id, normal_role_id, channel_id, leader_id) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            str(guild_id), ekip_ismi, str(boss_role_id), str(og_role_id), str(normal_role_id), str(channel_id), str(leader_id)
        )

async def get_all_teams(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM ekip_teams WHERE guild_id = $1", str(guild_id))
        return [dict(row) for row in rows]

async def get_team(guild_id, ekip_ismi):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM ekip_teams WHERE guild_id = $1 AND ekip_ismi = $2", str(guild_id), ekip_ismi)
        return dict(row) if row else None

async def get_ekip_team_by_channel(channel_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM ekip_teams WHERE channel_id = $1", str(channel_id))
        return dict(row) if row else None

async def delete_ekip_team(team_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM ekip_teams WHERE id = $1", int(team_id))

# --- YAYINCI SİSTEMİ ---
async def set_yayinci_message(user_id, message):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO yayinci (user_id, custom_message) VALUES ($1, $2) ON CONFLICT(user_id) DO UPDATE SET custom_message=EXCLUDED.custom_message",
            str(user_id), message
        )

async def get_yayinci_message(user_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT custom_message FROM yayinci WHERE user_id = $1", str(user_id))
        return row['custom_message'] if row else "Yayındayım, hepinizi bekliyorum!"

# --- CONFIG ---
async def update_server_channels(guild_id, rules=None, suggestions=None, suggestions_log=None, apps=None, ticket_category=None, ticket_log=None, ticket_staff=None, ticket_logo=None,
                                 ekip_category=None, ekip_staff_role=None, ekip_log_channel=None, yayinci_channel=None, yayinci_role=None, uyari_log_channel=None, uyari_staff_role=None):
    pool = await get_conn()
    async with pool.acquire() as conn:
        # Önce kaydın varlığını kontrol et
        row = await conn.fetchrow("SELECT 1 FROM server_config WHERE guild_id = $1", str(guild_id))
        if not row:
            await conn.execute("INSERT INTO server_config (guild_id) VALUES ($1)", str(guild_id))
        
        if rules: await conn.execute("UPDATE server_config SET rules_channel = $1 WHERE guild_id = $2", str(rules), str(guild_id))
        if suggestions: await conn.execute("UPDATE server_config SET suggestions_channel = $1 WHERE guild_id = $2", str(suggestions), str(guild_id))
        if suggestions_log: await conn.execute("UPDATE server_config SET suggestions_log_channel = $1 WHERE guild_id = $2", str(suggestions_log), str(guild_id))
        if apps: await conn.execute("UPDATE server_config SET applications_channel = $1 WHERE guild_id = $2", str(apps), str(guild_id))
        if ticket_category: await conn.execute("UPDATE server_config SET ticket_category = $1 WHERE guild_id = $2", str(ticket_category), str(guild_id))
        if ticket_log: await conn.execute("UPDATE server_config SET ticket_log_channel = $1 WHERE guild_id = $2", str(ticket_log), str(guild_id))
        if ticket_staff: await conn.execute("UPDATE server_config SET ticket_staff_role = $1 WHERE guild_id = $2", str(ticket_staff), str(guild_id))
        if ticket_logo: await conn.execute("UPDATE server_config SET ticket_logo_url = $1 WHERE guild_id = $2", str(ticket_logo), str(guild_id))
        if ekip_category: await conn.execute("UPDATE server_config SET ekip_category = $1 WHERE guild_id = $2", str(ekip_category), str(guild_id))
        if ekip_staff_role: await conn.execute("UPDATE server_config SET ekip_staff_role = $1 WHERE guild_id = $2", str(ekip_staff_role), str(guild_id))
        if ekip_log_channel: await conn.execute("UPDATE server_config SET ekip_log_channel = $1 WHERE guild_id = $2", str(ekip_log_channel), str(guild_id))
        if yayinci_channel: await conn.execute("UPDATE server_config SET yayinci_channel = $1 WHERE guild_id = $2", str(yayinci_channel), str(guild_id))
        if yayinci_role: await conn.execute("UPDATE server_config SET yayinci_role = $1 WHERE guild_id = $2", str(yayinci_role), str(guild_id))
        if uyari_log_channel: await conn.execute("UPDATE server_config SET uyari_log_channel = $1 WHERE guild_id = $2", str(uyari_log_channel), str(guild_id))
        if uyari_staff_role: await conn.execute("UPDATE server_config SET uyari_staff_role = $1 WHERE guild_id = $2", str(uyari_staff_role), str(guild_id))

async def update_automod_config(guild_id, links=None, spam=None, words=None):
    pool = await get_conn()
    async with pool.acquire() as conn:
        if links is not None: await conn.execute("UPDATE server_config SET automod_links = $1 WHERE guild_id = $2", int(links), str(guild_id))
        if spam is not None: await conn.execute("UPDATE server_config SET automod_spam = $1 WHERE guild_id = $2", int(spam), str(guild_id))
        if words is not None: await conn.execute("UPDATE server_config SET automod_words = $1 WHERE guild_id = $2", words, str(guild_id))

# --- GIVEAWAY SYSTEM ---
async def add_giveaway(guild_id, channel_id, message_id, prize, winners, end_time):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO giveaways (guild_id, channel_id, message_id, prize, winners, end_time) VALUES ($1, $2, $3, $4, $5, $6)",
            str(guild_id), str(channel_id), str(message_id), prize, int(winners), end_time.strftime("%Y-%m-%d %H:%M:%S")
        )

async def get_active_giveaways():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM giveaways WHERE status = 'active'")
        return [dict(row) for row in rows]

async def update_giveaway_status(giveaway_id, status):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET status = $1 WHERE id = $2", status, int(giveaway_id))

# --- INVITE SYSTEM ---
async def update_invite_count(guild_id, inviter_id, amount=1):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO invites (guild_id, inviter_id, count) 
            VALUES ($1, $2, $3)
            ON CONFLICT(guild_id, inviter_id) DO UPDATE SET count = invites.count + $4
        """, str(guild_id), str(inviter_id), amount, amount)

async def get_invite_leaderboard(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM invites WHERE guild_id = $1 ORDER BY count DESC LIMIT 10", str(guild_id))
        return [dict(row) for row in rows]

async def get_server_config(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM server_config WHERE guild_id = $1", str(guild_id))
        return dict(row) if row else None

# --- TOKEN SYSTEM ---
async def add_token(token, role):
    pool = await get_conn()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tokens (token, role, created_at) VALUES ($1, $2, $3)",
            token, role, timestamp
        )

async def get_all_tokens():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM tokens ORDER BY created_at DESC")
        return [dict(row) for row in rows]

async def delete_token(token):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tokens WHERE token = $1", token)

async def validate_token(token):
    try:
        pool = await get_conn()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tokens WHERE token = $1", token)
            return dict(row) if row else None
    except Exception as e:
        print(f"❌ Token doğrulama hatası (DB): {e}")
        return None

# --- AUTO RESPONDER ---
async def add_auto_responder(guild_id, keyword, response):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO auto_responders (guild_id, keyword, response) VALUES ($1, $2, $3)",
            str(guild_id), keyword.lower(), response
        )

async def get_auto_responders(guild_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM auto_responders WHERE guild_id = $1", str(guild_id))
        return [dict(row) for row in rows]

async def delete_auto_responder(responder_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM auto_responders WHERE id = $1", int(responder_id))

async def find_auto_response(guild_id, keyword):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT response FROM auto_responders WHERE guild_id = $1 AND keyword = $2",
            str(guild_id), keyword.lower()
        )
        return row['response'] if row else None

# --- ANALYTICS ---
async def increment_stat(guild_id, stat_type):
    pool = await get_conn()
    async with pool.acquire() as conn:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        # PostgreSQL specific UPSERT (using dynamic mapping for column name is tricky with placeholders, 
        # but here we know stat_type is a trusted internal string)
        sql = f"""
            INSERT INTO daily_stats (date, guild_id, {stat_type}) 
            VALUES ($1, $2, 1)
            ON CONFLICT(date, guild_id) DO UPDATE SET {stat_type} = daily_stats.{stat_type} + 1
        """
        await conn.execute(sql, today, str(guild_id))

async def get_analytics_data(guild_id, days=7):
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM daily_stats 
            WHERE guild_id = $1 
            ORDER BY date DESC LIMIT $2
        """, str(guild_id), days)
        data = [dict(row) for row in rows]
        data.reverse()
        return data
