import aiosqlite
import datetime
import os

DB_PATH = "bot_database.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Loglar Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                type TEXT,
                user_id TEXT,
                username TEXT,
                content TEXT,
                guild_id TEXT
            )
        """)
        # Ekonomi Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS economy (
                user_id TEXT,
                guild_id TEXT,
                wallet INTEGER DEFAULT 0,
                bank INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, guild_id)
            )
        """)
        # Uyarı Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                guild_id TEXT,
                moderator_id TEXT,
                reason TEXT,
                timestamp TEXT
            )
        """)
        # Sunucu Ayarları Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS server_config (
                guild_id TEXT PRIMARY KEY,
                log_channel TEXT,
                rules_channel TEXT,
                suggestions_channel TEXT,
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                title TEXT,
                content TEXT
            )
        """)
        # Öneriler Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS suggestions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                user_id TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                message_id TEXT
            )
        """)
        # Başvurular Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                user_id TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                type TEXT,
                message_id TEXT
            )
        """)
        # Ekip Sistemi Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ekip_teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS yayinci (
                user_id TEXT PRIMARY KEY,
                custom_message TEXT
            )
        """)
        # Token Sistemi Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                role TEXT,
                created_at TEXT,
                used_by TEXT DEFAULT NULL
            )
        """)
        # Otomatik Cevaplayıcı Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auto_responders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                keyword TEXT,
                response TEXT
            )
        """)
        # Davet Sistemi Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS invites (
                guild_id TEXT,
                inviter_id TEXT,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, inviter_id)
            )
        """)
        # Çekiliş Sistemi Tablosu
        await db.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT,
                channel_id TEXT,
                message_id TEXT,
                prize TEXT,
                winners INTEGER,
                end_time TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        await db.commit()

# --- LOGGING ---
async def add_log(log_type, user_id, username, content, guild_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await db.execute(
            "INSERT INTO logs (timestamp, type, user_id, username, content, guild_id) VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, log_type, str(user_id), username, content, str(guild_id) if guild_id else None)
        )
        await db.commit()

async def get_logs(log_type=None, limit=100):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if log_type:
            cursor = await db.execute(
                "SELECT * FROM logs WHERE type = ? ORDER BY id DESC LIMIT ?",
                (log_type, limit)
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM logs ORDER BY id DESC LIMIT ?",
                (limit,)
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# --- ECONOMY ---
async def get_balance(user_id, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT wallet, bank FROM economy WHERE user_id = ? AND guild_id = ?",
            (str(user_id), str(guild_id))
        )
        row = await cursor.fetchone()
        if row: return dict(row)
        
        # İlk kez ise oluştur
        await db.execute(
            "INSERT INTO economy (user_id, guild_id, wallet, bank) VALUES (?, ?, ?, ?)",
            (str(user_id), str(guild_id), 100, 0) # Başlangıç 100 Coin
        )
        await db.commit()
        return {"wallet": 100, "bank": 0}

async def update_wallet(user_id, guild_id, amount):
    async with aiosqlite.connect(DB_PATH) as db:
        await get_balance(user_id, guild_id) # Varlığı garanti et
        await db.execute(
            "UPDATE economy SET wallet = wallet + ? WHERE user_id = ? AND guild_id = ?",
            (amount, str(user_id), str(guild_id))
        )
        await db.commit()

# --- MODERATION ---
async def add_warn(user_id, guild_id, mod_id, reason):
    async with aiosqlite.connect(DB_PATH) as db:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await db.execute(
            "INSERT INTO warnings (user_id, guild_id, moderator_id, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
            (str(user_id), str(guild_id), str(mod_id), reason, timestamp)
        )
        await db.commit()

async def get_warns(user_id, guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM warnings WHERE user_id = ? AND guild_id = ?",
            (str(user_id), str(guild_id))
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_all_warns(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM warnings WHERE guild_id = ? ORDER BY timestamp DESC", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# --- RULES ---
async def add_rule(guild_id, title, content):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO rules (guild_id, title, content) VALUES (?, ?, ?)",
            (str(guild_id), title, content)
        )
        await db.commit()

async def get_rules(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM rules WHERE guild_id = ?", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# --- SUGGESTIONS & APPLICATIONS ---
async def add_suggestion(guild_id, user_id, content, message_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO suggestions (guild_id, user_id, content, message_id) VALUES (?, ?, ?, ?)",
            (str(guild_id), str(user_id), content, str(message_id))
        )
        await db.commit()

async def update_suggestion_status(suggestion_id, status):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE suggestions SET status = ? WHERE id = ?", (status, suggestion_id))
        await db.commit()

async def add_application(guild_id, user_id, content, app_type, message_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO applications (guild_id, user_id, content, type, message_id) VALUES (?, ?, ?, ?, ?)",
            (str(guild_id), str(user_id), content, app_type, str(message_id))
        )
        await db.commit()

async def get_suggestion_list(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM suggestions WHERE guild_id = ? ORDER BY id DESC", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_application_list(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM applications WHERE guild_id = ? ORDER BY id DESC", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def update_application_status(app_id, status):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE applications SET status = ? WHERE id = ?", (status, app_id))
        await db.commit()

async def get_application_by_id(app_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM applications WHERE id = ?", (app_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None



# --- EKİP SİSTEMİ ---
async def add_ekip_team(guild_id, ekip_ismi, boss_role_id, og_role_id, normal_role_id, channel_id, leader_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO ekip_teams (guild_id, ekip_ismi, boss_role_id, og_role_id, normal_role_id, channel_id, leader_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (str(guild_id), ekip_ismi, str(boss_role_id), str(og_role_id), str(normal_role_id), str(channel_id), str(leader_id))
        )
        await db.commit()

async def get_all_teams(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM ekip_teams WHERE guild_id = ?", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_team(guild_id, ekip_ismi):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM ekip_teams WHERE guild_id = ? AND ekip_ismi = ?", (str(guild_id), ekip_ismi))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_ekip_team_by_channel(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM ekip_teams WHERE channel_id = ?", (str(channel_id),))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def delete_ekip_team(team_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM ekip_teams WHERE id = ?", (team_id,))
        await db.commit()

# --- YAYINCI SİSTEMİ ---
async def set_yayinci_message(user_id, message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO yayinci (user_id, custom_message) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET custom_message=excluded.custom_message",
            (str(user_id), message)
        )
        await db.commit()

async def get_yayinci_message(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT custom_message FROM yayinci WHERE user_id = ?", (str(user_id),))
        row = await cursor.fetchone()
        return row[0] if row else "Yayındayım, hepinizi bekliyorum!"


# --- CONFIG ---
async def update_server_channels(guild_id, rules=None, suggestions=None, apps=None, ticket_category=None, ticket_log=None, ticket_staff=None, ticket_logo=None,
                                 ekip_category=None, ekip_staff_role=None, ekip_log_channel=None, yayinci_channel=None, yayinci_role=None, uyari_log_channel=None, uyari_staff_role=None):
    async with aiosqlite.connect(DB_PATH) as db:
        # Önce kaydın varlığını kontrol et
        cursor = await db.execute("SELECT 1 FROM server_config WHERE guild_id = ?", (str(guild_id),))
        if not await cursor.fetchone():
            await db.execute("INSERT INTO server_config (guild_id) VALUES (?)", (str(guild_id),))
        
        if rules: await db.execute("UPDATE server_config SET rules_channel = ? WHERE guild_id = ?", (str(rules), str(guild_id)))
        if suggestions: await db.execute("UPDATE server_config SET suggestions_channel = ? WHERE guild_id = ?", (str(suggestions), str(guild_id)))
        if apps: await db.execute("UPDATE server_config SET applications_channel = ? WHERE guild_id = ?", (str(apps), str(guild_id)))
        if ticket_category: await db.execute("UPDATE server_config SET ticket_category = ? WHERE guild_id = ?", (str(ticket_category), str(guild_id)))
        if ticket_log: await db.execute("UPDATE server_config SET ticket_log_channel = ? WHERE guild_id = ?", (str(ticket_log), str(guild_id)))
        if ticket_staff: await db.execute("UPDATE server_config SET ticket_staff_role = ? WHERE guild_id = ?", (str(ticket_staff), str(guild_id)))
        if ticket_logo: await db.execute("UPDATE server_config SET ticket_logo_url = ? WHERE guild_id = ?", (str(ticket_logo), str(guild_id)))
        if ekip_category: await db.execute("UPDATE server_config SET ekip_category = ? WHERE guild_id = ?", (str(ekip_category), str(guild_id)))
        if ekip_staff_role: await db.execute("UPDATE server_config SET ekip_staff_role = ? WHERE guild_id = ?", (str(ekip_staff_role), str(guild_id)))
        if ekip_log_channel: await db.execute("UPDATE server_config SET ekip_log_channel = ? WHERE guild_id = ?", (str(ekip_log_channel), str(guild_id)))
        if yayinci_channel: await db.execute("UPDATE server_config SET yayinci_channel = ? WHERE guild_id = ?", (str(yayinci_channel), str(guild_id)))
        if yayinci_role: await db.execute("UPDATE server_config SET yayinci_role = ? WHERE guild_id = ?", (str(yayinci_role), str(guild_id)))
        if uyari_log_channel: await db.execute("UPDATE server_config SET uyari_log_channel = ? WHERE guild_id = ?", (str(uyari_log_channel), str(guild_id)))
        if uyari_staff_role: await db.execute("UPDATE server_config SET uyari_staff_role = ? WHERE guild_id = ?", (str(uyari_staff_role), str(guild_id)))
        await db.commit()

async def update_automod_config(guild_id, links=None, spam=None, words=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if links is not None: await db.execute("UPDATE server_config SET automod_links = ? WHERE guild_id = ?", (int(links), str(guild_id)))
        if spam is not None: await db.execute("UPDATE server_config SET automod_spam = ? WHERE guild_id = ?", (int(spam), str(guild_id)))
        if words is not None: await db.execute("UPDATE server_config SET automod_words = ? WHERE guild_id = ?", (words, str(guild_id)))
        await db.commit()

# --- GIVEAWAY SYSTEM ---
async def add_giveaway(guild_id, channel_id, message_id, prize, winners, end_time):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO giveaways (guild_id, channel_id, message_id, prize, winners, end_time) VALUES (?, ?, ?, ?, ?, ?)",
            (str(guild_id), str(channel_id), str(message_id), prize, winners, end_time.strftime("%Y-%m-%d %H:%M:%S"))
        )
        await db.commit()

async def get_active_giveaways():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM giveaways WHERE status = 'active'")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def update_giveaway_status(giveaway_id, status):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE giveaways SET status = ? WHERE id = ?", (status, giveaway_id))
        await db.commit()

# --- INVITE SYSTEM ---
async def update_invite_count(guild_id, inviter_id, amount=1):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO invites (guild_id, inviter_id, count) 
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, inviter_id) DO UPDATE SET count = count + ?
        """, (str(guild_id), str(inviter_id), amount, amount))
        await db.commit()

async def get_invite_leaderboard(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM invites WHERE guild_id = ? ORDER BY count DESC LIMIT 10", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_server_config(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM server_config WHERE guild_id = ?", (str(guild_id),))
        row = await cursor.fetchone()
        return dict(row) if row else None


# --- TOKEN SYSTEM ---
async def add_token(token, role):
    async with aiosqlite.connect(DB_PATH) as db:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await db.execute(
            "INSERT INTO tokens (token, role, created_at) VALUES (?, ?, ?)",
            (token, role, timestamp)
        )
        await db.commit()

async def get_all_tokens():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM tokens ORDER BY created_at DESC")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def delete_token(token):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM tokens WHERE token = ?", (token,))
        await db.commit()

async def validate_token(token):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM tokens WHERE token = ?", (token,))
            row = await cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"❌ Token doğrulama hatası (DB): {e}")
        return None

# --- AUTO RESPONDER ---
async def add_auto_responder(guild_id, keyword, response):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO auto_responders (guild_id, keyword, response) VALUES (?, ?, ?)",
            (str(guild_id), keyword.lower(), response)
        )
        await db.commit()

async def get_auto_responders(guild_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM auto_responders WHERE guild_id = ?", (str(guild_id),))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def delete_auto_responder(responder_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM auto_responders WHERE id = ?", (responder_id,))
        await db.commit()

async def find_auto_response(guild_id, keyword):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Case insensitive exact match check
        cursor = await db.execute(
            "SELECT response FROM auto_responders WHERE guild_id = ? AND keyword = ?",
            (str(guild_id), keyword.lower())
        )
        row = await cursor.fetchone()
        return row['response'] if row else None

# --- ANALYTICS ---
async def increment_stat(guild_id, stat_type):
    async with aiosqlite.connect(DB_PATH) as db:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        # Upsert logic
        await db.execute(f"""
            INSERT INTO daily_stats (date, guild_id, {stat_type}) 
            VALUES (?, ?, 1)
            ON CONFLICT(date, guild_id) DO UPDATE SET {stat_type} = {stat_type} + 1
        """, (today, str(guild_id)))
        await db.commit()

async def get_analytics_data(guild_id, days=7):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Son X günün verisini çek
        cursor = await db.execute("""
            SELECT * FROM daily_stats 
            WHERE guild_id = ? 
            ORDER BY date DESC LIMIT ?
        """, (str(guild_id), days))
        rows = await cursor.fetchall()
        # Tarihe göre geri sar (grafik için soldan sağa)
        data = [dict(row) for row in rows]
        data.reverse()
        return data
