import aiosqlite
import time
import re
import os
from datetime import datetime, timedelta
import logging
import sys

# Render-friendly database path with fallback
def get_db_path():
    """
    Get database path that works on both Render and local development
    Render provides /tmp directory for ephemeral storage
    """
    # Try to use Render's persistent storage if available
    if os.getenv('RENDER'):
        # On Render, we can use /tmp for database
        return "/tmp/nullprotocol.db"
    elif os.getenv('RENDER_DB_PATH'):
        # Custom database path from environment
        return os.getenv('RENDER_DB_PATH')
    else:
        # Local development - use current directory
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "nullprotocol.db")

DB_PATH = get_db_path()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Print to console for Render logs
        logging.FileHandler('/tmp/bot.log') if os.getenv('RENDER') else logging.NullHandler()
    ]
)

logger = logging.getLogger(__name__)

# Helper function to parse time
def parse_time_string(time_str):
    """
    Parse time string like: 
    "30m" = 30 minutes
    "2h" = 2 hours (120 minutes)
    "1h30m" = 90 minutes
    "24h" = 1440 minutes
    Returns minutes or None
    """
    if not time_str or str(time_str).lower() == 'none':
        return None
    
    time_str = str(time_str).lower()
    total_minutes = 0
    
    # Extract hours
    hour_match = re.search(r'(\d+)h', time_str)
    if hour_match:
        total_minutes += int(hour_match.group(1)) * 60
    
    # Extract minutes
    minute_match = re.search(r'(\d+)m', time_str)
    if minute_match:
        total_minutes += int(minute_match.group(1))
    
    # Extract days
    day_match = re.search(r'(\d+)d', time_str)
    if day_match:
        total_minutes += int(day_match.group(1)) * 24 * 60
    
    # If only hours or minutes specified
    if hour_match or minute_match or day_match:
        return total_minutes if total_minutes > 0 else None
    
    # If no h/m/d specified, assume minutes if it's a number
    if time_str.isdigit():
        total_minutes = int(time_str)
        return total_minutes if total_minutes > 0 else None
    
    return None

async def init_db():
    """
    Initialize database with optimized settings for Render
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            # Enable foreign keys and WAL mode for better performance
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute("PRAGMA journal_mode = WAL")
            await db.execute("PRAGMA synchronous = NORMAL")
            await db.execute("PRAGMA cache_size = -2000")  # 2MB cache
            await db.execute("PRAGMA temp_store = MEMORY")
            await db.execute("PRAGMA mmap_size = 268435456")  # 256MB
            
            # Users Table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    credits INTEGER DEFAULT 5,
                    joined_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    referrer_id INTEGER,
                    is_banned INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    last_active TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id) ON DELETE SET NULL
                )
            """)
            
            # Admins Table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    level TEXT DEFAULT 'admin',
                    added_by INTEGER,
                    added_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            # Redeem Codes Table with expiry in MINUTES
            await db.execute("""
                CREATE TABLE IF NOT EXISTS redeem_codes (
                    code TEXT PRIMARY KEY,
                    amount INTEGER NOT NULL,
                    max_uses INTEGER NOT NULL,
                    current_uses INTEGER DEFAULT 0,
                    expiry_minutes INTEGER,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    CHECK(amount > 0),
                    CHECK(max_uses > 0),
                    CHECK(current_uses >= 0)
                )
            """)
            
            # Redeem logs to track who used which code
            await db.execute("""
                CREATE TABLE IF NOT EXISTS redeem_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    claimed_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, code),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY (code) REFERENCES redeem_codes(code) ON DELETE CASCADE
                )
            """)
            
            # Lookup Logs Table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS lookup_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    api_type TEXT NOT NULL,
                    input_data TEXT,
                    result TEXT,
                    lookup_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes for better performance
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_referrer ON users(referrer_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_credits ON users(credits)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redeem_codes_active ON redeem_codes(is_active)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redeem_codes_expiry ON redeem_codes(expiry_minutes)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redeem_logs_user ON redeem_logs(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_redeem_logs_code ON redeem_logs(code)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_lookup_logs_user ON lookup_logs(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_lookup_logs_date ON lookup_logs(lookup_date)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_lookup_logs_type ON lookup_logs(api_type)")
            
            # Create triggers for auto cleanup
            await db.execute("""
                CREATE TRIGGER IF NOT EXISTS cleanup_expired_codes
                AFTER INSERT ON redeem_codes
                BEGIN
                    UPDATE redeem_codes 
                    SET is_active = 0 
                    WHERE expiry_minutes IS NOT NULL 
                    AND expiry_minutes > 0
                    AND datetime(created_date, '+' || expiry_minutes || ' minutes') < datetime('now');
                END;
            """)
            
            # Create trigger for last_active update
            await db.execute("""
                CREATE TRIGGER IF NOT EXISTS update_last_active_trigger
                AFTER UPDATE OF credits ON users
                FOR EACH ROW
                BEGIN
                    UPDATE users 
                    SET last_active = datetime('now') 
                    WHERE user_id = NEW.user_id;
                END;
            """)
            
            await db.commit()
            logger.info(f"✅ Database initialized successfully at {DB_PATH}")
            
            # Run vacuum to optimize
            await db.execute("VACUUM")
            
            return True
    except Exception as e:
        logger.error(f"❌ Error initializing database: {e}", exc_info=True)
        # Try fallback path on error
        if os.getenv('RENDER'):
            global DB_PATH
            DB_PATH = "/tmp/nullprotocol_fallback.db"
            logger.info(f"Trying fallback database path: {DB_PATH}")
            return await init_db()
        return False

async def get_user(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None

async def add_user(user_id, username, referrer_id=None):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            # Check if user exists
            async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                if await cursor.fetchone():
                    return False
            
            current_time = datetime.now().isoformat()
            username_clean = username if username else ""
            
            # If referrer exists, give bonus to referrer
            if referrer_id:
                # Verify referrer exists
                async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,)) as cursor:
                    if await cursor.fetchone():
                        # Give referrer bonus
                        await db.execute(
                            "UPDATE users SET credits = credits + 3, total_earned = total_earned + 3 WHERE user_id = ?",
                            (referrer_id,)
                        )
            
            await db.execute("""
                INSERT INTO users (user_id, username, credits, joined_date, referrer_id, last_active) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, username_clean, 5, current_time, referrer_id, current_time))
            await db.commit()
            logger.info(f"✅ Added user {user_id} (@{username_clean})")
            return True
    except Exception as e:
        logger.error(f"Error adding user {user_id}: {e}")
        return False

async def update_credits(user_id, amount):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            current_time = datetime.now().isoformat()
            if amount > 0:
                await db.execute(
                    """UPDATE users 
                       SET credits = credits + ?, 
                           total_earned = total_earned + ?,
                           last_active = ?
                       WHERE user_id = ?""",
                    (amount, amount, current_time, user_id)
                )
            else:
                await db.execute(
                    "UPDATE users SET credits = credits + ?, last_active = ? WHERE user_id = ?",
                    (amount, current_time, user_id)
                )
            await db.commit()
            return True
    except Exception as e:
        logger.error(f"Error updating credits for {user_id}: {e}")
        return False

async def set_ban_status(user_id, status):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "UPDATE users SET is_banned = ?, last_active = ? WHERE user_id = ?", 
                (status, datetime.now().isoformat(), user_id)
            )
            await db.commit()
            logger.info(f"User {user_id} ban status set to {status}")
            return True
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {e}")
        return False

async def create_redeem_code(code, amount, max_uses, expiry_minutes=None):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            # Check if code already exists
            async with db.execute("SELECT code FROM redeem_codes WHERE code = ?", (code,)) as cursor:
                if await cursor.fetchone():
                    return False, "Code already exists"
            
            # Validate inputs
            if amount <= 0:
                return False, "Amount must be positive"
            if max_uses <= 0:
                return False, "Max uses must be positive"
            
            await db.execute("""
                INSERT INTO redeem_codes 
                (code, amount, max_uses, expiry_minutes, is_active, created_date)
                VALUES (?, ?, ?, ?, 1, ?)
            """, (code, amount, max_uses, expiry_minutes, datetime.now().isoformat()))
            await db.commit()
            logger.info(f"✅ Created redeem code: {code} ({amount} credits, {max_uses} uses)")
            return True, "Code created successfully"
    except Exception as e:
        logger.error(f"Error creating code {code}: {e}")
        return False, f"Error: {str(e)}"

async def redeem_code_db(user_id, code):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=15) as db:
            await db.execute("BEGIN TRANSACTION")
            
            # Check if user exists
            async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                if not await cursor.fetchone():
                    await db.execute("ROLLBACK")
                    return "user_not_found"
            
            # Check if user already claimed this code
            async with db.execute("""
                SELECT 1 FROM redeem_logs WHERE user_id = ? AND code = ?
            """, (user_id, code)) as cursor:
                if await cursor.fetchone():
                    await db.execute("ROLLBACK")
                    return "already_claimed"
            
            # Get code details
            async with db.execute("""
                SELECT amount, max_uses, current_uses, expiry_minutes, created_date, is_active
                FROM redeem_codes WHERE code = ?
            """, (code,)) as cursor:
                data = await cursor.fetchone()
                
            if not data:
                await db.execute("ROLLBACK")
                return "invalid"
            
            amount, max_uses, current_uses, expiry_minutes, created_date, is_active = data
            
            # Check if code is active
            if not is_active:
                await db.execute("ROLLBACK")
                return "inactive"
            
            # Check max uses
            if current_uses >= max_uses:
                await db.execute("ROLLBACK")
                return "limit_reached"
            
            # Check expiry
            if expiry_minutes is not None and expiry_minutes > 0:
                try:
                    created_dt = datetime.fromisoformat(created_date)
                    expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
                    if datetime.now() > expiry_dt:
                        await db.execute("ROLLBACK")
                        return "expired"
                except Exception as e:
                    logger.error(f"Error checking expiry: {e}")
            
            # Update current uses
            await db.execute("""
                UPDATE redeem_codes SET current_uses = current_uses + 1 WHERE code = ?
            """, (code,))
            
            # Add credits to user
            await db.execute("""
                UPDATE users SET 
                    credits = credits + ?, 
                    total_earned = total_earned + ?, 
                    last_active = ? 
                WHERE user_id = ?
            """, (amount, amount, datetime.now().isoformat(), user_id))
            
            # Log the claim
            await db.execute("""
                INSERT INTO redeem_logs (user_id, code, claimed_date)
                VALUES (?, ?, ?)
            """, (user_id, code, datetime.now().isoformat()))
            
            await db.execute("COMMIT")
            logger.info(f"✅ User {user_id} redeemed code {code} for {amount} credits")
            return amount
            
    except Exception as e:
        try:
            await db.execute("ROLLBACK")
        except:
            pass
        logger.error(f"Error redeeming code {code} for user {user_id}: {e}")
        return f"error: {str(e)}"

async def get_all_users():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT user_id FROM users ORDER BY user_id") as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return []

async def get_user_by_username(username):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT user_id FROM users WHERE username = ?", (username,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.error(f"Error getting user by username {username}: {e}")
        return None

async def get_top_referrers(limit=10):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT referrer_id, COUNT(*) as referrals 
                FROM users 
                WHERE referrer_id IS NOT NULL 
                GROUP BY referrer_id 
                ORDER BY referrals DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting top referrers: {e}")
        return []

async def get_bot_stats():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                total_users = (await cursor.fetchone())[0] or 0
            
            async with db.execute("SELECT COUNT(*) FROM users WHERE credits > 0") as cursor:
                active_users = (await cursor.fetchone())[0] or 0
            
            async with db.execute("SELECT SUM(credits) FROM users") as cursor:
                total_credits = (await cursor.fetchone())[0] or 0
            
            async with db.execute("SELECT SUM(total_earned) FROM users") as cursor:
                credits_distributed = (await cursor.fetchone())[0] or 0
            
            return {
                'total_users': total_users,
                'active_users': active_users,
                'total_credits': total_credits,
                'credits_distributed': credits_distributed
            }
    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        return {'total_users': 0, 'active_users': 0, 'total_credits': 0, 'credits_distributed': 0}

async def get_users_in_range(start_date, end_date):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, credits, joined_date 
                FROM users 
                WHERE datetime(joined_date) BETWEEN datetime(?, 'unixepoch') AND datetime(?, 'unixepoch')
                ORDER BY datetime(joined_date) DESC
            """, (start_date, end_date)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting users in range: {e}")
        return []

async def add_admin(user_id, level='admin'):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            # Check if user exists in users table
            async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cursor:
                if not await cursor.fetchone():
                    # Add user first
                    await db.execute(
                        "INSERT INTO users (user_id, username, last_active) VALUES (?, ?, ?)",
                        (user_id, 'admin_user', datetime.now().isoformat())
                    )
            
            # Check if already admin
            async with db.execute("SELECT user_id FROM admins WHERE user_id = ?", (user_id,)) as cursor:
                if await cursor.fetchone():
                    return False, "User is already an admin"
            
            await db.execute(
                "INSERT INTO admins (user_id, level, added_by, added_date) VALUES (?, ?, ?, ?)",
                (user_id, level, 0, datetime.now().isoformat())
            )
            await db.commit()
            logger.info(f"✅ Added admin {user_id} with level {level}")
            return True, "Admin added successfully"
    except Exception as e:
        logger.error(f"Error adding admin {user_id}: {e}")
        return False, f"Error: {str(e)}"

async def remove_admin(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
            await db.commit()
            logger.info(f"✅ Removed admin {user_id}")
            return True
    except Exception as e:
        logger.error(f"Error removing admin {user_id}: {e}")
        return False

async def get_all_admins():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT a.user_id, a.level, a.added_date 
                FROM admins a
                ORDER BY a.added_date DESC
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting all admins: {e}")
        return []

async def is_admin(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT level FROM admins WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else False
    except Exception as e:
        logger.error(f"Error checking admin status for {user_id}: {e}")
        return False

async def get_expired_codes():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT code, amount, current_uses, max_uses, expiry_minutes, created_date 
                FROM redeem_codes 
                WHERE is_active = 1 
                AND expiry_minutes IS NOT NULL 
                AND expiry_minutes > 0
                AND datetime(created_date, '+' || expiry_minutes || ' minutes') < datetime('now')
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting expired codes: {e}")
        return []

async def delete_redeem_code(code):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM redeem_codes WHERE code = ?", (code,))
            await db.commit()
            logger.info(f"✅ Deleted redeem code {code}")
            return True
    except Exception as e:
        logger.error(f"Error deleting redeem code {code}: {e}")
        return False

async def deactivate_code(code):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            # Check if code exists
            async with db.execute("SELECT code FROM redeem_codes WHERE code = ?", (code,)) as cursor:
                if not await cursor.fetchone():
                    return False, "Code not found"
            
            await db.execute("UPDATE redeem_codes SET is_active = 0 WHERE code = ?", (code,))
            await db.commit()
            logger.info(f"✅ Deactivated code {code}")
            return True, "Code deactivated successfully"
    except Exception as e:
        logger.error(f"Error deactivating code {code}: {e}")
        return False, f"Error: {str(e)}"

async def get_all_codes():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses, 
                       expiry_minutes, created_date, is_active
                FROM redeem_codes
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting all codes: {e}")
        return []

async def get_user_stats(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT 
                    COALESCE((SELECT COUNT(*) FROM users WHERE referrer_id = ?), 0) as referrals,
                    COALESCE((SELECT COUNT(*) FROM redeem_logs WHERE user_id = ?), 0) as codes_claimed,
                    COALESCE((SELECT SUM(rc.amount) 
                     FROM redeem_logs rl 
                     JOIN redeem_codes rc ON rl.code = rc.code 
                     WHERE rl.user_id = ?), 0) as total_from_codes
                FROM users WHERE user_id = ?
            """, (user_id, user_id, user_id, user_id)) as cursor:
                row = await cursor.fetchone()
                return row if row else (0, 0, 0)
    except Exception as e:
        logger.error(f"Error getting user stats for {user_id}: {e}")
        return (0, 0, 0)

async def get_recent_users(limit=20):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, joined_date 
                FROM users 
                ORDER BY datetime(joined_date) DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting recent users: {e}")
        return []

async def get_active_codes():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 1
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting active codes: {e}")
        return []

async def get_inactive_codes():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT code, amount, max_uses, current_uses
                FROM redeem_codes
                WHERE is_active = 0
                ORDER BY created_date DESC
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting inactive codes: {e}")
        return []

async def delete_user(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.commit()
            logger.info(f"✅ Deleted user {user_id}")
            return True
    except Exception as e:
        logger.error(f"Error deleting user {user_id}: {e}")
        return False

async def reset_user_credits(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "UPDATE users SET credits = 0, last_active = ? WHERE user_id = ?", 
                (datetime.now().isoformat(), user_id)
            )
            await db.commit()
            logger.info(f"✅ Reset credits for user {user_id}")
            return True
    except Exception as e:
        logger.error(f"Error resetting credits for user {user_id}: {e}")
        return False

async def search_users(query):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, credits 
                FROM users 
                WHERE username LIKE ? OR CAST(user_id AS TEXT) LIKE ?
                LIMIT 20
            """, (f"%{query}%", f"%{query}%")) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error searching users for query {query}: {e}")
        return []

async def get_daily_stats(days=7):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT 
                    DATE(joined_date) as join_date,
                    COUNT(*) as new_users
                FROM users 
                WHERE DATE(joined_date) >= DATE('now', '-' || ? || ' days')
                GROUP BY DATE(joined_date)
                ORDER BY join_date DESC
            """, (days,)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting daily stats: {e}")
        return []

async def log_lookup(user_id, api_type, input_data, result):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("""
                INSERT INTO lookup_logs (user_id, api_type, input_data, result, lookup_date)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, api_type, input_data[:500], str(result)[:2000], datetime.now().isoformat()))
            await db.commit()
            return True
    except Exception as e:
        logger.error(f"Error logging lookup for user {user_id}: {e}")
        return False

async def get_lookup_stats():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT api_type, COUNT(*) as count 
                FROM lookup_logs 
                GROUP BY api_type
                ORDER BY count DESC
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting lookup stats: {e}")
        return []

async def get_total_lookups():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT COUNT(*) FROM lookup_logs") as cursor:
                return (await cursor.fetchone())[0] or 0
    except Exception as e:
        logger.error(f"Error getting total lookups: {e}")
        return 0

async def get_user_lookups(user_id, limit=50):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT api_type, input_data, lookup_date 
                FROM lookup_logs 
                WHERE user_id = ?
                ORDER BY datetime(lookup_date) DESC
                LIMIT ?
            """, (user_id, limit)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting user lookups for {user_id}: {e}")
        return []

async def get_premium_users():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, credits
                FROM users 
                WHERE credits >= 100 AND is_banned = 0
                ORDER BY credits DESC
                LIMIT 50
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting premium users: {e}")
        return []

async def get_low_credit_users():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, credits
                FROM users 
                WHERE credits <= 5 AND is_banned = 0
                ORDER BY credits ASC
                LIMIT 50
            """) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting low credit users: {e}")
        return []

async def get_inactive_users(days=30):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            async with db.execute("""
                SELECT user_id, username, last_active, credits
                FROM users 
                WHERE (last_active < ? OR last_active IS NULL) 
                AND is_banned = 0
                AND user_id NOT IN (SELECT user_id FROM admins)
                ORDER BY 
                    CASE WHEN last_active IS NULL THEN 1 ELSE 0 END,
                    last_active ASC
                LIMIT 50
            """, (cutoff_date,)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting inactive users: {e}")
        return []

async def update_last_active(user_id):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "UPDATE users SET last_active = ? WHERE user_id = ?", 
                (datetime.now().isoformat(), user_id)
            )
            await db.commit()
            return True
    except Exception as e:
        logger.error(f"Error updating last active for {user_id}: {e}")
        return False

async def get_leaderboard(limit=10):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT user_id, username, credits
                FROM users 
                WHERE is_banned = 0
                ORDER BY credits DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                return await cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        return []

async def bulk_update_credits(user_ids, amount):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=20) as db:
            await db.execute("BEGIN TRANSACTION")
            for user_id in user_ids:
                current_time = datetime.now().isoformat()
                if amount > 0:
                    await db.execute(
                        """UPDATE users 
                           SET credits = credits + ?, 
                               total_earned = total_earned + ?,
                               last_active = ?
                           WHERE user_id = ?""", 
                        (amount, amount, current_time, user_id)
                    )
                else:
                    await db.execute(
                        "UPDATE users SET credits = credits + ?, last_active = ? WHERE user_id = ?", 
                        (amount, current_time, user_id)
                    )
            await db.execute("COMMIT")
            logger.info(f"✅ Bulk updated {len(user_ids)} users with {amount} credits")
            return True
    except Exception as e:
        try:
            await db.execute("ROLLBACK")
        except:
            pass
        logger.error(f"Error bulk updating credits: {e}")
        return False

async def get_code_usage_stats(code):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("""
                SELECT 
                    rc.amount, rc.max_uses, rc.current_uses,
                    COUNT(DISTINCT rl.user_id) as unique_users,
                    GROUP_CONCAT(DISTINCT rl.user_id) as user_ids
                FROM redeem_codes rc
                LEFT JOIN redeem_logs rl ON rc.code = rl.code
                WHERE rc.code = ?
                GROUP BY rc.code
            """, (code,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return (row[0], row[1], row[2], row[3], row[4])
                return None
    except Exception as e:
        logger.error(f"Error getting code usage stats for {code}: {e}")
        return None

async def cleanup_expired_codes():
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("""
                UPDATE redeem_codes 
                SET is_active = 0 
                WHERE is_active = 1 
                AND expiry_minutes IS NOT NULL 
                AND expiry_minutes > 0
                AND datetime(created_date, '+' || expiry_minutes || ' minutes') < datetime('now')
            """)
            await db.commit()
            
            async with db.execute("SELECT changes()") as cursor:
                changes = (await cursor.fetchone())[0]
                logger.info(f"✅ Cleaned up {changes} expired codes")
                return changes
    except Exception as e:
        logger.error(f"Error cleaning up expired codes: {e}")
        return 0

async def backup_database():
    """Create a backup of the database"""
    try:
        if os.path.exists(DB_PATH):
            backup_name = f"nullprotocol_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            backup_path = os.path.join(os.path.dirname(DB_PATH), backup_name)
            
            # Use aiosqlite to copy the database
            async with aiosqlite.connect(DB_PATH) as src_db:
                async with aiosqlite.connect(backup_path) as dst_db:
                    await src_db.backup(dst_db)
            
            logger.info(f"✅ Database backed up to {backup_path}")
            return backup_path
        return None
    except Exception as e:
        logger.error(f"Error backing up database: {e}")
        return None

async def vacuum_database():
    """Optimize database"""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            await db.execute("VACUUM")
            await db.commit()
            logger.info("✅ Database optimized (VACUUM)")
            return True
    except Exception as e:
        logger.error(f"Error vacuuming database: {e}")
        return False

async def check_database_health():
    """Check database health and connection"""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=5) as db:
            # Test connection with simple query
            async with db.execute("SELECT 1") as cursor:
                result = await cursor.fetchone()
                if result and result[0] == 1:
                    return True, "Database is healthy"
            return False, "Database test query failed"
    except Exception as e:
        return False, f"Database connection error: {str(e)}"

async def get_database_size():
    """Get database file size"""
    try:
        if os.path.exists(DB_PATH):
            size_bytes = os.path.getsize(DB_PATH)
            # Convert to MB
            size_mb = size_bytes / (1024 * 1024)
            return size_mb
        return 0
    except Exception as e:
        logger.error(f"Error getting database size: {e}")
        return 0

# Render-specific database maintenance
async def render_database_maintenance():
    """
    Perform database maintenance tasks for Render deployment
    This should be called periodically
    """
    try:
        logger.info("🔄 Starting Render database maintenance...")
        
        # 1. Clean up expired codes
        expired_cleaned = await cleanup_expired_codes()
        
        # 2. Vacuum database if it's getting large
        db_size = await get_database_size()
        if db_size > 10:  # If > 10MB
            logger.info(f"Database size is {db_size:.2f} MB, running VACUUM...")
            await vacuum_database()
        
        # 3. Backup if needed (only for large deployments)
        if db_size > 5:  # If > 5MB
            await backup_database()
        
        logger.info(f"✅ Render maintenance completed. Cleaned {expired_cleaned} expired codes.")
        return True
    except Exception as e:
        logger.error(f"Error in Render database maintenance: {e}")
        return False