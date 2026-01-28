import logging
import os
import json
import asyncio
import httpx
import secrets
import csv
import tempfile
import shutil
import sys
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    FSInputFile, ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from database import (
    init_db, add_user, get_user, update_credits, 
    create_redeem_code, redeem_code_db, get_all_users, 
    set_ban_status, get_bot_stats, get_users_in_range,
    add_admin, remove_admin, get_all_admins, is_admin,
    get_expired_codes, delete_redeem_code, get_top_referrers,
    deactivate_code, get_all_codes, parse_time_string,
    get_user_by_username, get_user_stats,
    get_recent_users, get_active_codes, get_inactive_codes,
    delete_user, reset_user_credits,
    search_users, get_daily_stats, log_lookup,
    get_lookup_stats, get_total_lookups, get_user_lookups,
    get_premium_users, get_low_credit_users, get_inactive_users,
    update_last_active, get_leaderboard,
    bulk_update_credits, get_code_usage_stats,
    check_database_health, render_database_maintenance
)

# --- CONFIGURATION ---
load_dotenv()

# Essential environment variables
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logging.error("❌ BOT_TOKEN environment variable missing!")
    exit(1)

OWNER_ID = os.getenv("OWNER_ID")
if not OWNER_ID:
    logging.error("❌ OWNER_ID environment variable missing!")
    exit(1)

try:
    OWNER_ID = int(OWNER_ID)
except ValueError:
    logging.error("❌ OWNER_ID must be a valid integer!")
    exit(1)

ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip().isdigit()]

# Render specific configurations
RENDER = os.getenv("RENDER", "").lower() == "true"
PORT = int(os.getenv("PORT", "8080"))
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL", "")

# Channels Config
CHANNELS_STR = os.getenv("FORCE_JOIN_CHANNELS", "")
CHANNELS = [int(x.strip()) for x in CHANNELS_STR.split(",") if x.strip().lstrip('-').isdigit()]

CHANNEL_LINKS_STR = os.getenv("FORCE_JOIN_LINKS", "")
CHANNEL_LINKS = [link.strip() for link in CHANNEL_LINKS_STR.split(",") if link.strip()]

# Log Channels
LOG_CHANNELS = {
    'num': os.getenv("LOG_CHANNEL_NUM"),
    'ifsc': os.getenv("LOG_CHANNEL_IFSC"),
    'email': os.getenv("LOG_CHANNEL_EMAIL"),
    'gst': os.getenv("LOG_CHANNEL_GST"),
    'vehicle': os.getenv("LOG_CHANNEL_VEHICLE"),
    'pincode': os.getenv("LOG_CHANNEL_PINCODE"),
    'instagram': os.getenv("LOG_CHANNEL_INSTAGRAM"),
    'github': os.getenv("LOG_CHANNEL_GITHUB"),
    'pakistan': os.getenv("LOG_CHANNEL_PAKISTAN"),
    'ip': os.getenv("LOG_CHANNEL_IP"),
    'ff_info': os.getenv("LOG_CHANNEL_FF_INFO"),
    'ff_ban': os.getenv("LOG_CHANNEL_FF_BAN")
}

# APIs
APIS = {
    'num': os.getenv("API_NUM"),
    'ifsc': os.getenv("API_IFSC"),
    'email': os.getenv("API_EMAIL"),
    'gst': os.getenv("API_GST"),
    'vehicle': os.getenv("API_VEHICLE"),
    'pincode': os.getenv("API_PINCODE"),
    'instagram': os.getenv("API_INSTAGRAM"),
    'github': os.getenv("API_GITHUB"),
    'pakistan': os.getenv("API_PAKISTAN"),
    'ip': os.getenv("API_IP"),
    'ff_info': os.getenv("API_FF_INFO"),
    'ff_ban': os.getenv("API_FF_BAN")
}

# Setup logging for Render
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Print to console for Render logs
    ]
)
logger = logging.getLogger(__name__)

# Setup bot with timeout for Render
bot = Bot(token=TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- FSM STATES ---
class Form(StatesGroup):
    waiting_for_redeem = State()
    waiting_for_broadcast = State()
    waiting_for_direct_message = State()
    waiting_for_dm_user = State()
    waiting_for_dm_content = State()
    waiting_for_custom_code = State()
    waiting_for_stats_range = State()
    waiting_for_code_deactivate = State()
    waiting_for_api_input = State()
    waiting_for_api_type = State()
    waiting_for_username = State()
    waiting_for_delete_user = State()
    waiting_for_reset_credits = State()
    waiting_for_bulk_message = State()
    waiting_for_code_stats = State()
    waiting_for_user_lookups = State()
    waiting_for_bulk_gift = State()
    waiting_for_user_search = State()
    waiting_for_settings = State()

# --- HELPERS ---
def get_branding():
    return {
        "meta": {
            "developer": "@Nullprotocol_X",
            "powered_by": "NULL PROTOCOL",
            "timestamp": datetime.now().isoformat()
        }
    }

def clean_api_response(data):
    """Remove other developer names from API response"""
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if key.lower() == 'branding':
                continue
            elif isinstance(value, str):
                if any(unwanted in value.lower() for unwanted in ['@patelkrish_99', 'patelkrish_99', 't.me/anshapi', 'anshapi', '@losernagiofficial']):
                    continue
                if 'credit' in value.lower() and 'nullprotocol' not in value.lower():
                    continue
                cleaned[key] = value
            elif isinstance(value, dict):
                cleaned[key] = clean_api_response(value)
            elif isinstance(value, list):
                cleaned[key] = [clean_api_response(item) if isinstance(item, dict) else item for item in value]
            else:
                cleaned[key] = value
        return cleaned
    elif isinstance(data, list):
        return [clean_api_response(item) if isinstance(item, dict) else item for item in data]
    return data

def format_json_for_display(data, max_length=3500):
    """Format JSON for display, truncate if too long"""
    try:
        formatted_json = json.dumps(data, indent=4, ensure_ascii=False)
    except Exception as e:
        formatted_json = f"Error formatting JSON: {str(e)}"
    
    if len(formatted_json) > max_length:
        truncated = formatted_json[:max_length]
        truncated += f"\n\n... [Data truncated, {len(formatted_json) - max_length} characters more]"
        return truncated, True
    return formatted_json, False

def create_readable_txt_file(raw_data, api_type, input_data):
    """Create readable TXT file from data"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(f"🔍 {api_type.upper()} Lookup Results\n")
        f.write(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"🔎 Input: {input_data}\n")
        f.write("="*50 + "\n\n")
        
        def write_readable(obj, indent=0, file=f):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    file.write("  " * indent + f"• {key}: ")
                    if isinstance(value, (dict, list)):
                        file.write("\n")
                        write_readable(value, indent + 1, file)
                    else:
                        file.write(f"{value}\n")
            elif isinstance(obj, list):
                for i, item in enumerate(obj, 1):
                    file.write("  " * indent + f"{i}. ")
                    if isinstance(item, (dict, list)):
                        file.write("\n")
                        write_readable(item, indent + 1, file)
                    else:
                        file.write(f"{item}\n")
            else:
                file.write(f"{obj}\n")
        
        write_readable(raw_data)
        
        f.write("\n" + "="*50 + "\n")
        f.write("👨‍💻 Developer: @Nullprotocol_X\n")
        f.write("⚡ Powered by: NULL PROTOCOL\n")
        return f.name

async def is_user_owner(user_id):
    return user_id == OWNER_ID

async def is_user_admin(user_id):
    if user_id == OWNER_ID:
        return 'owner'
    if user_id in ADMIN_IDS:
        return 'admin'
    try:
        db_admin = await is_admin(user_id)
        return db_admin
    except Exception:
        return False

async def is_user_banned(user_id):
    try:
        user = await get_user(user_id)
        if user and len(user) > 5 and user[5] == 1:
            return True
        return False
    except Exception:
        return False

async def check_membership(user_id):
    admin_level = await is_user_admin(user_id)
    if admin_level: 
        return True
    
    if not CHANNELS:
        return True
        
    try:
        for channel_id in CHANNELS:
            try:
                member = await bot.get_chat_member(channel_id, user_id)
                if member.status in ['left', 'kicked']:
                    return False
            except Exception as e:
                logger.error(f"Error checking membership for channel {channel_id}: {e}")
                continue
        return True
    except Exception as e:
        logger.error(f"Error in check_membership: {e}")
        return True  # Return True on error to not block users

def get_join_keyboard():
    buttons = []
    for i, link in enumerate(CHANNEL_LINKS):
        if link:
            buttons.append([InlineKeyboardButton(text=f"📢 Join Channel {i+1}", url=link)])
    buttons.append([InlineKeyboardButton(text="✅ Verify Join", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- UPDATED MAIN MENU ---
def get_main_menu(user_id):
    keyboard = [
        [
            InlineKeyboardButton(text="📱 Number", callback_data="api_num"),
            InlineKeyboardButton(text="🏦 IFSC", callback_data="api_ifsc")
        ],
        [
            InlineKeyboardButton(text="📧 Email", callback_data="api_email"),
            InlineKeyboardButton(text="📋 GST", callback_data="api_gst")
        ],
        [
            InlineKeyboardButton(text="🚗 Vehicle", callback_data="api_vehicle"),
            InlineKeyboardButton(text="📮 Pincode", callback_data="api_pincode")
        ],
        [
            InlineKeyboardButton(text="📷 Instagram", callback_data="api_instagram"),
            InlineKeyboardButton(text="🐱 GitHub", callback_data="api_github")
        ],
        [
            InlineKeyboardButton(text="🇵🇰 Pakistan", callback_data="api_pakistan"),
            InlineKeyboardButton(text="🌐 IP Lookup", callback_data="api_ip")
        ],
        [
            InlineKeyboardButton(text="🔥 FF Info", callback_data="api_ff_info"),
            InlineKeyboardButton(text="🚫 FF Ban", callback_data="api_ff_ban")
        ],
        [
            InlineKeyboardButton(text="🎁 Redeem", callback_data="redeem"),
            InlineKeyboardButton(text="🔗 Refer & earn", callback_data="refer_earn")
        ],
        [
            InlineKeyboardButton(text="👤 Profile", callback_data="profile"),
            InlineKeyboardButton(text="💳 Buy Credits", url="https://t.me/Nullprotocol_X")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- START & JOIN ---
@dp.message(CommandStart())
async def start_command(message: types.Message, command: CommandObject = None):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id):
        await message.answer("🚫 <b>You are BANNED from using this bot.</b>", parse_mode="HTML")
        return

    try:
        existing_user = await get_user(user_id)
        if not existing_user:
            referrer_id = None
            if command and command.args and command.args.startswith("ref_"):
                try:
                    referrer_id = int(command.args.split("_")[1])
                    if referrer_id == user_id: 
                        referrer_id = None
                except: 
                    pass
            
            await add_user(user_id, message.from_user.username, referrer_id)
            if referrer_id:
                await update_credits(referrer_id, 3)
                try: 
                    await bot.send_message(referrer_id, "🎉 <b>Referral +3 Credits!</b>", parse_mode="HTML")
                except: 
                    pass

        if not await check_membership(user_id):
            await message.answer(
                "👋 <b>Welcome to OSINT LOOKUP</b>\n\n"
                "⚠️ <b>Bot use karne ke liye channels join karein:</b>",
                reply_markup=get_join_keyboard(), 
                parse_mode="HTML"
            )
            return

        welcome_msg = f"""
🔓 <b>Access Granted!</b>

Welcome <b>{message.from_user.first_name}</b>,

<b>OSINT LOOKUP</b> - Premium Lookup Services
Select a service from menu below:
"""
        
        await message.answer(
            welcome_msg,
            reply_markup=get_main_menu(user_id), 
            parse_mode="HTML"
        )
        await update_last_active(user_id)
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        await message.answer("❌ An error occurred. Please try again.")

@dp.callback_query(F.data == "check_join")
async def verify_join(callback: types.CallbackQuery):
    try:
        if await check_membership(callback.from_user.id):
            await callback.message.delete()
            await callback.message.answer("✅ <b>Verified!</b>", 
                                        reply_markup=get_main_menu(callback.from_user.id), 
                                        parse_mode="HTML")
        else:
            await callback.answer("❌ Abhi bhi kuch channels join nahi kiye!", show_alert=True)
    except Exception as e:
        logger.error(f"Error in verify_join: {e}")
        await callback.answer("❌ Error verifying join!", show_alert=True)

# --- PROFILE ---
@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery):
    try:
        user_data = await get_user(callback.from_user.id)
        if not user_data: 
            await callback.answer("❌ User not found!", show_alert=True)
            return
        
        admin_level = await is_user_admin(callback.from_user.id)
        credits = "♾️ Unlimited" if admin_level else (user_data[2] if len(user_data) > 2 else 0)
        
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=ref_{user_data[0]}"
        
        stats = await get_user_stats(callback.from_user.id)
        referrals = stats[0] if stats else 0
        codes_claimed = stats[1] if stats else 0
        total_from_codes = stats[2] if stats else 0
        
        msg = (f"👤 <b>User Profile</b>\n\n"
               f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
               f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
               f"💰 <b>Credits:</b> {credits}\n"
               f"📊 <b>Total Earned:</b> {user_data[6] if len(user_data) > 6 else 0}\n"
               f"👥 <b>Referrals:</b> {referrals}\n"
               f"🎫 <b>Codes Claimed:</b> {codes_claimed}\n"
               f"📅 <b>Joined:</b> {datetime.fromtimestamp(float(user_data[3])).strftime('%d-%m-%Y') if len(user_data) > 3 else 'N/A'}\n"
               f"🔗 <b>Referral Link:</b>\n<code>{link}</code>")
        
        await callback.message.edit_text(msg, parse_mode="HTML", 
                                       reply_markup=get_main_menu(callback.from_user.id))
    except Exception as e:
        logger.error(f"Error in show_profile: {e}")
        await callback.answer("❌ Error loading profile!", show_alert=True)

# --- REFERRAL SECTION ---
@dp.callback_query(F.data == "refer_earn")
async def refer_earn_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
        
        msg = (
            "🔗 <b>Refer & Earn Program</b>\n\n"
            "Apne dosto ko invite karein aur free credits paayein!\n"
            "Per Referral: <b>+3 Credits</b>\n\n"
            "👇 <b>Your Link:</b>\n"
            f"<code>{link}</code>\n\n"
            "📊 <b>How it works:</b>\n"
            "1. Apna link share karein\n"
            "2. Jo bhi is link se join karega\n"
            "3. Aapko milenge <b>3 credits</b>"
        )
        
        back_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]
        ])
        await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=back_kb)
    except Exception as e:
        logger.error(f"Error in refer_earn_handler: {e}")
        await callback.answer("❌ Error!", show_alert=True)

@dp.callback_query(F.data == "back_home")
async def go_home(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text(
            f"🔓 <b>Main Menu</b>",
            reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error in go_home: {e}")

# --- REDEEM SYSTEM ---
@dp.callback_query(F.data == "redeem")
async def redeem_start(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.message.answer(
            "🎁 <b>Redeem Code</b>\n\n"
            "Enter your redeem code below:\n\n"
            "📌 <i>Note: Each code can be used only once per user</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_redeem")]
            ]),
            parse_mode="HTML"
        )
        await state.set_state(Form.waiting_for_redeem)
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in redeem_start: {e}")
        await callback.answer("❌ Error!", show_alert=True)

@dp.callback_query(F.data == "cancel_redeem")
async def cancel_redeem_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

# --- API PROCESSING FUNCTION ---
async def process_api_call(message: types.Message, api_type: str, input_data: str):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id): 
        return

    try:
        user = await get_user(user_id)
        admin_level = await is_user_admin(user_id)
        
        if not admin_level and (not user or len(user) <= 2 or user[2] < 1):
            await message.reply("❌ <b>Insufficient Credits!</b>", parse_mode="HTML")
            return
    except Exception as e:
        logger.error(f"Error checking user credits: {e}")
        await message.reply("❌ <b>Error checking your account!</b>", parse_mode="HTML")
        return

    status_msg = await message.reply("🔄 <b>Fetching Data...</b>", parse_mode="HTML")
    
    try:
        if api_type not in APIS or not APIS[api_type]:
            await status_msg.edit_text("❌ <b>This service is currently unavailable.</b>", parse_mode="HTML")
            return
            
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{APIS[api_type]}{input_data}"
            resp = await client.get(url, timeout=30)
            
            try:
                raw_data = resp.json()
            except:
                raw_data = {"error": "Invalid JSON response", "raw": resp.text[:500]}
            
            raw_data = clean_api_response(raw_data)
            
            if isinstance(raw_data, dict):
                raw_data.update(get_branding())
            elif isinstance(raw_data, list):
                data = {"results": raw_data}
                data.update(get_branding())
                raw_data = data
            else:
                data = {"data": str(raw_data)}
                data.update(get_branding())
                raw_data = data

    except Exception as e:
        raw_data = {"error": "Server Error", "details": str(e)}
        raw_data.update(get_branding())

    await status_msg.delete()
    
    formatted_json, is_truncated = format_json_for_display(raw_data, 3500)
    formatted_json = formatted_json.replace('<', '&lt;').replace('>', '&gt;')
    
    should_send_as_file = False
    try:
        json_size = len(json.dumps(raw_data, ensure_ascii=False))
    except:
        json_size = 0
    
    if json_size > 3000 or (isinstance(raw_data, dict) and any(isinstance(v, list) and len(v) > 10 for v in raw_data.values())):
        should_send_as_file = True
    
    temp_file = None
    txt_file = None
    
    if should_send_as_file:
        try:
            # Use /tmp directory for Render compatibility
            if RENDER:
                temp_dir = "/tmp"
            else:
                temp_dir = tempfile.gettempdir()
            
            json_file = os.path.join(temp_dir, f"{api_type}_{input_data}_{secrets.token_hex(8)}.json")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=4, ensure_ascii=False)
            
            txt_file = create_readable_txt_file(raw_data, api_type, input_data)
            
            await message.reply_document(
                FSInputFile(json_file, filename=f"{api_type}_{input_data}.json"),
                caption=(
                    f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
                    f"📊 <b>Input:</b> <code>{input_data}</code>\n"
                    f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                    f"📄 <b>File Type:</b> JSON\n\n"
                    f"📝 <i>Data saved as file for better readability</i>\n\n"
                    f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
                    f"⚡ <b>Powered by:</b> NULL PROTOCOL"
                ),
                parse_mode="HTML"
            )
            
            await message.reply_document(
                FSInputFile(txt_file, filename=f"{api_type}_{input_data}_readable.txt"),
                caption=(
                    f"📄 <b>Readable Text Format</b>\n\n"
                    f"<i>Alternative format for easy reading on mobile</i>"
                ),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Error sending file to user: {e}")
            short_msg = (
                f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
                f"📊 <b>Input:</b> <code>{input_data}</code>\n"
                f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
                f"⚠️ <b>Data too large for message</b>\n"
                f"📄 <i>Attempted to send as file but failed</i>\n\n"
                f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
                f"⚡ <b>Powered by:</b> NULL PROTOCOL"
            )
            await message.reply(short_msg, parse_mode="HTML")
    
    else:
        colored_json = (
            f"🔍 <b>{api_type.upper()} Lookup Results</b>\n\n"
            f"📊 <b>Input:</b> <code>{input_data}</code>\n"
            f"📅 <b>Date:</b> {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n"
        )
        
        if is_truncated:
            colored_json += "⚠️ <i>Response truncated for display</i>\n\n"
        
        colored_json += f"<pre>{formatted_json}</pre>\n\n"
        colored_json += (
            f"📝 <b>Note:</b> Data is for informational purposes only\n"
            f"👨‍💻 <b>Developer:</b> @Nullprotocol_X\n"
            f"⚡ <b>Powered by:</b> NULL PROTOCOL"
        )
        
        await message.reply(colored_json, parse_mode="HTML")

    if not admin_level:
        await update_credits(user_id, -1)
    
    try:
        await log_lookup(user_id, api_type, input_data, json.dumps(raw_data, indent=2))
    except Exception as e:
        logger.error(f"Error logging lookup: {e}")
    
    await update_last_active(user_id)

    log_channel = LOG_CHANNELS.get(api_type)
    if log_channel and log_channel != "-1000000000000":
        try:
            username = message.from_user.username or 'N/A'
            user_info = f"👤 User: {user_id} (@{username})"
            
            if should_send_as_file and json_file and os.path.exists(json_file):
                await bot.send_document(
                    chat_id=int(log_channel),
                    document=FSInputFile(json_file, filename=f"{api_type}_{input_data}.json"),
                    caption=(
                        f"📊 <b>Lookup Log - {api_type.upper()}</b>\n\n"
                        f"{user_info}\n"
                        f"🔎 Type: {api_type}\n"
                        f"⌨️ Input: <code>{input_data}</code>\n"
                        f"📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                        f"📊 Size: {json_size} characters\n"
                        f"📄 Format: JSON File"
                    ),
                    parse_mode="HTML"
                )
                
                if txt_file and os.path.exists(txt_file):
                    await bot.send_document(
                        chat_id=int(log_channel),
                        document=FSInputFile(txt_file, filename=f"{api_type}_{input_data}_readable.txt"),
                        caption="📄 Readable Text Format"
                    )
                    
            else:
                log_message = (
                    f"📊 <b>Lookup Log - {api_type.upper()}</b>\n\n"
                    f"{user_info}\n"
                    f"🔎 Type: {api_type}\n"
                    f"⌨️ Input: <code>{input_data}</code>\n"
                    f"📅 Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
                    f"📊 Size: {json_size} characters\n\n"
                    f"📄 Result:\n<pre>{formatted_json[:1500]}</pre>"
                )
                
                if len(formatted_json) > 1500:
                    log_message += "\n... [truncated for log channel]"
                
                await bot.send_message(
                    int(log_channel),
                    log_message,
                    parse_mode="HTML"
                )
                
        except Exception as e:
            logger.error(f"Failed to log to channel: {e}")

    # Cleanup temporary files
    if 'json_file' in locals() and json_file and os.path.exists(json_file):
        try:
            os.unlink(json_file)
        except:
            pass
    
    if txt_file and os.path.exists(txt_file):
        try:
            os.unlink(txt_file)
        except:
            pass

# --- INPUT HANDLERS FOR APIs ---
@dp.callback_query(F.data.startswith("api_"))
async def ask_api_input(callback: types.CallbackQuery, state: FSMContext):
    if await is_user_banned(callback.from_user.id): 
        return
    if not await check_membership(callback.from_user.id):
        await callback.answer("❌ Join channels first!", show_alert=True)
        return
    
    api_type = callback.data.split('_')[1]
    
    if api_type not in APIS or not APIS[api_type]:
        await callback.answer("❌ This service is temporarily unavailable", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_api_input)
    await state.update_data(api_type=api_type)
    
    api_map = {
        'num': "📱 Enter Mobile Number (10 digits)",
        'ifsc': "🏦 Enter IFSC Code (11 characters)",
        'email': "📧 Enter Email Address",
        'gst': "📋 Enter GST Number (15 characters)",
        'vehicle': "🚗 Enter Vehicle RC Number",
        'pincode': "📮 Enter Pincode (6 digits)",
        'instagram': "📷 Enter Instagram Username (without @)",
        'github': "🐱 Enter GitHub Username",
        'pakistan': "🇵🇰 Enter Pakistan Mobile Number (with country code)",
        'ip': "🌐 Enter IP Address",
        'ff_info': "🔥 Enter Free Fire UID",
        'ff_ban': "🚫 Enter Free Fire UID for Ban Check"
    }
    
    if api_type in api_map:
        await callback.message.answer(
            f"<b>{api_map[api_type]}</b>\n\n"
            f"<i>Type /cancel to cancel</i>\n\n"
            f"📄 <i>Note: Large responses will be sent as files</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_api")]
            ])
        )

@dp.callback_query(F.data == "cancel_api")
async def cancel_api_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

# --- FIXED BROADCAST HANDLER ---
@dp.message(Form.waiting_for_broadcast)
async def broadcast_message(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        await state.clear()
        return
    
    try:
        users = await get_all_users()
        sent = 0
        failed = 0
        status = await message.answer("🚀 Broadcasting to all users...")
        
        # Limit broadcast to prevent timeout on Render
        broadcast_limit = 100 if RENDER else 1000
        
        for uid in users[:broadcast_limit]:
            try:
                await message.copy_to(uid)
                sent += 1
                await asyncio.sleep(0.1)  # Increased delay for Render
            except Exception as e:
                failed += 1
        
        await status.edit_text(
            f"✅ <b>Broadcast Complete!</b>\n\n"
            f"✅ Sent: <b>{sent}</b>\n"
            f"❌ Failed: <b>{failed}</b>\n"
            f"👥 Total Users: <b>{len(users)}</b>\n"
            f"📊 Limited to first {broadcast_limit} users for stability",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await message.answer(f"❌ Broadcast failed: {str(e)}")
    
    await state.clear()

# --- MESSAGE HANDLER FOR ALL INPUTS ---
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_inputs(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if await is_user_banned(user_id): 
        return
    
    current_state = await state.get_state()
    
    if current_state == Form.waiting_for_api_input.state:
        data = await state.get_data()
        api_type = data.get('api_type')
        
        if api_type:
            await process_api_call(message, api_type, message.text.strip())
        await state.clear()
        return
    
    elif current_state == Form.waiting_for_redeem.state:
        code = message.text.strip().upper()
        result = await redeem_code_db(user_id, code)
        
        if isinstance(result, int):
            user_data = await get_user(user_id)
            new_balance = (user_data[2] if user_data and len(user_data) > 2 else 0) + result
            await message.answer(
                f"✅ <b>Code Redeemed Successfully!</b>\n"
                f"➕ <b>{result} Credits</b> added to your account.\n\n"
                f"💰 <b>New Balance:</b> {new_balance}",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "already_claimed":
            await message.answer(
                "❌ <b>You have already claimed this code!</b>\n"
                "Each user can claim a code only once.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "invalid":
            await message.answer(
                "❌ <b>Invalid Code!</b>\n"
                "Please check the code and try again.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "inactive":
            await message.answer(
                "❌ <b>Code is Inactive!</b>\n"
                "This code has been deactivated by admin.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "limit_reached":
            await message.answer(
                "❌ <b>Code Limit Reached!</b>\n"
                "This code has been used by maximum users.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "expired":
            await message.answer(
                "❌ <b>Code Expired!</b>\n"
                "This code is no longer valid.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        else:
            await message.answer(
                "❌ <b>Error processing code!</b>\n"
                "Please try again later.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        
        await state.clear()
        return
    
    # Direct message states
    elif current_state == Form.waiting_for_dm_user.state:
        try:
            target_id = int(message.text.strip())
            await state.update_data(dm_user_id=target_id)
            await message.answer(f"📨 Now send the message for user {target_id}:")
            await state.set_state(Form.waiting_for_dm_content)
        except:
            await message.answer("❌ Invalid user ID. Please enter a numeric ID.")
        return
    
    elif current_state == Form.waiting_for_dm_content.state:
        data = await state.get_data()
        target_id = data.get('dm_user_id')
        
        if target_id:
            try:
                await message.copy_to(target_id)
                await message.answer(f"✅ Message sent to user {target_id}")
            except Exception as e:
                await message.answer(f"❌ Failed to send message: {str(e)}")
        
        await state.clear()
        return
    
    # Custom code creation state
    elif current_state == Form.waiting_for_custom_code.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 3:
                raise ValueError("Minimum 3 arguments required")
            
            code = parts[0].upper()
            amt = int(parts[1])
            uses = int(parts[2])
            
            expiry_minutes = None
            if len(parts) >= 4:
                expiry_minutes = parse_time_string(parts[3])
            
            await create_redeem_code(code, amt, uses, expiry_minutes)
            
            expiry_text = ""
            if expiry_minutes:
                if expiry_minutes < 60:
                    expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
                else:
                    hours = expiry_minutes // 60
                    mins = expiry_minutes % 60
                    expiry_text = f"⏰ Expires in: {hours}h {mins}m"
            else:
                expiry_text = "⏰ No expiry"
            
            await message.answer(
                f"✅ <b>Code Created!</b>\n\n"
                f"🎫 <b>Code:</b> <code>{code}</code>\n"
                f"💰 <b>Amount:</b> {amt} credits\n"
                f"👥 <b>Max Uses:</b> {uses}\n"
                f"{expiry_text}\n\n"
                f"📝 <i>Note: Each user can claim only once</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(
                f"❌ <b>Error:</b> {str(e)}\n\n"
                f"<b>Format:</b> <code>CODE AMOUNT USES [TIME]</code>\n"
                f"<b>Examples:</b>\n"
                f"• <code>WELCOME50 50 10</code>\n"
                f"• <code>FLASH100 100 5 15m</code>\n"
                f"• <code>SPECIAL200 200 3 1h</code>",
                parse_mode="HTML"
            )
        await state.clear()
        return
    
    # Stats range state
    elif current_state == Form.waiting_for_stats_range.state:
        try:
            days = int(message.text.strip())
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
            
            if not users:
                await message.answer(f"❌ No users found in last {days} days.")
                return
            
            # Use /tmp directory for Render compatibility
            if RENDER:
                temp_dir = "/tmp"
            else:
                temp_dir = tempfile.gettempdir()
            
            temp_file = os.path.join(temp_dir, f"users_{secrets.token_hex(8)}.csv")
            
            with open(temp_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['User ID', 'Username', 'Credits', 'Join Date'])
                for user in users:
                    if len(user) > 3:
                        join_date = datetime.fromtimestamp(float(user[3])).strftime('%Y-%m-%d %H:%M:%S') if user[3] else 'N/A'
                        username = user[1] if len(user) > 1 and user[1] else 'N/A'
                        credits = user[2] if len(user) > 2 else 0
                        writer.writerow([user[0], username, credits, join_date])
            
            await message.reply_document(
                FSInputFile(temp_file),
                caption=f"📊 Users data for last {days} days\nTotal users: {len(users)}"
            )
            
            # Cleanup
            if os.path.exists(temp_file):
                os.unlink(temp_file)
            
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        
        await state.clear()
        return
    
    # Code deactivate state
    elif current_state == Form.waiting_for_code_deactivate.state:
        try:
            code = message.text.strip().upper()
            await deactivate_code(code)
            await message.answer(f"✅ Code <code>{code}</code> has been deactivated.", parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Username search state
    elif current_state == Form.waiting_for_username.state:
        username = message.text.strip()
        user_id_result = await get_user_by_username(username)
        
        if user_id_result:
            user_data = await get_user(user_id_result)
            if user_data:
                msg = (f"👤 <b>User Found</b>\n\n"
                       f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
                       f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
                       f"💰 <b>Credits:</b> {user_data[2] if len(user_data) > 2 else 0}\n"
                       f"📊 <b>Total Earned:</b> {user_data[6] if len(user_data) > 6 else 0}\n"
                       f"🚫 <b>Banned:</b> {'Yes' if len(user_data) > 5 and user_data[5] == 1 else 'No'}")
                await message.answer(msg, parse_mode="HTML")
            else:
                await message.answer("❌ User data not found.")
        else:
            await message.answer("❌ User not found.")
        
        await state.clear()
        return
    
    # Delete user state
    elif current_state == Form.waiting_for_delete_user.state:
        try:
            uid = int(message.text.strip())
            await delete_user(uid)
            await message.answer(f"✅ User {uid} deleted successfully.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Reset credits state
    elif current_state == Form.waiting_for_reset_credits.state:
        try:
            uid = int(message.text.strip())
            await reset_user_credits(uid)
            await message.answer(f"✅ Credits reset for user {uid}.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Code stats state
    elif current_state == Form.waiting_for_code_stats.state:
        try:
            code = message.text.strip().upper()
            stats = await get_code_usage_stats(code)
            
            if stats:
                amount, max_uses, current_uses, unique_users, user_ids = stats
                msg = (f"📊 <b>Code Statistics: {code}</b>\n\n"
                       f"💰 <b>Amount:</b> {amount} credits\n"
                       f"🎯 <b>Uses:</b> {current_uses}/{max_uses}\n"
                       f"👥 <b>Unique Users:</b> {unique_users}\n"
                       f"🆔 <b>Users:</b> {user_ids or 'None'}")
                await message.answer(msg, parse_mode="HTML")
            else:
                await message.answer(f"❌ Code {code} not found.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # User lookups state
    elif current_state == Form.waiting_for_user_lookups.state:
        try:
            uid = int(message.text.strip())
            lookups = await get_user_lookups(uid, 20)
            
            if not lookups:
                await message.answer(f"❌ No lookups found for user {uid}.")
                return
            
            text = f"📊 <b>Recent Lookups for User {uid}</b>\n\n"
            for i, (api_type, input_data, lookup_date) in enumerate(lookups, 1):
                try:
                    date_str = datetime.fromisoformat(lookup_date).strftime('%d/%m %H:%M')
                except:
                    date_str = lookup_date
                text += f"{i}. {api_type.upper()}: {input_data} - {date_str}\n"
            
            if len(text) > 4000:
                # Use /tmp directory for Render compatibility
                if RENDER:
                    temp_dir = "/tmp"
                else:
                    temp_dir = tempfile.gettempdir()
                
                temp_file = os.path.join(temp_dir, f"lookups_{secrets.token_hex(8)}.txt")
                with open(temp_file, 'w', encoding='utf-8') as f:
                    f.write(text)
                
                await message.reply_document(
                    FSInputFile(temp_file),
                    caption=f"Lookup history for user {uid}"
                )
                # Cleanup
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            else:
                await message.answer(text, parse_mode="HTML")
                
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Bulk gift state
    elif current_state == Form.waiting_for_bulk_gift.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 2:
                raise ValueError("Format: AMOUNT USERID1 USERID2 ...")
            
            amount = int(parts[0])
            user_ids = [int(uid) for uid in parts[1:]]
            
            # Limit bulk operations for Render stability
            if RENDER and len(user_ids) > 50:
                await message.answer("⚠️ <b>Render Limit:</b> Maximum 50 users at once for bulk operations.", parse_mode="HTML")
                return
            
            await bulk_update_credits(user_ids, amount)
            
            msg = f"✅ Gifted {amount} credits to {len(user_ids)} users:\n"
            for uid in user_ids[:10]:
                msg += f"• <code>{uid}</code>\n"
            if len(user_ids) > 10:
                msg += f"... and {len(user_ids) - 10} more"
            
            await message.answer(msg, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # User search state
    elif current_state == Form.waiting_for_user_search.state:
        query = message.text.strip()
        users = await search_users(query)
        
        if not users:
            await message.answer("❌ No users found.")
            return
        
        text = f"🔍 <b>Search Results for '{query}'</b>\n\n"
        for user_id, username, credits in users[:15]:
            text += f"🆔 <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
        
        if len(users) > 15:
            text += f"\n... and {len(users) - 15} more results"
        
        await message.answer(text, parse_mode="HTML")
        await state.clear()
        return
    
    # Settings state
    elif current_state == Form.waiting_for_settings.state:
        await message.answer("⚙️ <b>Settings updated!</b>", parse_mode="HTML")
        await state.clear()
        return
    
    # If no state and user sends random text, show menu
    else:
        if message.text.strip():
            await message.answer(
                "Please use the menu buttons to select an option.",
                reply_markup=get_main_menu(user_id)
            )

# Handle media messages in broadcast
@dp.message(Form.waiting_for_broadcast, F.content_type.in_({'photo', 'video', 'audio', 'document'}))
async def broadcast_media(message: types.Message, state: FSMContext):
    # This will be handled by the broadcast_message function
    pass

# --- CANCEL COMMAND ---
@dp.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ No active operation to cancel.")
        return
    
    await state.clear()
    await message.answer("✅ Operation cancelled.", reply_markup=get_main_menu(message.from_user.id))

# --- ENHANCED ADMIN PANEL ---
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    panel_text = "🛠 <b>ADMIN CONTROL PANEL</b>\n\n"
    
    # Add Render info if applicable
    if RENDER:
        panel_text += "🚀 <b>Render Deployment Active</b>\n\n"
    
    # Basic commands for all admins
    panel_text += "<b>📊 User Management:</b>\n"
    panel_text += "📢 <code>/broadcast</code> - Send to all users\n"
    panel_text += "📨 <code>/dm</code> - Direct message to user\n"
    panel_text += "🎁 <code>/gift ID AMOUNT</code> - Add credits\n"
    panel_text += "🎁 <code>/bulkgift AMOUNT ID1 ID2...</code> - Bulk gift\n"
    panel_text += "📉 <code>/removecredits ID AMOUNT</code> - Remove credits\n"
    panel_text += "🔄 <code>/resetcredits ID</code> - Reset user credits to 0\n"
    panel_text += "🚫 <code>/ban ID</code> - Ban user\n"
    panel_text += "🟢 <code>/unban ID</code> - Unban user\n"
    panel_text += "🗑 <code>/deleteuser ID</code> - Delete user\n"
    panel_text += "🔍 <code>/searchuser QUERY</code> - Search users\n"
    panel_text += "👥 <code>/users [PAGE]</code> - List users (10 per page)\n"
    panel_text += "📈 <code>/recentusers DAYS</code> - Recent users\n"
    panel_text += "📊 <code>/userlookups ID</code> - User lookup history\n"
    panel_text += "🏆 <code>/leaderboard</code> - Credits leaderboard\n"
    panel_text += "💰 <code>/premiumusers</code> - Premium users (100+ credits)\n"
    panel_text += "📉 <code>/lowcreditusers</code> - Users with low credits\n"
    panel_text += "⏰ <code>/inactiveusers DAYS</code> - Inactive users\n\n"
    
    # Redeem Code Management
    panel_text += "<b>🎫 Code Management:</b>\n"
    panel_text += "🎲 <code>/gencode AMOUNT USES [TIME]</code> - Random code\n"
    panel_text += "🎫 <code>/customcode CODE AMOUNT USES [TIME]</code> - Custom code\n"
    panel_text += "📋 <code>/listcodes</code> - List all codes\n"
    panel_text += "✅ <code>/activecodes</code> - List active codes\n"
    panel_text += "❌ <code>/inactivecodes</code> - List inactive codes\n"
    panel_text += "🚫 <code>/deactivatecode CODE</code> - Deactivate code\n"
    panel_text += "📊 <code>/codestats CODE</code> - Code usage statistics\n"
    panel_text += "⌛️ <code>/checkexpired</code> - Check expired codes\n"
    panel_text += "🧹 <code>/cleanexpired</code> - Remove expired codes\n\n"
    
    # Statistics
    panel_text += "<b>📈 Statistics:</b>\n"
    panel_text += "📊 <code>/stats</code> - Bot statistics\n"
    panel_text += "📅 <code>/dailystats DAYS</code> - Daily statistics\n"
    panel_text += "🔍 <code>/lookupstats</code> - Lookup statistics\n"
    panel_text += "💾 <code>/backup DAYS</code> - Download user data\n"
    panel_text += "🏆 <code>/topref [LIMIT]</code> - Top referrers\n\n"
    
    # Owner-only commands
    if admin_level == 'owner':
        panel_text += "<b>👑 Owner Commands:</b>\n"
        panel_text += "➕ <code>/addadmin ID</code> - Add admin\n"
        panel_text += "➖ <code>/removeadmin ID</code> - Remove admin\n"
        panel_text += "👥 <code>/listadmins</code> - List all admins\n"
        panel_text += "⚙️ <code>/settings</code> - Bot settings\n"
        panel_text += "💾 <code>/fulldbbackup</code> - Full database backup\n"
        panel_text += "🩺 <code>/dbhealth</code> - Check database health\n"
    
    # Time format examples
    panel_text += "\n<b>⏰ Time Formats:</b>\n"
    panel_text += "• <code>30m</code> = 30 minutes\n"
    panel_text += "• <code>2h</code> = 2 hours\n"
    panel_text += "• <code>1h30m</code> = 1.5 hours\n"
    panel_text += "• <code>1d</code> = 24 hours\n"
    
    # Add quick action buttons
    buttons = [
        [InlineKeyboardButton(text="📊 Quick Stats", callback_data="quick_stats"),
         InlineKeyboardButton(text="👥 Recent Users", callback_data="recent_users")],
        [InlineKeyboardButton(text="🎫 Active Codes", callback_data="active_codes"),
         InlineKeyboardButton(text="🏆 Top Referrers", callback_data="top_ref")],
        [InlineKeyboardButton(text="🚀 Broadcast", callback_data="broadcast_now"),
         InlineKeyboardButton(text="❌ Close", callback_data="close_panel")]
    ]
    
    await message.answer(panel_text, parse_mode="HTML", 
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# --- BROADCAST COMMAND ---
@dp.message(Command("broadcast"))
async def broadcast_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "📢 <b>Send message to broadcast</b> (text, photo, video, audio, document, poll, sticker):\n\n"
        "This will be sent to all users.",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_broadcast)

# --- DIRECT MESSAGE COMMAND ---
@dp.message(Command("dm"))
async def dm_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("👤 <b>Enter user ID to send message:</b>")
    await state.set_state(Form.waiting_for_dm_user)

# --- USERS LIST WITH PAGINATION ---
@dp.message(Command("users"))
async def users_list(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    page = 1
    if command.args and command.args.isdigit():
        page = int(command.args)
    
    users = await get_all_users()
    total_users = len(users)
    per_page = 10
    total_pages = (total_users + per_page - 1) // per_page if total_users > 0 else 1
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
    
    for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
        user_data = await get_user(user_id)
        if user_data:
            text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2] if len(user_data) > 2 else 0} credits\n"
    
    text += f"\nTotal Users: {total_users}"
    
    # Pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
    
    if buttons:
        await message.answer(text, parse_mode="HTML", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    else:
        await message.answer(text, parse_mode="HTML")

# --- SEARCH USER COMMAND ---
@dp.message(Command("searchuser"))
async def search_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔍 <b>Enter username or user ID to search:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_search)

# --- DELETE USER COMMAND ---
@dp.message(Command("deleteuser"))
async def delete_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🗑 <b>Enter user ID to delete:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_delete_user)

# --- RESET CREDITS COMMAND ---
@dp.message(Command("resetcredits"))
async def reset_credits_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔄 <b>Enter user ID to reset credits:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_reset_credits)

# --- RECENT USERS COMMAND ---
@dp.message(Command("recentusers"))
async def recent_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
    
    text = f"📅 <b>Recent Users (Last {days} days)</b>\n\n"
    
    if not users:
        text += "No users found."
    else:
        for user in users[:20]:
            if len(user) > 3 and user[3]:
                join_date = datetime.fromtimestamp(float(user[3])).strftime('%d-%m-%Y')
                text += f"• <code>{user[0]}</code> - @{user[1] or 'N/A'} - {join_date}\n"
            else:
                text += f"• <code>{user[0]}</code> - @{user[1] or 'N/A'} - N/A\n"
        
        if len(users) > 20:
            text += f"\n... and {len(users) - 20} more"
    
    await message.answer(text, parse_mode="HTML")

# --- ACTIVE CODES COMMAND ---
@dp.message(Command("activecodes"))
async def active_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_active_codes()
    
    if not codes:
        await message.reply("✅ No active codes found.")
        return
    
    text = "✅ <b>Active Redeem Codes</b>\n\n"
    
    for code_data in codes[:10]:
        if len(code_data) >= 4:
            code, amount, max_uses, current_uses = code_data[:4]
            text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more active codes"
    
    await message.reply(text, parse_mode="HTML")

# --- INACTIVE CODES COMMAND ---
@dp.message(Command("inactivecodes"))
async def inactive_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_inactive_codes()
    
    if not codes:
        await message.reply("❌ No inactive codes found.")
        return
    
    text = "❌ <b>Inactive Redeem Codes</b>\n\n"
    
    for code_data in codes[:10]:
        if len(code_data) >= 4:
            code, amount, max_uses, current_uses = code_data[:4]
            text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more inactive codes"
    
    await message.reply(text, parse_mode="HTML")

# --- LEADERBOARD COMMAND ---
@dp.message(Command("leaderboard"))
async def leaderboard_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    leaderboard = await get_leaderboard(10)
    
    if not leaderboard:
        await message.reply("❌ No users found.")
        return
    
    text = "🏆 <b>Credits Leaderboard</b>\n\n"
    
    for i, (user_id, username, credits) in enumerate(leaderboard, 1):
        medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i}."))
        text += f"{medal} <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    await message.reply(text, parse_mode="HTML")

# --- DAILY STATS COMMAND ---
@dp.message(Command("dailystats"))
async def daily_stats_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    stats = await get_daily_stats(days)
    
    text = f"📈 <b>Daily Statistics (Last {days} days)</b>\n\n"
    
    if not stats:
        text += "No statistics available."
    else:
        for date, new_users, lookups in stats:
            text += f"📅 {date}: +{new_users} users, {lookups} lookups\n"
    
    await message.reply(text, parse_mode="HTML")

# --- LOOKUP STATS COMMAND ---
@dp.message(Command("lookupstats"))
async def lookup_stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    total_lookups = await get_total_lookups()
    api_stats = await get_lookup_stats()
    
    text = f"🔍 <b>Lookup Statistics</b>\n\n"
    text += f"📊 <b>Total Lookups:</b> {total_lookups}\n\n"
    
    if api_stats:
        text += "<b>By API Type:</b>\n"
        for api_type, count in api_stats:
            text += f"• {api_type.upper()}: {count} lookups\n"
    
    await message.reply(text, parse_mode="HTML")

# --- USER LOOKUPS COMMAND ---
@dp.message(Command("userlookups"))
async def user_lookups_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔍 <b>Enter user ID to view lookup history:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_lookups)

# --- CODE STATS COMMAND ---
@dp.message(Command("codestats"))
async def code_stats_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("📊 <b>Enter code to view statistics:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_stats)

# --- PREMIUM USERS COMMAND ---
@dp.message(Command("premiumusers"))
async def premium_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    users = await get_premium_users()
    
    if not users:
        await message.reply("❌ No premium users found.")
        return
    
    text = "💰 <b>Premium Users (100+ credits)</b>\n\n"
    
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more premium users"
    
    await message.reply(text, parse_mode="HTML")

# --- LOW CREDIT USERS COMMAND ---
@dp.message(Command("lowcreditusers"))
async def low_credit_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    users = await get_low_credit_users()
    
    if not users:
        await message.reply("✅ No users with low credits.")
        return
    
    text = "📉 <b>Users with Low Credits (≤5 credits)</b>\n\n"
    
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more users"
    
    await message.reply(text, parse_mode="HTML")

# --- INACTIVE USERS COMMAND ---
@dp.message(Command("inactiveusers"))
async def inactive_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 30
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    users = await get_inactive_users(days)
    
    if not users:
        await message.reply(f"✅ No inactive users found (last {days} days).")
        return
    
    text = f"⏰ <b>Inactive Users (Last {days} days)</b>\n\n"
    
    for user_id, username, last_active in users[:15]:
        try:
            last_active_dt = datetime.fromisoformat(last_active)
            days_ago = (datetime.now() - last_active_dt).days
            text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {days_ago} days ago\n"
        except:
            text += f"• <code>{user_id}</code> - @{username or 'N/A'} - N/A\n"
    
    if len(users) > 15:
        text += f"\n... and {len(users) - 15} more inactive users"
    
    await message.reply(text, parse_mode="HTML")

# --- BULK GIFT COMMAND ---
@dp.message(Command("bulkgift"))
async def bulk_gift_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "🎁 <b>Bulk Gift Credits</b>\n\n"
        "Format: <code>AMOUNT USERID1 USERID2 USERID3 ...</code>\n\n"
        "Example: <code>50 123456 789012 345678</code>\n\n"
        "Enter the amount and user IDs separated by spaces:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_bulk_gift)

# --- GIFT CREDITS COMMAND ---
@dp.message(Command("gift"))
async def gift_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        if len(args) < 2:
            raise ValueError("Not enough arguments")
        
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, amt)
        await message.reply(f"✅ Added {amt} credits to user {uid}")
        
        try:
            await bot.send_message(uid, f"🎁 <b>Admin Gifted You {amt} Credits!</b>", parse_mode="HTML")
        except:
            pass
    except Exception as e:
        await message.reply(f"Usage: /gift <user_id> <amount>\nError: {str(e)}")

# --- REMOVE CREDITS COMMAND ---
@dp.message(Command("removecredits"))
async def remove_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        if len(args) < 2:
            raise ValueError("Not enough arguments")
        
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, -amt)
        await message.reply(f"✅ Removed {amt} credits from user {uid}")
        
        try:
            await bot.send_message(uid, f"⚠️ <b>Admin Removed {amt} Credits From Your Account!</b>", parse_mode="HTML")
        except:
            pass
    except Exception as e:
        await message.reply(f"Usage: /removecredits <user_id> <amount>\nError: {str(e)}")

# --- GENERATE CODE COMMAND ---
@dp.message(Command("gencode"))
async def generate_random_code(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        
        if len(args) < 2:
            raise ValueError("Minimum 2 arguments required")
        
        amt = int(args[0])
        uses = int(args[1])
        
        expiry_minutes = None
        if len(args) >= 3:
            expiry_minutes = parse_time_string(args[2])
        
        code = f"PRO-{secrets.token_hex(3).upper()}"
        
        await create_redeem_code(code, amt, uses, expiry_minutes)
        
        expiry_text = ""
        if expiry_minutes:
            if expiry_minutes < 60:
                expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
            else:
                hours = expiry_minutes // 60
                mins = expiry_minutes % 60
                expiry_text = f"⏰ Expires in: {hours}h {mins}m"
        else:
            expiry_text = "⏰ No expiry"
        
        await message.reply(
            f"✅ <b>Code Created!</b>\n\n"
            f"🎫 <b>Code:</b> <code>{code}</code>\n"
            f"💰 <b>Amount:</b> {amt} credits\n"
            f"👥 <b>Max Uses:</b> {uses}\n"
            f"{expiry_text}\n\n"
            f"📝 <i>Note: Each user can claim only once</i>",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.reply(
            f"❌ <b>Usage:</b> <code>/gencode AMOUNT USES [TIME]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"• <code>/gencode 50 10</code> - No expiry\n"
            f"• <code>/gencode 100 5 30m</code> - 30 minutes expiry\n"
            f"• <code>/gencode 200 3 2h</code> - 2 hours expiry\n"
            f"• <code>/gencode 500 1 1h30m</code> - 1.5 hours expiry\n\n"
            f"<b>Error:</b> {str(e)}",
            parse_mode="HTML"
        )

# --- CUSTOM CODE COMMAND ---
@dp.message(Command("customcode"))
async def custom_code_command(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "🎫 <b>Enter code details:</b>\n"
        "Format: <code>CODE AMOUNT USES [TIME]</code>\n\n"
        "Examples:\n"
        "• <code>WELCOME50 50 10</code>\n"
        "• <code>FLASH100 100 5 15m</code>\n"
        "• <code>SPECIAL200 200 3 1h</code>\n\n"
        "Time formats: 30m, 2h, 1h30m",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_custom_code)

# --- LIST CODES COMMAND ---
@dp.message(Command("listcodes"))
async def list_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_all_codes()
    
    if not codes:
        await message.reply("❌ No redeem codes found.")
        return
    
    text = "🎫 <b>All Redeem Codes</b>\n\n"
    
    for code_data in codes:
        if len(code_data) >= 7:
            code, amount, max_uses, current_uses, expiry_minutes, created_date, is_active = code_data[:7]
            
            status = "✅ Active" if is_active else "❌ Inactive"
            
            expiry_text = ""
            if expiry_minutes:
                try:
                    created_dt = datetime.fromisoformat(created_date)
                    expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
                    
                    if expiry_dt > datetime.now():
                        time_left = expiry_dt - datetime.now()
                        hours = time_left.seconds // 3600
                        minutes = (time_left.seconds % 3600) // 60
                        expiry_text = f"⏳ {hours}h {minutes}m left"
                    else:
                        expiry_text = "⌛️ Expired"
                except:
                    expiry_text = "⏰ Expiry N/A"
            else:
                expiry_text = "♾️ No expiry"
            
            try:
                created_str = datetime.fromisoformat(created_date).strftime('%d/%m/%y %H:%M')
            except:
                created_str = created_date
            
            text += (
                f"🎟 <b>{code}</b> ({status})\n"
                f"💰 Amount: {amount} | 👥 Uses: {current_uses}/{max_uses}\n"
                f"{expiry_text}\n"
                f"📅 Created: {created_str}\n"
                f"{'-'*30}\n"
            )
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await message.answer(part, parse_mode="HTML")
    else:
        await message.reply(text, parse_mode="HTML")

# --- DEACTIVATE CODE COMMAND ---
@dp.message(Command("deactivatecode"))
async def deactivate_code_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("❌ <b>Enter code to deactivate:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_deactivate)

# --- CHECK EXPIRED COMMAND ---
@dp.message(Command("checkexpired"))
async def check_expired_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    expired = await get_expired_codes()
    
    if not expired:
        await message.reply("✅ No expired codes found.")
        return
    
    text = "⌛️ <b>Expired Codes</b>\n\n"
    
    for code_data in expired:
        if len(code_data) >= 6:
            code, amount, current_uses, max_uses, expiry_minutes, created_date = code_data[:6]
            
            try:
                created_dt = datetime.fromisoformat(created_date)
                expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
                expiry_str = expiry_dt.strftime('%d/%m/%y %H:%M')
            except:
                expiry_str = "N/A"
            
            text += (
                f"🎟 <code>{code}</code>\n"
                f"💰 Amount: {amount} | 👥 Used: {current_uses}/{max_uses}\n"
                f"⏰ Expired on: {expiry_str}\n"
                f"{'-'*20}\n"
            )
    
    text += f"\nTotal: {len(expired)} expired codes"
    await message.reply(text, parse_mode="HTML")

# --- BAN USER COMMAND ---
@dp.message(Command("ban"))
async def ban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        if not command.args:
            raise ValueError("No user ID provided")
        
        uid = int(command.args)
        await set_ban_status(uid, 1)
        await message.reply(f"🚫 User {uid} banned.")
    except Exception as e:
        await message.reply(f"Usage: /ban <user_id>\nError: {str(e)}")

# --- UNBAN USER COMMAND ---
@dp.message(Command("unban"))
async def unban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        if not command.args:
            raise ValueError("No user ID provided")
        
        uid = int(command.args)
        await set_ban_status(uid, 0)
        await message.reply(f"🟢 User {uid} unbanned.")
    except Exception as e:
        await message.reply(f"Usage: /unban <user_id>\nError: {str(e)}")

# --- STATS COMMAND ---
@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        stats = await get_bot_stats()
        top_ref = await get_top_referrers(5)
        total_lookups = await get_total_lookups()
        
        stats_text = f"📊 <b>Bot Statistics</b>\n\n"
        stats_text += f"👥 <b>Total Users:</b> {stats.get('total_users', 0)}\n"
        stats_text += f"📈 <b>Active Users:</b> {stats.get('active_users', 0)}\n"
        stats_text += f"💰 <b>Total Credits in System:</b> {stats.get('total_credits', 0)}\n"
        stats_text += f"🎁 <b>Credits Distributed:</b> {stats.get('credits_distributed', 0)}\n"
        stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
        
        if top_ref:
            stats_text += "🏆 <b>Top 5 Referrers:</b>\n"
            for i, (ref_id, count) in enumerate(top_ref, 1):
                stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
        
        await message.reply(stats_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply(f"❌ Error getting statistics: {str(e)}")

# --- BACKUP COMMAND ---
@dp.message(Command("backup"))
async def backup_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("📅 <b>Enter number of days for data:</b>\n"
                       "Example: 7 (for last 7 days)\n"
                       "0 for all data")
    await state.set_state(Form.waiting_for_stats_range)

# --- TOP REFERRERS COMMAND ---
@dp.message(Command("topref"))
async def top_ref_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    limit = 10
    if command.args and command.args.isdigit():
        limit = int(command.args)
    
    top_ref = await get_top_referrers(limit)
    
    if not top_ref:
        await message.reply("❌ No referrals yet.")
        return
    
    text = f"🏆 <b>Top {limit} Referrers</b>\n\n"
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await message.reply(text, parse_mode="HTML")

# --- CLEAN EXPIRED COMMAND ---
@dp.message(Command("cleanexpired"))
async def clean_expired_cmd(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    
    expired = await get_expired_codes()
    
    if not expired:
        await message.reply("✅ No expired codes found.")
        return
    
    deleted = 0
    for code_data in expired:
        try:
            if code_data:
                await delete_redeem_code(code_data[0])
                deleted += 1
        except Exception as e:
            logger.error(f"Error deleting code: {e}")
    
    await message.reply(f"🧹 Cleaned {deleted} expired codes.")

# --- ADD ADMIN COMMAND ---
@dp.message(Command("addadmin"))
async def add_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        if not command.args:
            raise ValueError("No user ID provided")
        
        uid = int(command.args)
        await add_admin(uid)
        await message.reply(f"✅ User {uid} added as admin.")
    except Exception as e:
        await message.reply(f"Usage: /addadmin <user_id>\nError: {str(e)}")

# --- REMOVE ADMIN COMMAND ---
@dp.message(Command("removeadmin"))
async def remove_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        if not command.args:
            raise ValueError("No user ID provided")
        
        uid = int(command.args)
        if uid == OWNER_ID:
            await message.reply("❌ Cannot remove owner!")
            return
        
        await remove_admin(uid)
        await message.reply(f"✅ Admin {uid} removed.")
    except Exception as e:
        await message.reply(f"Usage: /removeadmin <user_id>\nError: {str(e)}")

# --- LIST ADMINS COMMAND ---
@dp.message(Command("listadmins"))
async def list_admins_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    admins = await get_all_admins()
    
    text = "👥 <b>Admin List</b>\n\n"
    
    text += f"👑 <b>Owner:</b> <code>{OWNER_ID}</code>\n\n"
    
    if ADMIN_IDS:
        text += "⚙️ <b>Static Admins:</b>\n"
        for admin_id in ADMIN_IDS:
            if admin_id != OWNER_ID:
                text += f"• <code>{admin_id}</code>\n"
    
    if admins:
        text += "\n🗃️ <b>Database Admins:</b>\n"
        for user_id, level in admins:
            text += f"• <code>{user_id}</code> - {level}\n"
    
    await message.reply(text, parse_mode="HTML")

# --- SETTINGS COMMAND ---
@dp.message(Command("settings"))
async def settings_cmd(message: types.Message, state: FSMContext):
    if not await is_user_owner(message.from_user.id):
        return
    
    await message.answer(
        "⚙️ <b>Bot Settings</b>\n\n"
        "1. Change bot name\n"
        "2. Update API endpoints\n"
        "3. Modify channel settings\n"
        "4. Adjust credit settings\n\n"
        "Enter setting number to modify:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_settings)

# --- DATABASE HEALTH CHECK COMMAND ---
@dp.message(Command("dbhealth"))
async def db_health_cmd(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        health_status, message_text = await check_database_health()
        if health_status:
            await message.reply(f"✅ <b>Database Health Check:</b>\n{message_text}", parse_mode="HTML")
        else:
            await message.reply(f"❌ <b>Database Health Check Failed:</b>\n{message_text}", parse_mode="HTML")
    except Exception as e:
        await message.reply(f"❌ Error checking database health: {str(e)}")

# --- FULL DATABASE BACKUP COMMAND ---
@dp.message(Command("fulldbbackup"))
async def full_db_backup(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        # Use /tmp directory for Render compatibility
        if RENDER:
            backup_path = f"/tmp/{backup_name}"
            db_path = "/tmp/nullprotocol.db"
        else:
            backup_path = backup_name
            db_path = "nullprotocol.db"
        
        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)
            
            await message.reply_document(
                FSInputFile(backup_path),
                caption="💾 Full database backup"
            )
            
            # Cleanup
            if os.path.exists(backup_path):
                os.remove(backup_path)
        else:
            await message.reply("❌ Database file not found.")
    except Exception as e:
        await message.reply(f"❌ Backup failed: {str(e)}")

# --- ADMIN CALLBACK QUERIES ---
@dp.callback_query(F.data == "quick_stats")
async def quick_stats_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    try:
        stats = await get_bot_stats()
        top_ref = await get_top_referrers(3)
        total_lookups = await get_total_lookups()
        
        stats_text = f"📊 <b>Quick Stats</b>\n\n"
        stats_text += f"👥 <b>Total Users:</b> {stats.get('total_users', 0)}\n"
        stats_text += f"📈 <b>Active Users:</b> {stats.get('active_users', 0)}\n"
        stats_text += f"💰 <b>Total Credits:</b> {stats.get('total_credits', 0)}\n"
        stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
        
        if top_ref:
            stats_text += "🏆 <b>Top 3 Referrers:</b>\n"
            for i, (ref_id, count) in enumerate(top_ref, 1):
                stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
        
        await callback.message.edit_text(stats_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in quick_stats: {e}")
        await callback.answer("❌ Error loading stats!", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == "close_panel")
async def close_panel_callback(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.answer()

@dp.callback_query(F.data == "recent_users")
async def recent_users_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    users = await get_recent_users(10)
    
    text = "📅 <b>Recent Users (Last 10)</b>\n\n"
    
    if not users:
        text += "No recent users."
    else:
        for user_id, username, joined_date in users:
            if joined_date:
                try:
                    join_dt = datetime.fromtimestamp(float(joined_date))
                    text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {join_dt.strftime('%d/%m %H:%M')}\n"
                except:
                    text += f"• <code>{user_id}</code> - @{username or 'N/A'} - N/A\n"
            else:
                text += f"• <code>{user_id}</code> - @{username or 'N/A'} - N/A\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "active_codes")
async def active_codes_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    codes = await get_active_codes()
    
    if not codes:
        await callback.answer("✅ No active codes found.", show_alert=True)
        return
    
    text = "✅ <b>Active Codes</b>\n\n"
    
    for code, amount, max_uses, current_uses in codes[:5]:
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 5:
        text += f"\n... and {len(codes) - 5} more"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "top_ref")
async def top_ref_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    top_ref = await get_top_referrers(5)
    
    if not top_ref:
        await callback.answer("❌ No referrals yet.", show_alert=True)
        return
    
    text = "🏆 <b>Top 5 Referrers</b>\n\n"
    
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "broadcast_now")
async def broadcast_now_callback(callback: types.CallbackQuery, state: FSMContext):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    await callback.message.answer("📢 <b>Send message to broadcast:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_broadcast)
    await callback.answer()

# Pagination for users
@dp.callback_query(F.data.startswith("users_"))
async def users_pagination(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    try:
        page = int(callback.data.split("_")[1])
        
        users = await get_all_users()
        total_users = len(users)
        per_page = 10
        total_pages = (total_users + per_page - 1) // per_page if total_users > 0 else 1
        
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
        
        for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
            user_data = await get_user(user_id)
            if user_data:
                text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2] if len(user_data) > 2 else 0} credits\n"
        
        text += f"\nTotal Users: {total_users}"
        
        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
        
        await callback.message.edit_text(text, parse_mode="HTML", 
                                       reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    except Exception as e:
        logger.error(f"Error in users_pagination: {e}")
        await callback.answer("❌ Error!", show_alert=True)
    await callback.answer()

# --- WEBHOOK SUPPORT FOR RENDER ---
async def on_startup():
    """Startup function for webhook mode"""
    try:
        await init_db()
        
        # Initialize static admins
        for admin_id in ADMIN_IDS:
            if admin_id != OWNER_ID:
                try:
                    await add_admin(admin_id)
                except Exception as e:
                    logger.error(f"Error adding admin {admin_id}: {e}")
        
        # Set webhook if using webhook mode
        if BASE_WEBHOOK_URL:
            webhook_url = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"
            await bot.set_webhook(
                webhook_url,
                drop_pending_updates=True
            )
            logger.info(f"Webhook set to: {webhook_url}")
        
        logger.info("🚀 OSINT LOOKUP Pro Bot Started...")
        logger.info(f"👑 Owner ID: {OWNER_ID}")
        logger.info(f"👥 Static Admins: {ADMIN_IDS}")
        logger.info(f"🔍 APIs Loaded: {len([k for k, v in APIS.items() if v])}")
        logger.info(f"📊 Log Channels: {len([k for k, v in LOG_CHANNELS.items() if v and v != '-1000000000000'])}")
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info("📱 /start - Start the bot")
        logger.info("🛠️ /admin - Admin panel")
        logger.info("❌ /cancel - Cancel current operation")
        logger.info("📄 Large responses will be sent as files automatically")
        logger.info("📊 Log channels will receive files for large data")
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
    except Exception as e:
        logger.error(f"Error in startup: {e}")

async def on_shutdown():
    """Shutdown function"""
    logger.info("Shutting down bot...")
    
    # Delete webhook if using webhook mode
    if BASE_WEBHOOK_URL:
        await bot.delete_webhook()
    
    await bot.session.close()

# --- MAIN FUNCTION WITH RENDER SUPPORT ---
async def main():
    """Main function with support for both polling and webhook modes"""
    try:
        # Initialize database
        await init_db()
        
        # Initialize static admins
        for admin_id in ADMIN_IDS:
            if admin_id != OWNER_ID:
                try:
                    await add_admin(admin_id)
                except Exception as e:
                    logger.error(f"Error adding admin {admin_id}: {e}")
        
        print("🚀 OSINT LOOKUP Pro Bot Started...")
        print(f"👑 Owner ID: {OWNER_ID}")
        print(f"👥 Static Admins: {ADMIN_IDS}")
        print(f"🔍 APIs Loaded: {len([k for k, v in APIS.items() if v])}")
        print(f"📊 Log Channels: {len([k for k, v in LOG_CHANNELS.items() if v and v != '-1000000000000'])}")
        print(f"🌐 Mode: {'Webhook' if BASE_WEBHOOK_URL else 'Polling'}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("📱 /start - Start the bot")
        print("🛠️ /admin - Admin panel")
        print("❌ /cancel - Cancel current operation")
        print("📄 Large responses will be sent as files automatically")
        print("📊 Log channels will receive files for large data")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Run database maintenance periodically
        if RENDER:
            asyncio.create_task(periodic_maintenance())
        
        # Start bot in appropriate mode
        if BASE_WEBHOOK_URL:
            # Webhook mode for Render
            from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
            from aiohttp import web
            
            app = web.Application()
            webhook_requests_handler = SimpleRequestHandler(
                dispatcher=dp,
                bot=bot,
                secret_token=os.getenv("WEBHOOK_SECRET", "")
            )
            
            webhook_requests_handler.register(app, path=WEBHOOK_PATH)
            setup_application(app, dp, bot=bot)
            
            # Add health check endpoint
            async def health_check(request):
                return web.Response(text="Bot is running 🟢")
            
            app.router.add_get("/health", health_check)
            app.router.add_get("/", health_check)
            
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, WEB_SERVER_HOST, PORT)
            
            await site.start()
            logger.info(f"Bot started in webhook mode on {WEB_SERVER_HOST}:{PORT}")
            
            # Keep running
            await asyncio.Event().wait()
            
        else:
            # Polling mode for local development
            await dp.start_polling(bot)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"❌ Bot failed to start: {e}")

async def periodic_maintenance():
    """Run periodic maintenance tasks for Render"""
    while True:
        try:
            # Run maintenance every 6 hours
            await asyncio.sleep(6 * 60 * 60)
            logger.info("Running periodic database maintenance...")
            await render_database_maintenance()
        except Exception as e:
            logger.error(f"Error in periodic maintenance: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")