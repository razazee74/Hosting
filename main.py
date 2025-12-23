import asyncio
import logging
import os
import subprocess
import json
import uuid
import time
import threading
import queue
from datetime import datetime, timedelta, timezone
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot import asyncio_filters
from typing import Dict, Tuple, Union

# --- Configuration (UPDATE THESE) ---
BOT_TOKEN = '8229476478:AAEmHc1UYpVlxc_XBbW2kDOxCwYR5URFHco' 
OWNER_ID = 7900401296 
HOSTING_DIR = 'hosted_files'
APPROVED_USERS_FILE = 'approved_users.json'
DB_FILE = "user_scripts_db.json" 

running_processes: Dict[str, Tuple[subprocess.Popen, queue.Queue]] = {}
approved_users = {}
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR # Only ERROR will be shown in terminal
)
logger = logging.getLogger(__name__)
bot = AsyncTeleBot(BOT_TOKEN)

def get_file_path(user_id: int, filename: str) -> str:
    """Returns file path under user's directory."""
    user_dir = os.path.join(HOSTING_DIR, str(user_id))
    if not os.path.exists(user_dir):
        os.makedirs(user_dir)
    return os.path.join(user_dir, filename)

def safe_terminate_process(uid: str) -> bool:
    """Safely terminates a running process (subprocess.Popen)."""
    if uid in running_processes:
        process, q = running_processes[uid]
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                
        del running_processes[uid]
        logger.warning(f"Process for UID {uid} terminated and removed from tracking.")
        return True
    return False

def terminate_all_user_scripts(user_id: int, db_data: dict) -> int:
    """Terminates all running scripts and deletes files for a given user."""
    terminated_count = 0
    uids_to_delete = list(db_data.get("hosted_scripts", {}).keys())
    
    for uid in uids_to_delete:
        # 1. Terminate running process
        safe_terminate_process(uid)
        
        # 2. Delete DB record and file (using DB manager's method)
        if db_manager.delete_script(user_id, uid):
             terminated_count += 1
             
    return terminated_count

def generate_custom_uid():
    """Generates a unique ID in the format R###J###."""
    while True:
        new_uid = f"R{uuid.uuid4().hex[:3].upper()}J{uuid.uuid4().hex[3:6].upper()}"
        is_unique = True
        
        if 'db_manager' in globals():
            for user_data in db_manager.data.values():
                if new_uid in user_data.get("hosted_scripts", {}):
                    is_unique = False
                    break
        if is_unique:
            return new_uid

class ScriptDB:
    """Manages script data persistence and user states using a JSON file."""
    def __init__(self):
        self._ensure_db_exists()
        self._load_data()

    def _ensure_db_exists(self):
        """Ensures the database file and hosting directory exist."""
        if not os.path.exists(DB_FILE):
            with open(DB_FILE, 'w') as f:
                json.dump({}, f)
        if not os.path.exists(HOSTING_DIR):
            os.makedirs(HOSTING_DIR)

    def _load_data(self):
        """Loads data from the JSON file."""
        try:
            with open(DB_FILE, 'r') as f:
                self.data = json.load(f)
        except json.JSONDecodeError:
             logger.error("DB file is corrupt. Resetting to empty dictionary.")
             self.data = {}
        except FileNotFoundError:
             self.data = {}
             
    def _save_data(self):
        """Saves current data back to the JSON file."""
        with open(DB_FILE, 'w') as f:
            json.dump(self.data, f, indent=4)
    
    def all_users_data(self):
        """Returns the entire data dictionary."""
        return self.data

    def generate_uid(self):
        """Generates a unique ID in the custom format."""
        return generate_custom_uid()

    def get_user_data(self, user_id):
        """Returns data for a specific user."""
        user_id_str = str(user_id)
        if user_id_str not in self.data:
            self.data[user_id_str] = {
                "current_process": "IDLE",
                "pending_file_name": None, 
                "pending_file_name_on_disk": None, 
                "pending_update_uid": None,
                "hosted_scripts": {}
            }
            self._save_data()
        return self.data[user_id_str]

    def update_user_state(self, user_id, key, value):
        """Updates a key in the user's state."""
        user_id_str = str(user_id)
        if user_id_str in self.data:
            self.data[user_id_str][key] = value
            self._save_data()

    def get_script_by_uid(self, user_id, uid):
        """Fetches a script's data using its UID."""
        user_data = self.get_user_data(user_id)
        return user_data["hosted_scripts"].get(uid)

    def update_script_data(self, user_id, uid, key, value):
        """Updates a specific property of a hosted script."""
        user_data = self.get_user_data(user_id)
        if uid in user_data["hosted_scripts"]:
            user_data["hosted_scripts"][uid][key] = value
            self._save_data()
            return True
        return False
        
    def add_new_script(self, user_id, uid, display_name, file_name): 
        """Adds a new hosted script entry."""
        user_data = self.get_user_data(user_id)
        user_data["hosted_scripts"][uid] = {
            "display_name": display_name,
            "file_name": file_name, # This is the safe UID.py name
            "status": "Running",
            "process_id": 0 # Restore PID tracking for subprocess
        }
        self._save_data()

    def delete_script(self, user_id, uid):
        """Deletes a script entry and its associated file."""
        user_data = self.get_user_data(user_id)
        if uid in user_data["hosted_scripts"]:
            script_data = user_data["hosted_scripts"].pop(uid)
            self._save_data()
            
            # Delete physical file
            file_path = get_file_path(user_id, script_data['file_name'])
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.warning(f"File {file_path} deleted for UID {uid}.")
            return True
        return False


db_manager = ScriptDB()

def load_approved_users():
    """Loads approved users from JSON file."""
    global approved_users
    if os.path.exists(APPROVED_USERS_FILE):
        try:
            with open(APPROVED_USERS_FILE, 'r') as f:
                data = json.load(f)
                approved_users = {}
                for k, v in data.items():
                    user_id = int(k)
                    # Handle different format loading for max_scripts compatibility
                    if isinstance(v, dict):
                         # New format: {'expiry': timestamp, 'name': name, 'max_scripts': int}
                        approved_users[user_id] = v
                    elif isinstance(v, list):
                        # Old format: [timestamp, first_name]
                        approved_users[user_id] = {
                            'expiry': v[0], 
                            'name': v[1], 
                            'max_scripts': 1 # Default max_scripts for old users
                        }
                    else:
                        # Very old format: timestamp
                        approved_users[user_id] = {
                            'expiry': v, 
                            'name': 'Unknown User', 
                            'max_scripts': 1 # Default max_scripts
                        }

                # Exclude owner from approved_users list
                if OWNER_ID in approved_users:
                    del approved_users[OWNER_ID]
                    logger.warning("Owner ID removed from approved users list during load.")

                logger.warning(f"Loaded {len(approved_users)} approved users.")
        except Exception as e:
            logger.error(f"Error loading approved users: {e}")

def save_approved_users():
    """Saves approved users to JSON file (NEW DICT FORMAT)."""
    try:
        # Save in the dictionary format: {user_id: {'expiry': timestamp, 'name': name, 'max_scripts': int}}
        data_to_save = {
            str(k): v 
            for k, v in approved_users.items()
        }
        with open(APPROVED_USERS_FILE, 'w') as f:
            json.dump(data_to_save, f)
        logger.warning("Saved approved users to file.")
    except Exception as e:
            logger.error(f"Error saving approved users: {e}")

def is_owner(user_id: int) -> bool:
    """Checks if user is bot owner."""
    return user_id == OWNER_ID

def is_authorized(user_id: int) -> bool:
    """Checks if user is authorized."""
    # Owner is always authorized
    if is_owner(user_id):
        return True
    
    # Check approved users
    if user_id in approved_users:
        expiration_time = datetime.fromtimestamp(approved_users[user_id]['expiry'], tz=timezone.utc)
        if expiration_time > datetime.now(timezone.utc):
            return True
        else:
            logger.warning(f"Access expired for user ID: {user_id}. Removing...")
            del approved_users[user_id]
            save_approved_users() 
            return False
    return False

def parse_time_duration(duration_str: str) -> timedelta | None:
    """Parses duration string like '1h', '3d', '2w', '5m'."""
    duration_str = duration_str.lower()
    if not duration_str or len(duration_str) < 2:
        return None
        
    try:
        num = float(duration_str[:-1])
    except ValueError:
        return None
        
    unit = duration_str[-1]

    if unit == 'h':
        return timedelta(hours=num)
    elif unit == 'd':
        return timedelta(days=num)
    elif unit == 'w':
        return timedelta(weeks=num)
    elif unit == 'm':
        # Added minutes support
        return timedelta(minutes=num) 
    else:
        return None

# --- Authorization Filter (Unchanged: kept for other handlers) ---
class AuthFilter(asyncio_filters.AdvancedCustomFilter):
    key = 'is_authorized'
    async def check(self, message, data): 
        return is_authorized(message.from_user.id)

bot.add_custom_filter(AuthFilter())
async def start_script(user_id: int, uid: str, script_data: dict, chat_id: int, silent_start: bool = False):
    """
    Starts a paused or new script using subprocess (Non-Docker).
    If silent_start is True, no Telegram messages are sent for success/failure.
    """
    
    file_name_on_disk = script_data['file_name']
    display_name = script_data['display_name']
    user_dir = os.path.join(HOSTING_DIR, str(user_id))
    local_path = get_file_path(user_id, file_name_on_disk)

    # --- FILE EXISTENCE CHECK ---
    if not os.path.exists(local_path):
        db_manager.update_script_data(user_id, uid, "status", "Error: File Missing")
        if not silent_start:
            error_msg = (
                f"❌ **FATAL ERROR: File Not Found!**\n\n"
                f"Could not find the file for **{display_name}** (`{uid}`).\n"
                f"This record is **corrupt**. Must **Terminate** this UID and Host the file again."
            )
            await bot.send_message(chat_id, error_msg, parse_mode="Markdown")
        return

    if uid in running_processes:
        if not silent_start:
             await bot.send_message(chat_id, f"Error: Script **{display_name}** (`{uid}`) is already being tracked as Running.", parse_mode="Markdown")
        return

    try:
        command = ['python', file_name_on_disk] 
        
        process = subprocess.Popen(
            command,
            cwd=user_dir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        q = queue.Queue()
        reader_thread = threading.Thread(target=read_script_output, args=(process, q), daemon=True)
        reader_thread.start()
        
        # Update Tracking and DB
        running_processes[uid] = (process, q)
        db_manager.update_script_data(user_id, uid, "status", "Running")
        db_manager.update_script_data(user_id, uid, "process_id", process.pid)
        
        # Capture Initial Output (Original 1-second capture logic)
        initial_lines = []
        start = time.time()
        
        while time.time() - start < 1: 
            try:
                line = q.get_nowait()
                initial_lines.append(line)
            except queue.Empty:
                await asyncio.sleep(0.05) 
        
        script_output = "".join(initial_lines).strip()
        
        # Check if it terminated immediately
        if process.poll() is not None:
            safe_terminate_process(uid)
            db_manager.update_script_data(user_id, uid, "status", "Stopped")
            
            if not silent_start:
                final_output = script_output or "No output."
                
                if "error" in script_output.lower() or "exception" in script_output.lower() or process.returncode != 0:
                    await bot.send_message(
                        chat_id,
                        f"❌ **Execution Failed!**\n\nProject **{display_name}** (`{uid}`) failed immediately upon start.\n\n**Error Output:**\n``{final_output}``",
                        parse_mode="Markdown", reply_markup=build_main_keyboard()
                    )
                else:
                    await bot.send_message(
                        chat_id,
                        f"✅ **Execution Complete!**\n\nProject **{display_name}** (`{uid}`) started and finished successfully.\n\n**Final Output:**\n``{final_output}``",
                        parse_mode="Markdown", reply_markup=build_main_keyboard()
                    )

        else:
            if not silent_start:
                output_msg = (
                    f"🚀 **Started Successfully!**\n\nProject **{display_name}** (`{uid}`) is now **Running**.\n\n"
                )
                if script_output:
                     output_msg += f"**Initial Output:**\n``{script_output}``"
                else:
                     output_msg += "✅ Your bot is now online"

                await bot.send_message(chat_id, output_msg, parse_mode="Markdown", reply_markup=build_main_keyboard())

    except Exception as e:
        logger.error(f"Error running script {file_name_on_disk} for user {user_id}: {e}")
        db_manager.update_script_data(user_id, uid, "status", "Error")
        if not silent_start:
            await bot.send_message(chat_id, f"❌ An unexpected error occurred while running the script: ``{e}``", parse_mode="Markdown")
        


# --- File Management Utilities (RESTORED) ---
def read_script_output(process: subprocess.Popen, q: queue.Queue):
    """Reads output from script's stdout."""
    try:
        while process.poll() is None:
             line = process.stdout.readline()
             if line:
                 q.put(line)
             else:
                 break
    except Exception as e:
        logger.debug(f"Reader thread finished for PID {process.pid}: {e}")


# --- Button Helpers (MODIFIED: Added Admin Keyboard) ---
def build_main_keyboard() -> types.ReplyKeyboardMarkup:
    """Main menu Reply Keyboard with 8 buttons."""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    keyboard.row("⚙️ Host", "🔪 Terminate")
    keyboard.row("⏸️ Pause", "🔄 Restart")
    keyboard.row("📃 Saved", "✏️ Update Name")
    keyboard.row("🪝 Pip", "❌ Cancel")
    return keyboard

def build_admin_keyboard() -> types.ReplyKeyboardMarkup:
    """Admin Panel Reply Keyboard."""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    keyboard.row("✅ Approve", "🚫 Unapprove")
    keyboard.row("👥 Approved", "♻️ Renew")
    keyboard.row("📝 List User Scripts", "🔙 Back to Main")
    return keyboard

def build_renew_keyboard() -> types.ReplyKeyboardMarkup:
    """Renew nested keyboard."""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    keyboard.row("⏱️ Renew Time Access", "💻 Renew Script Access")
    keyboard.row("🔙 Back to Admin")
    return keyboard

# --- State Reset (Unchanged) ---
def reset_user_state(user_id: int):
    """Resets user's state to IDLE."""
    db_manager.update_user_state(user_id, "current_process", "IDLE")
    db_manager.update_user_state(user_id, "pending_file_name", None)
    db_manager.update_user_state(user_id, "pending_file_name_on_disk", None) 
    db_manager.update_user_state(user_id, "pending_update_uid", None)
    db_manager.update_user_state(user_id, "admin_target_id", None) # New Admin Target ID
    db_manager.update_user_state(user_id, "admin_target_step", None) # New Admin Step
    # Changed logger level from WARNING to DEBUG for state reset, as ERROR is now the base level
    logger.debug(f"User {user_id} state reset to IDLE.") 

# --- File/Document Handler (Unchanged) ---
def is_project_related_document(message: types.Message):
    """Checks if message is a .py file."""
    if message.content_type != 'document':
        return False
    
    file_name = message.document.file_name
    return file_name.endswith('.py')

# --- Command Handlers (REPLACED/MODIFIED) ---

# REPLACEMENT: Unified start/menu handler for both authorized and unauthorized users.
@bot.message_handler(commands=['start', 'menu'])
async def start_command_combined(message: types.Message):
    """Unified start/menu handler for both authorized and unauthorized users."""
    user_id = message.from_user.id

    # Unauthorized check
    if not is_authorized(user_id):
        keyboard = types.InlineKeyboardMarkup()
        url_button = types.InlineKeyboardButton(text="Gᴇᴛ Aᴄᴄᴇss", url="https://t.me/reurge")
        keyboard.add(url_button)

        await bot.send_message(
            message.chat.id,
            "🚫 **You're not authorised to use this bot**",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        return

    # Authorized (existing /start logic)
    reset_user_state(user_id)
    is_owner_bool = is_owner(user_id)
    owner_commands = ""
    if is_owner_bool:
        owner_commands = (
            "\n**You're the boss — unlimited access granted. 👑**"
        )

    instructions = (
        "Hello! I am your **Python File Host & Manager Bot** 🐍.\n\n"
        "**Use the buttons below to manage your projects.**"
        f"{owner_commands}"
    )
    await bot.send_message(
        message.chat.id,
        instructions,
        parse_mode="Markdown",
        reply_markup=build_main_keyboard()
    )
    
# Handler 3: Owner Admin Panel (Only Owner gets a response)
@bot.message_handler(commands=['adminpanel'], is_authorized=True) 
async def admin_panel_command(message: types.Message):
    user_id_requester = message.from_user.id
    if not is_owner(user_id_requester):
        # Ignore for authorized but non-owner users (No response as requested)
        return

    reset_user_state(user_id_requester)
    db_manager.update_user_state(user_id_requester, "current_process", "ADMIN_PANEL_IDLE")
    
    admin_instructions = (
        "👑 **Here's ya Admin Panel**"
    )
    await bot.send_message(message.chat.id, admin_instructions, parse_mode="Markdown", reply_markup=build_admin_keyboard())

# --- Main Message Handler (FIXED HOST FLOW & MARKDOWN) ---
@bot.message_handler(content_types=['text', 'document'], is_authorized=True)
async def main_message_handler(message: types.Message):
    user_id = message.from_user.id
    user_data = db_manager.get_user_data(user_id)
    state = user_data["current_process"]
    text = message.text
    chat_id = message.chat.id

    is_command = text and text.startswith('/')
    
    # 0. Button Handler (Prioritized)
    
    # --- Main Bot Buttons ---
    if text in ["⚙️ Host", "🔪 Terminate", "⏸️ Pause", "🔄 Restart", "📃 Saved", "✏️ Update Name", "🪝 Pip", "❌ Cancel"]:
        
        if text != "❌ Cancel" and state != "IDLE":
             reset_user_state(user_id)
        
        
        if text == "⚙️ Host":
             # 1. Resource Limit Check (NEW CHECK)
            if not is_owner(user_id) and user_id in approved_users:
                user_script_count = len(user_data.get("hosted_scripts", {}))
                max_scripts = approved_users[user_id].get('max_scripts', 1)
                
                if user_script_count >= max_scripts:
                    await bot.send_message(chat_id, f"❌ **Hosting Limit Reached!**\nYou are currently hosting **{user_script_count}** scripts (Limit: **{max_scripts}**). You can **Terminate** an existing script before hosting a new one.", parse_mode="Markdown", reply_markup=build_main_keyboard())
                    return # Block hosting
            
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_NAME")
            await bot.send_message(chat_id, "Now provide the **name** you want to save/host the script with:", parse_mode="Markdown", reply_markup=build_main_keyboard())
        
        # ... (Terminate, Pause, Restart, Saved, Update Name, Pip, Cancel logic remains the same) ...
        
        elif text == "🔪 Terminate":
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_TERMINATE_UID")
            await bot.send_message(chat_id, "Now provide the **UID** of the script you want to **Terminate** (permanently delete from host).", parse_mode="Markdown", reply_markup=build_main_keyboard())

        elif text == "⏸️ Pause":
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_PAUSE_UID")
            await bot.send_message(chat_id, "Now provide the **UID** of the script you want to **Pause** (stop execution temporarily).", parse_mode="Markdown", reply_markup=build_main_keyboard())

        elif text == "🔄 Restart":
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_RESTART_UID")
            await bot.send_message(chat_id, "Now provide the **UID** of the script you want to **Restart** or **Play**.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        
        elif text == "📃 Saved":
            scripts = user_data["hosted_scripts"]
            
            if not scripts:
                text_msg = "📑 **Your Saved Scripts**:\n\nNo files are currently saved or hosted."
            else:
                text_msg = "📑 **Your Saved Scripts**:\n\n"
                for uid, script in scripts.items():
                    current_status = script['status']
                    
                    if uid in running_processes:
                        process, _ = running_processes[uid]
                        if process.poll() is None:
                            status_icon = "🟢" 
                        else:
                            safe_terminate_process(uid) 
                            db_manager.update_script_data(user_id, uid, "status", "Stopped")
                            status_icon = "⚪"
                    elif current_status == 'Running':
                        status_icon = "🟢" 
                    elif current_status == 'Paused':
                        status_icon = "⏸️"
                    else:
                        status_icon = "⚪"
                        
                    # FIXED: Use single backtick for UID
                    text_msg += f"{status_icon} `{uid}` | **{script['display_name']}**\n"
                    
            await bot.send_message(chat_id, text_msg, parse_mode="Markdown", reply_markup=build_main_keyboard())
        
        elif text == "✏️ Update Name":
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_UPDATE_UID")
            await bot.send_message(chat_id, "Now provide the script's **UID** to change its display name.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            
        elif text == "🪝 Pip": 
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_PIP_PACKAGES")
            await bot.send_message(
                chat_id, 
                "**Install Packages:** Must list the Python packages you need, separated by commas (e.g., ``requests, telebot, pytz``). I will use ``pip install --user`` to install them on the host.",
                parse_mode="Markdown", 
                reply_markup=build_main_keyboard()
            )
        
        elif text == "❌ Cancel":
            if state != "IDLE":
                await bot.send_message(chat_id, "🚫 **Operation Canceled.** Returning to main menu.", parse_mode="Markdown", reply_markup=build_main_keyboard())
                reset_user_state(user_id)
            pass
        
        return
    
    # --- Admin Panel Buttons (Only for Owner) ---
    if is_owner(user_id):
        
        # Admin Panel Level 1 Buttons
        if text in ["✅ Approve", "🚫 Unapprove", "👥 Approved", "♻️ Renew", "📝 List User Scripts", "🔙 Back to Main", "🔙 Back to Admin"]:
            
            # Reset state only if moving from a non-ADMIN_PANEL_IDLE state or explicitly going back
            if state not in ["ADMIN_PANEL_IDLE", "R_WAITING_ACCESS_TYPE"] or text in ["🔙 Back to Main", "🔙 Back to Admin"]:
                 reset_user_state(user_id)
                 
            if text == "🔙 Back to Main":
                await bot.send_message(chat_id, "⬅️ **Returning to Main Menu.**", parse_mode="Markdown", reply_markup=build_main_keyboard())
                return

            if text == "🔙 Back to Admin":
                 # Reset state to ADMIN_PANEL_IDLE
                db_manager.update_user_state(user_id, "current_process", "ADMIN_PANEL_IDLE")
                await bot.send_message(chat_id, "⬅️ **Returning to Admin Panel.**", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                return
            
            db_manager.update_user_state(user_id, "current_process", "ADMIN_PANEL_IDLE") # Ensure base state is IDLE for the panel
                
            if text == "✅ Approve":
                db_manager.update_user_state(user_id, "current_process", "A_WAITING_ID")
                # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, "Now send the **User ID** (e.g., `123456789`) of the person you want to **Approve**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                return
            
            if text == "🚫 Unapprove":
                db_manager.update_user_state(user_id, "current_process", "U_WAITING_ID")
                # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, "Now send the **User ID** (e.g., `123456789`) of the person you want to **Unapprove** and **Terminate All Scripts**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                return

            if text == "👥 Approved":
                if not approved_users:
                    response = "ℹ️ **No users** are currently approved."
                else:
                    response = "📑 **Approved Users:**\n\n"
                    # Sort by expiration time
                    sorted_users = sorted(approved_users.items(), key=lambda item: item[1]['expiry'])
                    
                    for i, (user_id_item, data) in enumerate(sorted_users, 1):
                        expiry_dt = datetime.fromtimestamp(data['expiry'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                        name = data.get('name', 'Unknown User')
                        max_scripts = data.get('max_scripts', 1)
                        
                        # Check for expiration during display
                        if datetime.fromtimestamp(data['expiry'], tz=timezone.utc) <= datetime.now(timezone.utc):
                            status = "🔴 Expired"
                        else:
                            status = f"✅ Expires: **{expiry_dt}**"

                        # FIXED: Use single backtick for User ID for copyability
                        response += f"{i}. **{name}** (`{user_id_item}`)\n   *Scripts Limit: {max_scripts} | {status}*\n"
                        
                await bot.send_message(chat_id, response, parse_mode="Markdown", reply_markup=build_admin_keyboard())
                return

            if text == "♻️ Renew":
                db_manager.update_user_state(user_id, "current_process", "R_WAITING_ACCESS_TYPE")
                await bot.send_message(chat_id, "Which type of access do you want to renew?", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                return

            if text == "📝 List User Scripts":
                db_manager.update_user_state(user_id, "current_process", "L_WAITING_ID")
                # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, "Now send the **User ID** (e.g., `123456789`) whose hosted scripts you want to **List**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                return

        # Admin Panel Level 2 Buttons (Renew)
        if state == "R_WAITING_ACCESS_TYPE":
             if text == "⏱️ Renew Time Access":
                db_manager.update_user_state(user_id, "current_process", "R_TIME_WAITING_ID")
                 # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, "Now send the **User ID** (e.g., `123456789`) for whom you want to **Renew Time Access**.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                return
                
             if text == "💻 Renew Script Access":
                db_manager.update_user_state(user_id, "current_process", "R_SCRIPT_WAITING_ID")
                 # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, "Now send the **User ID** (e.g., `123456789`) for whom you want to **Renew Script Access (Max Scripts)**.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                return
                
             if text not in ["⏱️ Renew Time Access", "💻 Renew Script Access", "🔙 Back to Admin"]:
                 # Ignore other text in this specific state
                 pass


    # --- State Handling (Admin Flows) ---
    
    # A1. Approve - WAITING_ID
    if state == "A_WAITING_ID" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            target_id = int(text.strip())
            if target_id == OWNER_ID:
                 await bot.send_message(chat_id, "ℹ️ Don't need to approve**Owner ID**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                 reset_user_state(user_id)
                 return
                 
            db_manager.update_user_state(user_id, "admin_target_id", target_id)
            db_manager.update_user_state(user_id, "current_process", "A_WAITING_DURATION")
            # FIXED: Use single backtick for User ID
            await bot.send_message(chat_id, f"User ID `{target_id}` set. Now, must send the **duration** (e.g., ``7h``, ``3d``, ``1w``).", parse_mode="Markdown", reply_markup=build_admin_keyboard())
        except ValueError:
            await bot.send_message(chat_id, "Invalid **User ID** format. Must try again.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
            reset_user_state(user_id)
        return
        
    # A2. Approve - WAITING_DURATION
    elif state == "A_WAITING_DURATION" and message.content_type == 'text' and not is_command and is_owner(user_id):
        duration_str = text.strip()
        duration = parse_time_duration(duration_str)
        
        if not duration:
            await bot.send_message(chat_id, "Invalid duration format. Use 'm', 'h', 'd', 'w'.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
            return

        expiration_time = datetime.now(timezone.utc) + duration
        expiration_timestamp = expiration_time.timestamp()
        
        db_manager.update_user_state(user_id, "admin_target_step", expiration_timestamp) # Store duration
        db_manager.update_user_state(user_id, "current_process", "A_WAITING_MAX_SCRIPTS")
        await bot.send_message(chat_id, "Time set. Now, now send the **maximum number of scripts** this user can host (e.g., **2, 5, 10**).", parse_mode="Markdown", reply_markup=build_admin_keyboard())
        return

    # A3. Approve - WAITING_MAX_SCRIPTS (Final step)
    elif state == "A_WAITING_MAX_SCRIPTS" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            max_scripts = int(text.strip())
            if max_scripts <= 0:
                 await bot.send_message(chat_id, "Max scripts must be a **positive number**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                 return
                 
            target_id = user_data["admin_target_id"]
            expiration_timestamp = user_data["admin_target_step"]
            expiration_time = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)

            # Fetch user info (async call)
            try:
                user_info = await bot.get_chat(target_id)
                first_name = user_info.first_name or 'User'
            except Exception:
                first_name = 'Unknown User'
            
            approved_users[target_id] = {
                'expiry': expiration_timestamp, 
                'name': first_name, 
                'max_scripts': max_scripts
            }
            save_approved_users()

            await bot.send_message(
                chat_id,
                # FIXED: Use single backtick for User ID
                f"✅ **{first_name}** (`{target_id}`) approved **successfully**!\n"
                f"Expires: ``{expiration_time.strftime('%Y-%m-%d %H:%M:%S UTC')}``\n"
                f"Max Scripts: **{max_scripts}**",
                parse_mode="Markdown", reply_markup=build_admin_keyboard()
            )
            logger.debug(f"User ID {target_id} approved with max_scripts={max_scripts}.")
            
        except ValueError:
            await bot.send_message(chat_id, "Invalid number format for **Max Scripts**. Can try again.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
        except Exception as e:
            logger.error(f"Error in final approve step: {e}")
            await bot.send_message(chat_id, "An **internal error** occurred during final approval.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
            
        reset_user_state(user_id)
        return
        
    # U1. Unapprove - WAITING_ID (Logic includes termination of all scripts)
    elif state == "U_WAITING_ID" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            target_id = int(text.strip())
            
            if target_id == OWNER_ID:
                await bot.send_message(chat_id, "Cannot **unapprove** the owner.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                reset_user_state(user_id)
                return
                
            if target_id in approved_users:
                # FIXED: Use single backtick for User ID fallback
                user_name = approved_users[target_id].get('name', f'User `{target_id}`') 
                
                # --- Termination Logic ---
                user_db_data = db_manager.get_user_data(target_id)
                terminated_count = terminate_all_user_scripts(target_id, user_db_data)
                
                # --- Remove Approval ---
                del approved_users[target_id]
                save_approved_users()
                
                await bot.send_message(
                    chat_id, 
                    # FIXED: Use single backtick for User ID
                    f"🗑️ **{user_name}** (`{target_id}`) **access revoked**.\n"
                    f"**{terminated_count}** script(s) have been **terminated and deleted**.", 
                    parse_mode="Markdown", reply_markup=build_admin_keyboard()
                )
                logger.debug(f"User ID {target_id} unapproved. {terminated_count} scripts terminated.")
            else:
                 # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, f"❌ User ID `{target_id}` was **not approved**.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
                
        except ValueError:
            await bot.send_message(chat_id, "Invalid **User ID** format.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
        except Exception as e:
            logger.error(f"Error in unapprove command: {e}")
            await bot.send_message(chat_id, "An **internal error** occurred during unapproval.", parse_mode="Markdown", reply_markup=build_admin_keyboard())

        reset_user_state(user_id)
        return

    # L1. List User Scripts - WAITING_ID (Owner sees another user's scripts)
    elif state == "L_WAITING_ID" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            target_id = int(text.strip())
            target_data = db_manager.get_user_data(target_id)
            scripts = target_data["hosted_scripts"]
            
            try:
                # FIXED: Use single backtick for User ID fallback
                user_info = await bot.get_chat(target_id)
                target_name = user_info.first_name or f'User ID `{target_id}`' 
            except Exception:
                # FIXED: Use single backtick for User ID fallback
                target_name = f'User ID `{target_id}`' 
            
            if not scripts:
                text_msg = f"📑 **{target_name}'s Saved Scripts**:\n\nNo files are currently saved or hosted."
            else:
                text_msg = f"📑 **{target_name}'s Saved Scripts**:\n\n"
                for uid, script in scripts.items():
                    current_status = script['status']
                    
                    # Owner's view includes checking live processes
                    if uid in running_processes:
                        status_icon = "🟢" 
                    elif current_status == 'Running':
                        status_icon = "🟢" 
                    elif current_status == 'Paused':
                        status_icon = "⏸️"
                    else:
                        status_icon = "⚪"
                        
                    # FIXED: Use single backtick for UID
                    text_msg += f"{status_icon} `{uid}` | **{script['display_name']}**\n"
                    
            await bot.send_message(chat_id, text_msg, parse_mode="Markdown", reply_markup=build_admin_keyboard())

        except ValueError:
            await bot.send_message(chat_id, "Invalid **User ID** format.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
        except Exception as e:
            logger.error(f"Error in List User Scripts: {e}")
            await bot.send_message(chat_id, "An **internal error** occurred while fetching the list.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
            
        reset_user_state(user_id)
        return

    # R1. Renew Time - WAITING_ID
    elif state == "R_TIME_WAITING_ID" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            target_id = int(text.strip())
            if target_id not in approved_users:
                 # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, f"❌ User ID `{target_id}` is **not currently approved**.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                reset_user_state(user_id)
                return
            
            db_manager.update_user_state(user_id, "admin_target_id", target_id)
            db_manager.update_user_state(user_id, "current_process", "R_TIME_WAITING_DURATION")
             # FIXED: Use single backtick for User ID
            await bot.send_message(chat_id, f"User ID `{target_id}` set. Send the **additional duration** to renew (e.g., ``5d``, ``2w``).", parse_mode="Markdown", reply_markup=build_renew_keyboard())

        except ValueError:
            await bot.send_message(chat_id, "Invalid **User ID** format.Can try again.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
            reset_user_state(user_id)
        return

    # R2. Renew Time - WAITING_DURATION (Final step)
    elif state == "R_TIME_WAITING_DURATION" and message.content_type == 'text' and not is_command and is_owner(user_id):
        duration_str = text.strip()
        duration = parse_time_duration(duration_str)
        
        if not duration:
            await bot.send_message(chat_id, "Invalid duration format. Use 'm', 'h', 'd', 'w'.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
            return

        target_id = user_data["admin_target_id"]
        current_data = approved_users[target_id]
        
        # Calculate new expiry time: If already expired, start from now. If not expired, renew from current expiry.
        current_expiry_dt = datetime.fromtimestamp(current_data['expiry'], tz=timezone.utc)
        start_time = max(current_expiry_dt, datetime.now(timezone.utc))
        new_expiry_time = start_time + duration
        
        current_data['expiry'] = new_expiry_time.timestamp()
        
        save_approved_users()
        
        await bot.send_message(
            chat_id, 
            # FIXED: Use single backtick for User ID
            f"✅ **Time Access Renewed!**\n\nAccess for **{current_data['name']}** (`{target_id}`) extended by ``{duration_str}``.\n"
            f"New Expiry: ``{new_expiry_time.strftime('%Y-%m-%d %H:%M:%S UTC')}``", 
            parse_mode="Markdown", reply_markup=build_admin_keyboard()
        )

        reset_user_state(user_id)
        return

    # R3. Renew Script Access - WAITING_ID
    elif state == "R_SCRIPT_WAITING_ID" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            target_id = int(text.strip())
            if target_id not in approved_users:
                 # FIXED: Use single backtick for User ID
                await bot.send_message(chat_id, f"❌ User ID `{target_id}` is **not currently approved**.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                reset_user_state(user_id)
                return
            
            db_manager.update_user_state(user_id, "admin_target_id", target_id)
            db_manager.update_user_state(user_id, "current_process", "R_SCRIPT_WAITING_MAX")
            current_max = approved_users[target_id].get('max_scripts', 1)
             # FIXED: Use single backtick for User ID
            await bot.send_message(chat_id, f"User ID `{target_id}` set (Current Max: **{current_max}**).\nSend the **NEW total Max Scripts** limit.", parse_mode="Markdown", reply_markup=build_renew_keyboard())

        except ValueError:
            await bot.send_message(chat_id, "Invalid **User ID** format. Can try again.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
            reset_user_state(user_id)
        return

    # R4. Renew Script Access - WAITING_MAX (Final step)
    elif state == "R_SCRIPT_WAITING_MAX" and message.content_type == 'text' and not is_command and is_owner(user_id):
        try:
            new_max_scripts = int(text.strip())
            if new_max_scripts <= 0:
                 await bot.send_message(chat_id, "Max scripts must be a **positive number**.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
                 return
                 
            target_id = user_data["admin_target_id"]
            current_data = approved_users[target_id]
            
            hosted_count = len(db_manager.get_user_data(target_id).get("hosted_scripts", {}))
            
            if new_max_scripts < hosted_count:
                 # FIX: Removed LaTeX and used standard English
                 await bot.send_message(
                     chat_id, 
                     f"❌ Cannot set **Max Scripts** to **{new_max_scripts}**. User is currently hosting **{hosted_count}** scripts. Limit must be **{hosted_count}** or more.", 
                     parse_mode="Markdown", 
                     reply_markup=build_renew_keyboard()
                 )
                 reset_user_state(user_id)
                 return
                 
            current_data['max_scripts'] = new_max_scripts
            
            save_approved_users()

            await bot.send_message(
                chat_id, 
                # FIXED: Use single backtick for User ID
                f"✅ **Script Access Renewed!**\n\nMax Scripts limit for **{current_data['name']}** (`{target_id}`) updated to **{new_max_scripts}**.", 
                parse_mode="Markdown", reply_markup=build_admin_keyboard()
            )

        except ValueError:
            await bot.send_message(chat_id, "Invalid number format for **Max Scripts**. Can try again.", parse_mode="Markdown", reply_markup=build_renew_keyboard())
        except Exception as e:
            logger.error(f"Error in renew max scripts step: {e}")
            await bot.send_message(chat_id, "An **internal error** occurred during renewal.", parse_mode="Markdown", reply_markup=build_admin_keyboard())
            
        reset_user_state(user_id)
        return


    # 1. Host - WAITING_FOR_NAME (File Name Input)
    elif state == "WAITING_FOR_NAME" and message.content_type == 'text' and not is_command: 
        display_name = text.strip()
        if not display_name:
            await bot.send_message(chat_id, "File name cannot be empty. Must provide a name.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            return

        db_manager.update_user_state(user_id, "pending_file_name", display_name)
        # FIX: Correctly setting next state to WAITING_FOR_FILE
        db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_FILE") 
        
        # Fixed Markdown
        await bot.send_message(chat_id, f"Thank you! Now, now send the **Python script (.py file)** that you wish to host as **'{display_name}'**.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        return

    # 2. Host - WAITING_FOR_FILE (Document handler)
    elif state == "WAITING_FOR_FILE" and is_project_related_document(message):
        
        # --- Check Resource Limit Again (Safety Check) ---
        if not is_owner(user_id) and user_id in approved_users:
             user_script_count = len(user_data.get("hosted_scripts", {}))
             max_scripts = approved_users[user_id].get('max_scripts', 1)
             
             if user_script_count >= max_scripts:
                 # This should ideally be caught in the button handler, but kept as a safety guardrail.
                 await bot.send_message(chat_id, f"❌ **Hosting Limit Reached!** You are already hosting {user_script_count}/{max_scripts} scripts.", parse_mode="Markdown", reply_markup=build_main_keyboard())
                 reset_user_state(user_id)
                 return
                 
        display_name = user_data["pending_file_name"]
        
        new_uid = db_manager.generate_uid()
        safe_file_name = f"{new_uid}.py" 
        local_path = get_file_path(user_id, safe_file_name)
        
        try:
            file_info = await bot.get_file(message.document.file_id)
            downloaded_file = await bot.download_file(file_info.file_path)
            with open(local_path, 'wb') as new_file:
                new_file.write(downloaded_file)
        except Exception as e:
            logger.error(f"Error saving file from user {user_id}: {e}")
            await bot.send_message(chat_id, f"❌ Sorry, an error occurred while saving the file: ``{e}``", parse_mode="Markdown", reply_markup=build_main_keyboard())
            reset_user_state(user_id)
            return
            
        # 2.2 Add to DB and Start Script (Use safe_file_name)
        db_manager.add_new_script(user_id, new_uid, display_name, safe_file_name)
        script_data = db_manager.get_script_by_uid(user_id, new_uid)

        await start_script(user_id, new_uid, script_data, chat_id, silent_start=False)
            
        reset_user_state(user_id)
        return

    # 3. Terminate - WAITING_FOR_TERMINATE_UID 
    elif state == "WAITING_FOR_TERMINATE_UID" and message.content_type == 'text' and not is_command:
        uid = text.strip().upper()
        script = db_manager.get_script_by_uid(user_id, uid)
        
        if not script:
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"❌ UID `{uid}` was not found in your saved scripts.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        else:
            safe_terminate_process(uid)
            if db_manager.delete_script(user_id, uid):
                 # FIXED: Use single backtick for UID
                 await bot.send_message(chat_id, f"🗑️ **Successfully Terminated!**\n\nScript **{script['display_name']}** (`{uid}`) has been stopped, and the file has been **permanently deleted** from the host.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            else:
                 # FIXED: Use single backtick for UID
                 await bot.send_message(chat_id, f"❌ Error deleting record for UID `{uid}`. Must contact support.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        
        reset_user_state(user_id)
        return

    # 4. Pause - WAITING_FOR_PAUSE_UID 
    elif state == "WAITING_FOR_PAUSE_UID" and message.content_type == 'text' and not is_command:
        uid = text.strip().upper()
        script = db_manager.get_script_by_uid(user_id, uid)
        
        if not script:
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"❌ UID `{uid}` was not found in your saved scripts.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        elif script['status'] == 'Paused' or script['status'] == 'Stopped' or script['status'].startswith('Error'):
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"ℹ️ Script **{script['display_name']}** (`{uid}`) is already **Paused**, **Stopped** or in an **Error** state.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        elif script['status'] == 'Running':
            if safe_terminate_process(uid):
                db_manager.update_script_data(user_id, uid, "status", "Paused")
                db_manager.update_script_data(user_id, uid, "process_id", 0)
                 # FIXED: Use single backtick for UID
                await bot.send_message(chat_id, f"⏸️ **Successfully Paused!**\n\nScript **{script['display_name']}** (`{uid}`) has been **stopped temporarily**.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            else:
                 db_manager.update_script_data(user_id, uid, "status", "Paused")
                 # FIXED: Use single backtick for UID
                 await bot.send_message(chat_id, f"⚠️ Script **{script['display_name']}** (`{uid}`) was marked as Running but was not found in active processes. Status updated to **Paused**.", parse_mode="Markdown", reply_markup=build_main_keyboard())

        reset_user_state(user_id)
        return
    
    # 5. Restart - WAITING_FOR_RESTART_UID 
    elif state == "WAITING_FOR_RESTART_UID" and message.content_type == 'text' and not is_command:
        uid = text.strip().upper()
        script = db_manager.get_script_by_uid(user_id, uid)
        
        if not script:
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"❌ UID `{uid}` was not found in your saved scripts.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            reset_user_state(user_id)
            return

        is_currently_running = uid in running_processes
        current_db_status = script['status']

         # FIXED: Use single backtick for UID
        if is_currently_running:
            await bot.send_message(chat_id, f"🔄 Restarting Running script: **{script['display_name']}** (`{uid}`)...", parse_mode="Markdown")
            safe_terminate_process(uid) 
            await start_script(user_id, uid, script, chat_id, silent_start=False)
            
        elif current_db_status == 'Running':
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"▶️ Restarting previously running script: **{script['display_name']}** (`{uid}`)...", parse_mode="Markdown")
            await start_script(user_id, uid, script, chat_id, silent_start=False)
            
        elif current_db_status == 'Paused' or current_db_status == 'Stopped' or current_db_status.startswith('Error'): 
            if current_db_status.startswith('Error'):
                 db_manager.update_script_data(user_id, uid, "status", "Stopped") 
            
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"▶️ Starting script: **{script['display_name']}** (`{uid}`)...", parse_mode="Markdown")
            await start_script(user_id, uid, script, chat_id, silent_start=False)

        else:
             # FIXED: Use single backtick for UID and status
             await bot.send_message(chat_id, f"❌ Script **{script['display_name']}** (`{uid}`) is in an unknown state: ``{current_db_status}``. Cannot restart.", parse_mode="Markdown", reply_markup=build_main_keyboard())

        reset_user_state(user_id)
        return

    # 6. Update Name - WAITING_FOR_UPDATE_UID 
    elif state == "WAITING_FOR_UPDATE_UID" and message.content_type == 'text' and not is_command:
        uid = text.strip().upper()
        script = db_manager.get_script_by_uid(user_id, uid)
        
        if script:
            db_manager.update_user_state(user_id, "pending_update_uid", uid)
            db_manager.update_user_state(user_id, "current_process", "WAITING_FOR_NEW_NAME")
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"Must provide the **new name** for UID `{uid}`. (Current: **{script['display_name']}**)", parse_mode="Markdown", reply_markup=build_main_keyboard())
        else:
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"❌ UID `{uid}` was not found in your saved scripts.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            reset_user_state(user_id)
        return

    # 7. Update Name - WAITING_FOR_NEW_NAME 
    elif state == "WAITING_FOR_NEW_NAME" and message.content_type == 'text' and not is_command:
        uid = user_data.get("pending_update_uid")
        new_name = text.strip()
        
        if db_manager.update_script_data(user_id, uid, "display_name", new_name):
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"✅ **Success!** File `{uid}`'s name has been updated to **'{new_name}'**.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        else:
             # FIXED: Use single backtick for UID
            await bot.send_message(chat_id, f"❌ Error changing name. UID `{uid}` might not exist.", parse_mode="Markdown", reply_markup=build_main_keyboard())
            
        reset_user_state(user_id)
        return

    # 8. Pip - WAITING_FOR_PIP_PACKAGES 
    elif state == "WAITING_FOR_PIP_PACKAGES" and message.content_type == 'text' and not is_command:
        packages_str = text.strip()
        packages_list = [p.strip() for p in packages_str.split(',') if p.strip()]
        
        if not packages_list:
             await bot.send_message(chat_id, "❌ Invalid input. Must list packages separated by commas.", parse_mode="Markdown", reply_markup=build_main_keyboard())
             return
             
        install_command = ['pip', 'install', '--user'] + packages_list 
        
        await bot.send_message(chat_id, f"🔄 Installing {len(packages_list)} package(s) on the host using ``pip install --user``. Must wait...", parse_mode="Markdown", reply_markup=build_main_keyboard())

        try:
            result = subprocess.run(
                install_command, 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=300
            )
            
            response_msg = (
                f"✅ **Success!** The following packages are now available to all your scripts on the host:\n"
                f"``{packages_str}``\n\n"
                f"You can now host your script."
            )
            logger.debug(f"Packages installed successfully: {packages_str}")
            await bot.send_message(chat_id, response_msg, parse_mode="Markdown", reply_markup=build_main_keyboard())
            
        except subprocess.CalledProcessError as e:
            error_msg = (
                f"❌ **Installation Failed!**\n\n"
                f"Could not install packages: ``{packages_list}``.\n"
                f"**Error Details:**\n"
                f"``{e.stderr[:500]}``"
            )
            logger.error(f"Package installation failed: {e.stderr}")
            await bot.send_message(chat_id, error_msg, parse_mode="Markdown", reply_markup=build_main_keyboard())

        except subprocess.TimeoutExpired:
            await bot.send_message(chat_id, "❌ **Installation Timeout.** The installation took too long (over 5 minutes) and was canceled.", parse_mode="Markdown", reply_markup=build_main_keyboard())
        except Exception as e:
            logger.error(f"Unexpected error during pip install: {e}")
            await bot.send_message(chat_id, f"❌ An unexpected error occurred: ``{e}``", parse_mode="Markdown", reply_markup=build_main_keyboard())

        reset_user_state(user_id)
        return

    # Fallback for unknown messages in IDLE state (Ignore)
    if message.content_type == 'text' and not is_command and state in ["IDLE", "ADMIN_PANEL_IDLE", "R_WAITING_ACCESS_TYPE"]:
        pass

# Handler 4: Unauthorized User (Ignores all non-/start/menu messages)
# Note: This handler is now for *non*-start/menu messages from unauthorized users.
@bot.message_handler(content_types=['text', 'document'], is_authorized=False)
async def unauthorized_ignore_handler(message: types.Message):
    """Ignores any message from an unauthorized user other than /start or /menu."""
    # Do nothing, bot will not respond to these messages if not authorized.
    pass

# --- Callback Query Handler (Unchanged) ---
@bot.callback_query_handler(func=lambda call: True, is_authorized=True)
async def general_callback_handler(call: types.CallbackQuery):
    """Handles any residual inline button presses and directs the user to the Reply Keyboard."""
    await bot.answer_callback_query(call.id)
    try:
        await bot.edit_message_text("Must use the **Reply Keyboard** buttons.", call.message.chat.id, call.message.message_id, parse_mode="Markdown", reply_markup=None)
    except Exception:
        await bot.send_message(call.message.chat.id, "Must use the **Reply Keyboard** buttons.", parse_mode="Markdown", reply_markup=build_main_keyboard())


# --- Main Application Setup (FINAL FIXES: Auto-Restart for Running Scripts) ---
async def start_bot() -> None:
    """Start the bot using async polling."""
    # Removed logging here as it's handled by main() below
    await bot.polling(non_stop=True, interval=1)

def main() -> None:
    """Entry point."""
    
    # Clean up any leftover processes before starting
    for uid in list(running_processes.keys()):
        safe_terminate_process(uid)
    
    load_approved_users() 

    scripts_to_restart = []
    
    for user_id_str, user_data in db_manager.data.items():
        user_id = int(user_id_str)
        for uid, script in user_data.get("hosted_scripts", {}).items():
             
             if script['status'] == 'Stopped (Process Lost)':
                db_manager.update_script_data(user_id, uid, "status", "Stopped")
                logger.debug(f"Cleaned up old 'Process Lost' status for {uid}.")
             
             if script['status'] == 'Running':
                 scripts_to_restart.append({'user_id': user_id, 'uid': uid, 'script': script})


    try:
        # Print only the required message to the terminal
        print("Bot is running...")
        
        loop = asyncio.get_event_loop()
        bot_task = loop.create_task(start_bot())

        if scripts_to_restart:
            logger.debug(f"Starting auto-restart for {len(scripts_to_restart)} scripts.")
            
            async def run_restarts():
                await asyncio.sleep(5) 
                
                start_time = time.monotonic()

                for item in scripts_to_restart:
                    user_id = item['user_id']
                    uid = item['uid']
                    script = item['script']
                    
                    await start_script(user_id, uid, script, user_id, silent_start=True) 
                    logger.debug(f"Auto-restarted script UID {uid}.")
                
                end_time = time.monotonic()
                logger.debug(f"All {len(scripts_to_restart)} scripts were processed for silent auto-restart in {end_time - start_time:.2f} seconds.")
                
            loop.create_task(run_restarts())

        loop.run_until_complete(bot_task)

    except KeyboardInterrupt:
        logger.error("Bot shutting down...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        save_approved_users()


if __name__ == '__main__':
    if not os.path.exists(HOSTING_DIR):
        os.makedirs(HOSTING_DIR)
    main()
