import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError
import snowflake.connector
from datetime import datetime
import os

# Telegram API credentials
api_id = os.environ['API_ID']
api_hash = os.environ['API_HASH']
phone_number = os.environ['PHONE_NUMBER']
group_id = os.environ['GROUP_ID']

# Snowflake connection credentials
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_DATABASE = os.environ['SNOWFLAKE_DATABASE']
SNOWFLAKE_SCHEMA = os.environ['SNOWFLAKE_SCHEMA']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']

# Session file path
session_file = 'faruktest_session'

# Global variable to store last_seen values
last_seen_list = []

async def fetch_last_seen():
    global last_seen_list
    # Initialize Telegram Client
    client = TelegramClient(session_file, api_id, api_hash)
    try:
        await client.start(phone_number)
        print("Telegram client started successfully.")

        # Iterate over participants in the specified group
        async for user in client.iter_participants(group_id):
            if user.status:
                last_seen = user.status.to_dict().get('was_online')
                print(f"Raw last_seen timestamp: {last_seen}", type(last_seen))
                last_seen_list.append(last_seen)
                if last_seen:
                    # Use the existing datetime object
                    try:
                        # Directly use last_seen, which is already a datetime object
                        last_seen_time = last_seen.strftime('%Y-%m-%d %H:%M:%S')
                        print(f"Formatted last_seen_time: {last_seen_time}")
                        save_to_snowflake(user.id, user.username, last_seen_time)
                    except Exception as e:
                        print(f"Error processing last_seen timestamp: {e}")

    except SessionPasswordNeededError:
        print("Two-step verification is enabled. Please provide a password.")
    except PhoneCodeInvalidError:
        print("Invalid phone code. Check if the phone number is correct.")
    except FloodWaitError as e:
        print(f"Flood wait error. Try again in {e.seconds} seconds.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        await client.disconnect()

def save_to_snowflake(user_id, username, last_seen_time):
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        cursor = conn.cursor()
        # Use the appropriate Snowflake SQL syntax for parameterized queries
        cursor.execute("""
            INSERT INTO last_seen (user_id, username, last_seen, checked_at)
            VALUES (%s, %s, %s, %s)
        """, (str(user_id), username, last_seen_time, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        print('Committed')
    except Exception as e:
        print(f"Error saving to Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    asyncio.run(fetch_last_seen())

if __name__ == "__main__":
    if os.path.exists(session_file):
        os.remove(session_file)  # Remove old session file if exists
    main()
