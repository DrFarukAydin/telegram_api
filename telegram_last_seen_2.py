import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError
import snowflake.connector
from datetime import datetime, timedelta, timezone
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
session_file = 'faruktest_session_2'

async def fetch_last_seen():
    
    # Initialize Telegram Client
    try:
        client = TelegramClient(session_file, int(api_id), api_hash)
        print("TelegramClient initialized successfully.")
    except Exception as e:
        print(f"Error initializing TelegramClient: {e}")
        return
        
    try:
        await client.start(phone_number)
        print("Telegram client started successfully.")

        # Iterate over participants in the specified group
        async for user in client.iter_participants(group_id):
            print(user)
            if user.status:
                last_seen = user.status.to_dict().get('was_online')
                
                if last_seen:
                    # Calculate points based on last seen time
                    points = calculate_points(last_seen)
                    save_to_snowflake(user.id, user.username, last_seen, points)
                        
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

def calculate_points(last_seen_time):
    # Ensure last_seen_time is timezone-naive by converting it to UTC and then making it naive
    if last_seen_time.tzinfo is not None:
        last_seen_time = last_seen_time.astimezone(timezone.utc).replace(tzinfo=None)

    now = datetime.utcnow()
    # Calculate the difference from the beginning of the current hour
    current_hour_start = now.replace(minute=0, second=0, microsecond=0)

    # Calculate time difference
    time_diff = current_hour_start - last_seen_time

    # If last seen within the last hour, full 24 points
    if time_diff < timedelta(hours=1):
        return 24
    # If last seen within the last 24 hours, reduce points based on hours passed
    elif time_diff < timedelta(hours=24):
        hours_diff = time_diff.total_seconds() // 3600  # Convert seconds to hours
        return max(24 - hours_diff, 0)
    # If last seen over 24 hours ago, 0 points
    else:
        return 0

def save_to_snowflake(user_id, username, last_seen_time, points_awarded):
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

        # Use MERGE statement to perform upsert operation
        cursor.execute(f"""
            MERGE INTO users AS target
            USING (SELECT 
                    '{user_id}' AS user_id, 
                    '{username}' AS username, 
                    '{last_seen_time}' AS last_seen, 
                    {points_awarded} AS points) AS source
            ON target.user_id = source.user_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.last_seen = source.last_seen, 
                    target.points = target.points + source.points
            WHEN NOT MATCHED THEN
                INSERT (user_id, username, last_seen, points) 
                VALUES (source.user_id, source.username, source.last_seen, source.points);
        """)
        
        # Insert into point history table
        cursor.execute("""
            INSERT INTO point_history (user_id, point_awarded, hour_checked)
            VALUES (%s, %s, %s)
        """, (str(user_id), points_awarded, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        print('Data committed to Snowflake successfully.')
    except Exception as e:
        print(f"Error saving to Snowflake: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def main():
    asyncio.run(fetch_last_seen())

if __name__ == "__main__":
    if os.path.exists(session_file):
        os.remove(session_file)  # Remove old session file if exists

    main()
