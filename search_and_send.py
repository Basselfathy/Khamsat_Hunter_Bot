import json
import random
import asyncio
from _logger import logger
from telethon import TelegramClient
from telethon.errors import FloodWaitError

# Load JSON data from a file
def load_json_file(file_path: str):
    """Load JSON data from a given file path."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"File {file_path} loaded [green]successfuly![green]")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        raise

# Save data to a JSON file
def save_to_json(data, file_name):
    try:
        with open(file_name, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
            logger.info(f"Data saved to [underline green]{file_name}[/underline green]")
    except Exception as e:
        logger.error(f"Error saving data! : {e}")

# Fetch specific fields from the JSON data
def fetch_jobs(data, keywords_list:list):
    jobs_list = []
    matched_jobs_counter = 0
    # Convert all keywords to lowercase for case-insensitive comparison
    
    keywords = [keyword.lower() for keyword in keywords_list]
    # Iterate over the products
    for item in data:
        job_title = item.get("job_title", None)
        job_desc = item.get("job_desc", None)
        post_date = item.get("post_date", None)
        job_link = item.get("job_link", None)
        
        if job_title:  # Ensure ingredients is not None
            # Convert ingredients to lowercase for case-insensitive comparison
            job_title , job_desc = job_title.lower(), job_desc.lower()
            found_keywords = [keyword for keyword in keywords if keyword in job_title or keyword in job_desc]
            
            if found_keywords:
                logger.info(f"FOUND keywords {found_keywords} in -> [underline]{job_title}[/underline].")
                # Append the collected information to a list
                jobs_list.append({
                    "Title":job_title,
                    "Desc": job_desc,
                    "Publish_date": post_date,
                    "Job_link": job_link,
                    "found_keywords": found_keywords
                })
                matched_jobs_counter += 1
    logger.info(f"FOUND {matched_jobs_counter} jobs/s have the searched keyword/s!")            
    return jobs_list

# Send message to Telegram
async def send_to_telegram(jobs, api_id, api_hash, phone_number, receiver_username):
    # Initialize the client
    client = TelegramClient('job_session', api_id, api_hash)
    
    await client.start(phone_number)
    
    # Get the receiver entity (this will automatically fetch user ID and access hash)
    try:
        receiver = await client.get_input_entity(receiver_username)
    except Exception as e:
        logger.error(f"Failed to fetch receiver entity for username {receiver_username}: {e}")
        await client.disconnect()
        return
    
    # Formatting each job in a message
    for job in jobs:
        message = (
            f"◄ **عنوان المنشور:** {job['Title']}\n\n"
            f"◄ **وقت النشر:** {job['Publish_date']}\n\n"
            f"◄ **رابط المنشور:** [Click here]({job['Job_link']})\n\n"
            f"◄ **كلمات البحث:** {', '.join(job['found_keywords'])}\n\n"
        )
        
        try:
            # Send message to the receiver
            await client.send_message(receiver, message, parse_mode='md')
            logger.info(f"Message sent for job: {job['Title']}")
            await asyncio.sleep(random.uniform(0.5,0.8))
        except FloodWaitError as e:
            logger.warning(f"Flood wait error: Need to wait for {e.seconds} seconds before sending more messages.")
            await asyncio.sleep(e.seconds)    
        except Exception as e:
            logger.error(f"Failed to send message for job: {job['Title']}, Error: {e}")
    
    await client.disconnect()

def search_and_send(input_filepath, output_filepath, keywords_list:list, api_id:int, api_hash:str, phone_number:str, receiver_user_id:str):

    json_data = load_json_file(input_filepath)
    if json_data:  # Ensure data was loaded successfully
        jobs = fetch_jobs(json_data, keywords_list)
        save_to_json(jobs, output_filepath)
        # Send the fetched jobs to Telegram
        if jobs:
            asyncio.run(send_to_telegram(jobs, api_id, api_hash, phone_number, receiver_user_id))
