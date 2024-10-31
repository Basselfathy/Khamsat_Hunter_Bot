import asyncio
import os
from pyppeteer import launch
from lxml import html
from _logger import logger
import json
import random
import httpx
from search_and_send import search_and_send as send
from config import BROWSER_PATH, API_HASH, API_ID, PHONE_NUMBER, RECEIVER_USER_ID


# Directories paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(SCRIPT_DIR, "Json_files")

# Files paths
jobs_urls_file = os.path.join(JSON_DIR, 'jobs_urls.json')
jobs_data_file = os.path.join(JSON_DIR, 'jobs_data.json')
matched_jobs_file = os.path.join(JSON_DIR, 'matched_jobs.json')
keywords_file = os.path.join(SCRIPT_DIR, 'keywords_list.txt')


# Function to check if a directory exist, else create one.
def check_for_dir(dir_path:str):
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logger.info(f"{dir_path} directory created!")
    except Exception as e:
        logger.error(f"Could't find or create directory {dir_path} : {e}")

# Semaphore limit for async requests
SEM_LIMIT = 5
semaphore = asyncio.Semaphore(SEM_LIMIT)

# --- Phase 1: Job Links Scraper ---
class KhamsatScraper:
    def __init__(self, base_url, pages_to_search=int, delay=int):
        self.base_url = base_url
        self.pages_to_search = pages_to_search
        self.delay = delay
        self.browser = None
        self.page = None

    async def init_browser(self):
        """Initialize the browser."""
        self.browser = await launch(headless=True, executablePath=BROWSER_PATH,
                                    args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled', '--start-maximized'])
        self.page = await self.browser.newPage()
        await self.page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36')
        logger.info("Browser initialized")

    async def load_page(self):
        """Navigate to the base URL."""
        try:
            await self.page.goto(self.base_url)
            logger.info(f"Navigated to {self.base_url}")
        except Exception as e:
            logger.error(f"Failed to load page: {e}")

    async def load_more_posts(self):
        """Click the 'Load More' button multiple times to load additional posts."""
        for i in range(self.pages_to_search):
            try:
                logger.info(f"Loading {i+1} more page/s")
                # Load more button XPath
                LOAD_MORE_BUTTON_XPATH = '//*[@id="community_loadmore_btn"]'
                await self.page.waitForXPath(LOAD_MORE_BUTTON_XPATH, {'visible': True})
                load_more_button = await self.page.xpath(LOAD_MORE_BUTTON_XPATH)
                await load_more_button[0].click()
                await asyncio.sleep(self.delay)
            except Exception as e:
                logger.error(f"Error during 'Load More' click attempt {i+1}: {e}")
                break

    async def scrape_job_links(self):
        """Scrape job links from the loaded content."""
        try:
            # Fetch the cookies
            cookies = await self.page.cookies()
            content = await self.page.content()
            tree = html.fromstring(content)
            # Job links XPath
            JOB_LINKS_XPATH = '//*[@class="forum_post"]/td[2]/h3/a'
            job_links = tree.xpath(JOB_LINKS_XPATH + '/@href')
            # Complete the job links by prepending the base URL
            full_links = [f"https://khamsat.com{link}" for link in job_links]
            logger.info(f"Found {len(job_links)} job links")
            return full_links, cookies
        except Exception as e:
            logger.error(f"Failed to scrape job links: {e}")
            return []

    async def close_browser(self):
        """Close the browser instance."""
        await self.browser.close()
        logger.info("Browser closed")

    async def run_scraper(self):
        """Run the full scraping process."""
        await self.init_browser()
        await self.load_page()
        await self.load_more_posts()
        job_links = await self.scrape_job_links()
        await self.close_browser()
        return job_links

def save_to_json(data, dir_path:str, file_path:str):
    """Save job links to a JSON file."""
    check_for_dir(dir_path)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Data saved to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save data to {file_path} : {e}")

def load_json_file(dir_path:str, file_path: str):
    """Load JSON data from a given file path."""
    check_for_dir(dir_path)
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"Successfully loaded JSON file from {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        raise

# --- Phase 2: Job Data Scraper ---
async def fetch_page_content(url:str, COOKIES:dict):
    """Fetch page content asynchronously, with redirect handling and retry on 429 error."""
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': 'https://khamsat.com/community/requests',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(url, headers=HEADERS, cookies=COOKIES)
            if response.status_code == 429:
                logger.warning("Too many requests (429). Pausing for 1 minute...")
                await asyncio.sleep(60)  # Wait for 1 minute
                return await fetch_page_content(url)  # Retry the request
            response.raise_for_status()
            return str(response.url), response.text  # Return final URL and page content
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except httpx.RequestError as e:
        logger.error(f"Request error occurred: {e}")
    return url, ""  # Return original URL in case of failure


def parse_page(content: str):
    """Parse page content and extract required data."""
    try:
        tree = html.fromstring(content)
        job_title = tree.xpath('//*[@id="header-group"]/div[1]/div/h1/text()')[0] if tree.xpath('//*[@id="header-group"]/div[1]/div/h1/text()') else None
        job_desc = ' '.join(tree.xpath('//div[@class="card-body"]/article/text()')).strip() if tree.xpath('//div[@class="card-body"]/article/text()') else None
        post_date = tree.xpath('//*[@id="sidebar"]/div[2]/span/@title')[0] if tree.xpath('//*[@id="sidebar"]/div[2]/span/@title') else None
        publisher_name = tree.xpath('//h3/a[@class="sidebar_user"]/text()')[0] if tree.xpath('//h3/a[@class="sidebar_user"]/text()') else None
        publisher_link = 'https://khamsat.com' + tree.xpath('//h3/a[@class="sidebar_user"]/@href')[0] if tree.xpath('//h3/a[@class="sidebar_user"]/@href') else None

        return {
            "job_title": job_title,
            "job_desc": job_desc,
            "post_date": post_date,
            "publisher_name": publisher_name,
            "publisher_link": publisher_link
        }
    except Exception as e:
        logger.error(f"Error during parsing: {e}")
        return {}

async def scrape_khamsat_job(url: str, COOKIES):
    """Fetch and parse a single job page."""
    async with semaphore:
        #logger.info(f"Fetching data from {url}")
        await asyncio.sleep(random.uniform(0.7, 1.5))  # Add random delay
        redirected_url, content = await fetch_page_content(url, COOKIES)
        if content:
            job_data = parse_page(content)
            job_data["job_link"] = redirected_url  # Add the redirected URL to the data
            return job_data
        return {}

async def scrape_khamsat_jobs(COOKIES):
    """Scrape job data from multiple pages and save it as JSON."""
    tasks = []
    jobs_url = load_json_file(JSON_DIR, jobs_urls_file)
    for url in jobs_url:
        tasks.append(scrape_khamsat_job(url, COOKIES))
    
    results = await asyncio.gather(*tasks)
    
    # Filter out empty results
    valid_results = [res for res in results if res]
    
    # Save results as JSON
    save_to_json(valid_results, JSON_DIR, jobs_data_file)  
    
    logger.info(f"Scraped {len(valid_results)} jobs and saved to {jobs_data_file}")



def read_keywords_from_file(filepath: str) -> list:
    """
    Reads keywords from a file, where each line is a keyword or phrase.
    Returns a list of keywords or an empty list if any error occurs.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        logger.error(f"The file '{filepath}' was not found.")
    except IOError:
        logger.error(f"Could not read the file '{filepath}'.")
    return []


# Main execution
if __name__ == "__main__":
    BASE_URL = 'https://khamsat.com/community/requests'

    # Prompt the user to input the number of pages to load, ensuring a valid integer >= 1
    while True:
        try:
            pages_to_search = int(input("Enter the number of pages to search within (must be 1 or higher): "))
            if pages_to_search < 1:
                raise ValueError("The number must be at least 1.")
            break  # Exit loop if valid input is given
        except ValueError as e:
            logger.error(f"Invalid input: {e}. Please enter a valid integer 1 or higher.")

    # Phase 1: Scrape job links
    khamsat_scraper = KhamsatScraper(BASE_URL, pages_to_search=pages_to_search-1, delay=3) 
    job_links, cookies = asyncio.run(khamsat_scraper.run_scraper())
    save_to_json(job_links, JSON_DIR, jobs_urls_file)
    fetched_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
    logger.info(f'Fetched cookies:\n{json.dumps(fetched_cookies, indent=2)}')
    
    # Phase 2: Scrape job details
    asyncio.run(scrape_khamsat_jobs(fetched_cookies))

    # Load keywords from the file
    KEYWORDS_LIST = read_keywords_from_file(keywords_file)
    if not KEYWORDS_LIST:
        logger.error("No keywords found or an error occurred while reading the file.")
    else:
        # Phase 3: Run the search and send to telegram process
        send(jobs_data_file, matched_jobs_file, KEYWORDS_LIST, API_ID, API_HASH, PHONE_NUMBER, RECEIVER_USER_ID)
