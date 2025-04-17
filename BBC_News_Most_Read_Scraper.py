import requests
from bs4 import BeautifulSoup
import csv
import datetime
import os
import traceback

# --- Configuration ---
URL = "https://www.bbc.co.uk/news"
DATA_DIR = "data" # Subdirectory for all data files
MOST_READ_FILE_PREFIX = "bbc_most_read_"
FRONT_PAGE_PROMO_FILE_PREFIX = "bbc_front_page_promos_" # Renamed prefix
TOP_N_MOST_READ = 10 # Number of 'most read' stories
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Helper Functions ---

def get_current_utc_date_str():
    """Returns the current date as YYYY-MM-DD string in UTC."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

def get_current_utc_timestamp_str():
    """Returns the current timestamp string in UTC."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def construct_filepath(prefix):
    """Constructs the full filepath for the daily CSV file."""
    current_date_str = get_current_utc_date_str()
    filename = f"{prefix}{current_date_str}.csv"
    return os.path.join(DATA_DIR, filename)

def ensure_dir_exists():
    """Ensures the DATA_DIR exists."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {DATA_DIR}: {e}")
        raise

# --- Scraping Functions ---

def scrape_most_read(soup):
    """
    Scrapes the 'Most Read' stories from the parsed HTML soup.
    Args:
        soup (BeautifulSoup): Parsed HTML object of the page.
    Returns:
        list: List of story dicts [{'rank', 'title', 'link'}, ...] or empty list.
    """
    stories = []
    print("Scraping 'Most Read' section...")
    try:
        most_read_list = soup.select_one('div[data-component="mostRead"] ol')
        if not most_read_list:
            print("Error: Could not find the 'Most Read' list container.")
            return []

        list_items = most_read_list.find_all('li', recursive=False, limit=TOP_N_MOST_READ)
        if not list_items:
             list_items = most_read_list.find_all('li', limit=TOP_N_MOST_READ)

        for index, item in enumerate(list_items):
            link_tag = item.select_one('a[class*="HeadlineLink"]')
            if link_tag and link_tag.text:
                title = link_tag.text.strip()
                link = link_tag.get('href')
                if link and not link.startswith('http'):
                    base_url = "https://www.bbc.co.uk"
                    if not link.startswith('/'): link = '/' + link
                    link = f"{base_url}{link}"
                stories.append({"rank": index + 1, "title": title, "link": link})
            else:
                 print(f"Warning: Could not extract title/link from 'Most Read' item {index+1}.")

        print(f"Successfully scraped {len(stories)} 'Most Read' stories.")
        return stories

    except Exception as e:
        print(f"An error occurred during 'Most Read' scraping: {e}")
        return []

def scrape_front_page_promos(soup):
    """
    Scrapes all prominent stories from the main front page promo area.
    Args:
        soup (BeautifulSoup): Parsed HTML object of the page.
    Returns:
        list: List of story dicts [{'title': ..., 'link': ...}, ...] or empty list.
    """
    stories = []
    print("Scraping front page promo section...")
    try:
        # Find the main container for the front page promo grid
        # Based on user-provided HTML: 'div.ssrcss-1euvvif-Wrap ul.ssrcss-y8stko-Grid'
        promo_grid = soup.select_one('div.ssrcss-1euvvif-Wrap ul.ssrcss-y8stko-Grid')

        if not promo_grid:
             # Fallback if the specific grid class changes
             promo_grid = soup.select_one('div.ssrcss-1euvvif-Wrap ul[class*="-Grid"]')
             if not promo_grid:
                  print("Error: Could not find the front page promo grid container.")
                  return []

        # Find all list items within that grid
        # Using 'recursive=False' to only get direct children li elements
        list_items = promo_grid.find_all('li', recursive=False)

        if not list_items:
            print("Warning: Found promo grid but no list items within it.")
            return []

        print(f"Found {len(list_items)} potential promo items in the grid.")
        for index, item in enumerate(list_items):
            # Find the link and title within each list item
            # These selectors might need refinement if structure varies between items
            link_tag = item.select_one('a[class*="-PromoLink"]')
            title_tag = item.select_one('p[class*="-PromoHeadline"]')

            if link_tag and title_tag:
                title = title_tag.text.strip()
                link = link_tag.get('href')

                if link and not link.startswith('http'):
                    base_url = "https://www.bbc.co.uk"
                    if not link.startswith('/'): link = '/' + link
                    link = f"{base_url}{link}"

                if title and link:
                    stories.append({"title": title, "link": link})
                else:
                    print(f"Warning: Found promo item {index+1} tags but missing title or link.")
            else:
                # This might catch non-story items or differently structured promos
                print(f"Note: Skipping item {index+1} in promo grid, couldn't find expected link/title tags.")
                # print(f"  Item HTML snippet: {str(item)[:200]}...") # Uncomment for debugging

        print(f"Successfully scraped {len(stories)} front page promo stories.")
        return stories

    except Exception as e:
        print(f"An error occurred during front page promo scraping: {e}")
        # print(traceback.format_exc())
        return []

# --- Saving Functions ---

def save_most_read_to_csv(stories):
    """Appends the 'Most Read' stories to their daily CSV file."""
    if not stories:
        print("No 'Most Read' stories to save.")
        return

    csv_filepath = construct_filepath(MOST_READ_FILE_PREFIX)
    timestamp = get_current_utc_timestamp_str()
    file_exists = os.path.isfile(csv_filepath)
    needs_header = not file_exists

    try:
        with open(csv_filepath, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'rank', 'title', 'link']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if needs_header: writer.writeheader()

            for story in stories:
                row_data = {field: story.get(field, '') for field in fieldnames}
                row_data['timestamp'] = timestamp
                writer.writerow(row_data)
        print(f"Successfully appended {len(stories)} 'Most Read' stories to {csv_filepath}")
    except IOError as e:
        print(f"Error writing 'Most Read' to CSV {csv_filepath}: {e}")
    except Exception as e:
        print(f"Unexpected error writing 'Most Read' CSV: {e}")

def save_front_page_promos_to_csv(promo_stories):
    """
    Appends the front page promo stories to their daily CSV file.
    Args:
        promo_stories (list): List of story dicts [{'title': ..., 'link': ...}, ...]
    """
    if not promo_stories:
        print("No front page promo stories to save.")
        return

    csv_filepath = construct_filepath(FRONT_PAGE_PROMO_FILE_PREFIX) # Use renamed prefix
    timestamp = get_current_utc_timestamp_str()
    file_exists = os.path.isfile(csv_filepath)
    needs_header = not file_exists

    try:
        with open(csv_filepath, 'a', newline='', encoding='utf-8') as csvfile:
            # Define fieldnames for the front page promo CSV, adding 'position'
            fieldnames = ['timestamp', 'position', 'title', 'link']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if needs_header: writer.writeheader()

            # Write story data, adding position based on scrape order
            for index, story in enumerate(promo_stories):
                row_data = {field: story.get(field, '') for field in fieldnames}
                row_data['timestamp'] = timestamp
                row_data['position'] = index + 1 # Add position
                writer.writerow(row_data)
        print(f"Successfully appended {len(promo_stories)} front page promos to {csv_filepath}")
    except IOError as e:
        print(f"Error writing front page promos to CSV {csv_filepath}: {e}")
    except Exception as e:
        print(f"Unexpected error writing front page promos CSV: {e}")


# --- Main Job Function ---

def run_scrape_job():
    """Fetches page, runs all scraping, and saves results."""
    print(f"\n--- Running scrape job via GitHub Actions at {get_current_utc_timestamp_str()} ---")

    try:
        ensure_dir_exists()
    except Exception:
        print("Failed to create data directory. Aborting job.")
        return

    soup = None
    try:
        print(f"Fetching {URL}...")
        response = requests.get(URL, headers=HEADERS, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        print("Page fetched and parsed successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Fatal Error: Could not fetch URL {URL}: {e}")
        return
    except Exception as e:
        print(f"Fatal Error: Could not parse page: {e}")
        return

    # Scrape Sections
    most_read_stories = scrape_most_read(soup)
    front_page_promo_stories = scrape_front_page_promos(soup) # Renamed variable

    # Save Results
    save_most_read_to_csv(most_read_stories)
    save_front_page_promos_to_csv(front_page_promo_stories) # Renamed function call

    print("--- Scrape job finished ---")

# --- Main Execution ---
if __name__ == "__main__":
    run_scrape_job()
