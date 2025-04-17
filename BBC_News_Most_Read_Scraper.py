import requests
from bs4 import BeautifulSoup
import csv
import datetime
import os

# --- Configuration ---
URL = "https://www.bbc.co.uk/news"
# Ensure the CSV file path is relative to the script location in the repo
CSV_FILE = "bbc_most_read_log.csv"
# Number of top stories to fetch
TOP_N = 10
# Headers to mimic a browser visit
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- Functions ---

def scrape_most_read():
    """
    Scrapes the 'Most Read' stories from the BBC News homepage.

    Returns:
        list: A list of dictionaries, where each dictionary contains
              'rank', 'title', and 'link' for a story. Returns an empty
              list if scraping fails.
    """
    # Use UTC time for consistency in Actions
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Scraping {URL}...")
    stories = []
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(URL, headers=HEADERS, timeout=20)
        response.raise_for_status() # Raise an exception for bad status codes

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Find the 'Most Read' section ---
        # Updated selector based on the provided HTML structure (April 2025)
        # Looks for an <ol> inside a <div> with data-component="mostRead"
        # The specific class 'ssrcss-1020bd1-Stack' is added for precision but might be fragile.
        # If this breaks, try a simpler selector like 'div[data-component="mostRead"] ol'
        most_read_list = soup.select_one('div[data-component="mostRead"] ol.ssrcss-1020bd1-Stack')

        # Fallback if the primary selector fails (e.g., class name changed)
        if not most_read_list:
             print("Warning: Primary selector failed. Trying fallback 'div[data-component=\"mostRead\"] ol'")
             most_read_list = soup.select_one('div[data-component="mostRead"] ol')

        if not most_read_list:
            print("Error: Could not find the 'Most Read' list container using known selectors. The website structure might have changed significantly.")
            return []

        # Find all list items (li) within the 'Most Read' list
        # The class 'ssrcss-wt6gvb-PromoItem' could be added: find_all('li', class_='ssrcss-wt6gvb-PromoItem', limit=TOP_N)
        # But finding direct 'li' children is usually robust enough here.
        list_items = most_read_list.find_all('li', limit=TOP_N)

        # Extract title and link from each list item
        for index, item in enumerate(list_items):
            # Updated selector for the link based on provided HTML (April 2025)
            # Looks for an <a> tag with the specific class 'ssrcss-qseizj-HeadlineLink'
            # If this breaks, try a simpler selector like item.find('a')
            link_tag = item.select_one('a.ssrcss-qseizj-HeadlineLink')

            if link_tag and link_tag.text:
                title = link_tag.text.strip()
                link = link_tag.get('href')
                # Construct absolute URL if the link is relative
                if link and not link.startswith('http'):
                    # Handle potential base URL variations and ensure leading slash
                    base_url = "https://www.bbc.co.uk"
                    if not link.startswith('/'):
                        link = '/' + link
                    link = f"{base_url}{link}"

                stories.append({
                    "rank": index + 1, # Rank based on order in the list
                    "title": title,
                    "link": link
                })
            else:
                 # Add more specific warning if possible
                 print(f"Warning: Could not extract title/link from list item {index+1} using selector 'a.ssrcss-qseizj-HeadlineLink'. Item HTML: {item}")


        print(f"Successfully scraped {len(stories)} stories.")
        return stories

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {URL}: {e}")
        return []
    except Exception as e:
        # Catching generic Exception to see any parsing errors etc.
        print(f"An error occurred during scraping or parsing: {e}")
        # import traceback # Uncomment for detailed debugging
        # print(traceback.format_exc()) # Uncomment for detailed debugging
        return []

def save_to_csv(stories):
    """
    Appends the scraped stories to a CSV file with a timestamp.

    Args:
        stories (list): A list of story dictionaries from scrape_most_read().
    """
    if not stories:
        print("No stories to save.")
        return

    # Use UTC timestamp for consistency in GitHub Actions
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    file_exists = os.path.isfile(CSV_FILE)

    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'rank', 'title', 'link']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header only if the file is new or empty
            if not file_exists or os.path.getsize(CSV_FILE) == 0:
                writer.writeheader()
                print(f"Written header to new/empty CSV file: {CSV_FILE}")

            # Write story data
            for story in stories:
                row_data = {field: story.get(field, '') for field in fieldnames}
                row_data['timestamp'] = timestamp # Overwrite/set timestamp
                writer.writerow(row_data)

        print(f"Successfully appended {len(stories)} stories to {CSV_FILE}")

    except IOError as e:
        print(f"Error writing to CSV file {CSV_FILE}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")


def run_scrape_job():
    """Runs the scraping and saving process."""
    print(f"\n--- Running scrape job via GitHub Actions at {datetime.datetime.now(datetime.timezone.utc)} ---")
    scraped_data = scrape_most_read()
    save_to_csv(scraped_data)
    print("--- Scrape job finished ---")

# --- Main Execution ---
if __name__ == "__main__":
    # This block executes when the script is run directly
    run_scrape_job()
