import requests
from bs4 import BeautifulSoup
import csv
import datetime
import os
import traceback # Added for potential debugging

# --- Configuration ---
URL = "https://www.bbc.co.uk/news"
# Define the subdirectory for data files
DATA_DIR = "data"
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
    Returns: list of story dicts or empty list on failure.
    """
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Scraping {URL}...")
    stories = []
    try:
        response = requests.get(URL, headers=HEADERS, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Selector based on data-component attribute and ol tag
        # Using a robust selector targeting the main container and the ordered list
        most_read_list = soup.select_one('div[data-component="mostRead"] ol')

        if not most_read_list:
            print("Error: Could not find the 'Most Read' list container.")
            return []

        # Find direct li children within the identified list
        list_items = most_read_list.find_all('li', recursive=False, limit=TOP_N)
        if not list_items: # Fallback if direct children fail (less likely)
             list_items = most_read_list.find_all('li', limit=TOP_N)


        for index, item in enumerate(list_items):
            # Selector for the link, looking for an 'a' tag with 'HeadlineLink' in its class
            # This is more robust to specific class name changes (e.g., ssrcss-xxxxxx-HeadlineLink)
            link_tag = item.select_one('a[class*="HeadlineLink"]')

            if link_tag and link_tag.text:
                title = link_tag.text.strip()
                link = link_tag.get('href')
                # Construct absolute URL if the link is relative
                if link and not link.startswith('http'):
                    base_url = "https://www.bbc.co.uk"
                    if not link.startswith('/'):
                        link = '/' + link
                    link = f"{base_url}{link}"

                stories.append({
                    "rank": index + 1,
                    "title": title,
                    "link": link
                })
            else:
                 # Provide more context in warning
                 print(f"Warning: Could not extract title/link from list item {index+1} using selector 'a[class*=\"HeadlineLink\"]'. Item HTML snippet: {str(item)[:200]}...")


        print(f"Successfully scraped {len(stories)} stories.")
        return stories

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {URL}: {e}")
        return []
    except Exception as e:
        print(f"An error occurred during scraping or parsing: {e}")
        # print(traceback.format_exc()) # Uncomment for detailed debugging
        return []

def save_to_csv(stories):
    """
    Appends the scraped stories to a CSV file named with the current date,
    stored within the DATA_DIR subdirectory. Creates the directory and
    file/header if they don't exist.

    Args:
        stories (list): A list of story dictionaries from scrape_most_read().
    """
    if not stories:
        print("No stories to save.")
        return

    try:
        # Ensure the data directory exists; create it if not.
        # The exist_ok=True prevents an error if the directory already exists.
        os.makedirs(DATA_DIR, exist_ok=True)

        # Get current date in UTC for filename consistency
        current_date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        # Construct the full path for the daily CSV file using os.path.join for cross-platform compatibility
        csv_filepath = os.path.join(DATA_DIR, f"bbc_most_read_{current_date_str}.csv")

        # Use UTC timestamp for the data rows, matching filename convention
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Check if the daily file exists *before* opening it
        file_exists = os.path.isfile(csv_filepath)
        # Check if the file is empty *after* opening (or rely on file_exists for new files)
        needs_header = not file_exists

        with open(csv_filepath, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['timestamp', 'rank', 'title', 'link']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header only if the file is newly created
            if needs_header:
                writer.writeheader()
                print(f"Written header to new CSV file: {csv_filepath}")
            # Optional: Check size even if file exists, in case it was created empty previously
            # elif csvfile.tell() == 0:
            #     writer.writeheader()
            #     print(f"Written header to existing but empty CSV file: {csv_filepath}")


            # Write story data
            for story in stories:
                # Prepare row data, ensuring all keys exist
                row_data = {field: story.get(field, '') for field in fieldnames}
                row_data['timestamp'] = timestamp # Add/overwrite timestamp for this batch
                writer.writerow(row_data)

        print(f"Successfully appended {len(stories)} stories to {csv_filepath}")

    except IOError as e:
        print(f"Error writing to CSV file {csv_filepath}: {e}")
        # print(traceback.format_exc()) # Uncomment for detailed debugging
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")
        # print(traceback.format_exc()) # Uncomment for detailed debugging


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
