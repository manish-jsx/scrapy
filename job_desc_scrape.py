import csv
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
from threading import Lock

# Setup logging
logging.basicConfig(filename='job_scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Lock for synchronized file access
file_lock = Lock()

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--incognito")  # Run in headless mode
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

def scrape_job_description(url, output_csv):
    logging.info(f"Starting scrape for URL: {url}")
    driver = create_driver()
    attempt = 0
    job_description = "Failed to fetch description"
    
    while attempt < 3:
        try:
            driver.get(url)
            logging.info(f"Loaded URL: {url}")

            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'styles_job-desc-container__txpYf'))
                )
                logging.info(f"Found job description container for URL: {url}")

                try:
                    read_more_button = driver.find_element(By.CLASS_NAME, 'styles_read-more__MyWkb')
                    read_more_button.click()
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'styles_JDC__dang-inner-html__h0K4t'))
                    )
                    logging.info(f"Clicked 'Read More' for URL: {url}")
                except NoSuchElementException:
                    logging.info(f"No 'Read More' button for URL: {url}")
                
                job_description_element = driver.find_element(By.CLASS_NAME, 'styles_job-desc-container__txpYf')
                job_description = job_description_element.text
                logging.info(f"Extracted job description for URL: {url}")

                # Write to CSV in real-time
                with file_lock:
                    with open(output_csv, mode='a', newline='', encoding='utf-8') as outfile:
                        writer = csv.DictWriter(outfile, fieldnames=['Apply URL', 'Job Description'])
                        writer.writerow({'Apply URL': url, 'Job Description': job_description})
                        logging.info(f"Written description for URL: {url}")
                break

            except TimeoutException:
                logging.warning(f"Timeout occurred for URL: {url}")
                attempt += 1
                time.sleep(10)  # Increase delay before retrying

        except WebDriverException as e:
            logging.error(f"WebDriverException for URL: {url} - {str(e)}")
            attempt += 1
            time.sleep(10)

    driver.quit()
    logging.info(f"Driver closed for URL: {url}")

def main():
    input_csv = 'all_job_listings_thread.csv'
    output_csv = 'all_job_listings_with_descriptions.csv'

    logging.info(f"Reading from input CSV: {input_csv}")

    # Prepare output CSV file with header
    with open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=['Apply URL', 'Job Description'])
        writer.writeheader()

    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        urls = [row['Apply URL'] for row in reader]
        logging.info(f"URLs extracted: {len(urls)}")

    logging.info("Starting to process URLs")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(scrape_job_description, url, output_csv) for url in urls]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Exception occurred: {str(e)}")

    logging.info("Completed processing URLs")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
