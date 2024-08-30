import math
import csv
import threading
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from queue import Queue

# Setup logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup WebDriver
def setup_driver():
    try:
        options = Options()
        options.add_argument("--incognito")
        # options.add_argument("--headless")  # Uncomment for headless mode

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        logging.info("WebDriver setup successfully.")
        return driver
    except Exception as e:
        logging.error(f"An error occurred while setting up WebDriver: {e}")
        raise

# Get max pages from the job listings
def get_max_pages(driver, base_url, query_params):
    try:
        driver.get(base_url + query_params)
        wait = WebDriverWait(driver, 10)
        total_pages_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span.styles_count-string__DlPaZ'))
        )
        total_pages_text = total_pages_element.text

        total_jobs_text = total_pages_text.split('of')[-1].strip()
        total_jobs = int(total_jobs_text.replace(',', ''))
        max_pages = min(math.ceil(total_jobs / 20), 15)  # Limit to 15 pages

        logging.info(f"Total number of jobs: {total_jobs}")
        logging.info(f"Maximum number of pages: {max_pages}")

        return max_pages
    except Exception as e:
        logging.error(f"An error occurred while retrieving max pages: {e}")
        return 1

# Extract walk-in details from a job listing URL
def extract_walkin_details(driver, apply_url):
    try:
        driver.get(apply_url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.styles_jhc__walkin__57j_D'))
        )
        time_element = driver.find_element(By.CSS_SELECTOR, '.styles_jhc__walkin__57j_D')
        venue_element = driver.find_element(By.CSS_SELECTOR, '.styles_jhc__venue__2cqi5')
        
        time_text = time_element.text.strip()
        venue_text = venue_element.text.strip()

        return time_text, venue_text
    except NoSuchElementException:
        return "N/A", "N/A"
    except TimeoutException:
        return "N/A", "N/A"

# Scrape job description from a job listing URL
def scrape_job_description(url, description_queue):
    logging.info(f"Starting scrape for URL: {url}")
    driver = setup_driver()
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

                # Add to queue for later processing
                description_queue.put((url, job_description))
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

# Process each job listing
def process_job_listing(job, driver, city_info, description_queue, job_queue):
    try:
        job_title = job.find_element(By.CSS_SELECTOR, '.title').text
        company = job.find_element(By.CSS_SELECTOR, '.comp-name').text
        experience = job.find_element(By.CSS_SELECTOR, '.exp-wrap .exp').text if job.find_elements(By.CSS_SELECTOR, '.exp-wrap .exp') else "N/A"
        location = job.find_element(By.CSS_SELECTOR, '.locWdth').text if job.find_elements(By.CSS_SELECTOR, '.locWdth') else "N/A"
        salary_elements = job.find_elements(By.CSS_SELECTOR, '.sal-wrap .ni-job-tuple-icon span')
        salary = salary_elements[0].get_attribute('title').strip() if salary_elements else "Not disclosed"

        apply_url = job.find_element(By.CSS_SELECTOR, '.title').get_attribute('href')

        # Check for walk-in details
        walkin_tag = job.find_elements(By.CSS_SELECTOR, '.ttc__walk-in')
        walkin = "Yes" if walkin_tag else "No"

        time, venue = "N/A", "N/A"
        if walkin == "Yes":
            time, venue = extract_walkin_details(driver, apply_url)

        # Add job listing to queue
        job_queue.put({
            'CITY ID': city_info['CITY ID'],
            'City': city_info['City'],
            'INDUSTRY ID': city_info['INDUSTRY ID'],
            'Job Title': job_title,
            'Company': company,
            'Experience': experience,
            'Location': location,
            'Salary': salary,
            'Apply URL': apply_url,
            'Walk-in': walkin,
            'Time': time,
            'Venue': venue
        })

        # Scrape job description asynchronously
        threading.Thread(target=scrape_job_description, args=(apply_url, description_queue)).start()

        logging.info(f"Extracted job: {job_title} - {apply_url}")

    except NoSuchElementException as e:
        logging.error(f"Failed to extract job details. Error: {str(e)}")
        return None

# Scrape jobs from a specific city
def scrape_jobs(driver, base_url, query_params, city_info, job_queue, description_queue):
    max_pages = get_max_pages(driver, base_url, query_params)
    all_jobs = []

    for page_number in range(1, max_pages + 1):
        page_url = f"{base_url}-{page_number}{query_params}"
        logging.info(f"Opening {page_url}")

        try:
            driver.get(page_url)
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.srp-jobtuple-wrapper'))
            )

            job_listings = driver.find_elements(By.CSS_SELECTOR, '.srp-jobtuple-wrapper')
            
            if not job_listings:
                logging.info("No more jobs found. Exiting loop.")
                break

            logging.info(f"Found {len(job_listings)} job listings on page {page_number}.")

            for job in job_listings:
                process_job_listing(job, driver, city_info, description_queue, job_queue)

        except TimeoutException as e:
            logging.error(f"TimeoutException: Unable to load page {page_url}. Error: {str(e)}")
            break
        except Exception as e:
            logging.error(f"Error while processing page {page_url}: {str(e)}")
            continue

# Worker function for threading
def worker(city, job_queue, description_queue):
    base_url = city['URL']
    city_info = {
        'CITY ID': city['CITY ID'],
        'City': city['City'],
        'INDUSTRY ID': city['INDUSTRY ID']
    }
    query_params = '?wfhType=0&jobPostType=1&jobAge=1'
    
    try:
        driver = setup_driver()
        scrape_jobs(driver, base_url, query_params, city_info, job_queue, description_queue)
        driver.quit()
    except Exception as e:
        logging.error(f"Error in worker function for city {city['City']}: {e}")

# Merge CSV files
def merge_csv_files(job_queue, description_queue):
    # Ensure files are created and opened
    job_file = open('all_job_listings.csv', mode='w', newline='', encoding='utf-8')
    description_file = open('all_job_descriptions.csv', mode='w', newline='', encoding='utf-8')
    
    job_fieldnames = ['CITY ID', 'City', 'INDUSTRY ID', 'Job Title', 'Company', 'Experience', 'Location', 'Salary', 'Apply URL', 'Walk-in', 'Time', 'Venue']
    description_fieldnames = ['Apply URL', 'Job Description']
    
    job_writer = csv.DictWriter(job_file, fieldnames=job_fieldnames)
    description_writer = csv.DictWriter(description_file, fieldnames=description_fieldnames)
    
    job_writer.writeheader()
    description_writer.writeheader()

    while True:
        if not job_queue.empty():
            job = job_queue.get()
            job_writer.writerow(job)
            job_file.flush()

        if not description_queue.empty():
            url, description = description_queue.get()
            description_writer.writerow({'Apply URL': url, 'Job Description': description})
            description_file.flush()

        if job_queue.empty() and description_queue.empty():
            break

    job_file.close()
    description_file.close()

    # Merge CSV files
    job_listings_df = pd.read_csv('all_job_listings.csv')
    job_descriptions_df = pd.read_csv('all_job_descriptions.csv')

    # Merge on 'Apply URL'
    merged_df = pd.merge(job_listings_df, job_descriptions_df, on='Apply URL', how='left')

    # Save the merged CSV
    merged_df.to_csv('merged_job_listings.csv', index=False, encoding='utf-8')
    logging.info("Merged CSV saved as 'merged_job_listings.csv'.")

# Main function to start scraping
def main():
    cities = [
        # Add city details here
    ]
    
    job_queue = Queue()
    description_queue = Queue()
    
    # Start data collection threads
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, city, job_queue, description_queue) for city in cities]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"An error occurred: {e}")

    # Start merging thread
    merge_thread = threading.Thread(target=merge_csv_files, args=(job_queue, description_queue))
    merge_thread.start()
    merge_thread.join()

if __name__ == "__main__":
    main()
