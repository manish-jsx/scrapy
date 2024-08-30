import time
import math
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

def setup_driver():
    try:
        options = Options()
        options.add_argument("--incognito")
        # options.add_argument("--headless")  # Uncomment for headless mode

        # Path to the local chromedriver
        service = Service('/Users/manishkumar/scrapper/FINAL/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
   
        print("WebDriver setup successfully.")
        return driver
    except Exception as e:
        print(f"An error occurred while setting up WebDriver: {e}")
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

        print(f"Total number of jobs: {total_jobs}")
        print(f"Maximum number of pages: {max_pages}")

        return max_pages
    except Exception as e:
        print(f"An error occurred while retrieving max pages: {e}")
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

# Process each job listing
def process_job_listing(job, driver, city_info):
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

        print(f"Extracted job: {job_title} - {apply_url}")

        return {
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
        }
    except NoSuchElementException as e:
        print(f"Failed to extract job details. Error: {str(e)}")
        return None

# Scrape jobs from a specific city
def scrape_jobs(driver, base_url, query_params, city_info):
    max_pages = get_max_pages(driver, base_url, query_params)
    all_jobs = []

    for page_number in range(1, max_pages + 1):
        page_url = f"{base_url}-{page_number}{query_params}"
        print(f"Opening {page_url}")

        try:
            driver.get(page_url)
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.srp-jobtuple-wrapper'))
            )

            job_listings = driver.find_elements(By.CSS_SELECTOR, '.srp-jobtuple-wrapper')
            
            if not job_listings:
                print("No more jobs found. Exiting loop.")
                break

            print(f"Found {len(job_listings)} job listings on page {page_number}.")

            for job in job_listings:
                job_data = process_job_listing(job, driver, city_info)
                if job_data:
                    all_jobs.append(job_data)

        except TimeoutException as e:
            print(f"TimeoutException: Unable to load page {page_url}. Error: {str(e)}")
            break
        except Exception as e:
            print(f"Error while processing page {page_url}: {str(e)}")
            continue

    return all_jobs

# Main function to scrape jobs sequentially
def main():
    all_job_data = []
    
    with open('WalkinJobs-Input.csv', 'r') as file:
        reader = csv.DictReader(file)
        city_urls = list(reader)

    for city in city_urls:
        base_url = city['URL']
        city_info = {
            'CITY ID': city['CITY ID'],
            'City': city['City'],
            'INDUSTRY ID': city['INDUSTRY ID']
        }
        query_params = '?wfhType=0&jobPostType=1&jobAge=1'
        
        driver = setup_driver()
        try:
            print(f"Starting scrape for {city_info['City']}...")
            city_jobs = scrape_jobs(driver, base_url, query_params, city_info)
            all_job_data.extend(city_jobs)
            print(f"Completed scrape for {city_info['City']}. Total jobs found: {len(city_jobs)}")
        finally:
            driver.quit()
            print(f"WebDriver for {city_info['City']} closed successfully.")

    try:
        with open('all_job_listings.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['CITY ID', 'City', 'INDUSTRY ID', 'Job Title', 'Company', 'Experience', 'Location', 'Salary', 'Apply URL', 'Walk-in', 'Time', 'Venue']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for job in all_job_data:
                writer.writerow(job)
        print("All job data successfully written to all_job_listings.csv.")
    except Exception as e:
        print(f"An error occurred while saving job data to CSV: {e}")

    print("\nCollected all job data:")
    for i, job in enumerate(all_job_data, start=1):
        print(f"{i}. {job['Job Title']} - {job['Apply URL']} - Walk-in: {job['Walk-in']}")

if __name__ == "__main__":
    main()
