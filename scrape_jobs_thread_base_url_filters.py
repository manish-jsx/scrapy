import time
import math
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    """Set up the WebDriver for Chrome."""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--incognito")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("WebDriver setup successfully.")
        return driver
    except WebDriverException as e:
        print(f"An error occurred while setting up WebDriver: {e}")
        raise

def get_total_jobs(driver, base_url, query_params):
    """Retrieve the total number of jobs."""
    driver.get(base_url + query_params)
    try:
        wait = WebDriverWait(driver, 10)
        total_pages_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span.styles_count-string__DlPaZ'))
        )
        total_jobs_text = total_pages_element.text.split('of')[-1].strip()
        total_jobs = int(total_jobs_text.replace(',', ''))
        return total_jobs
    except (TimeoutException, NoSuchElementException, ValueError) as e:
        print(f"An error occurred while retrieving total jobs: {e}")
        return 0

def calculate_max_pages(total_jobs, jobs_per_page=20):
    """Calculate the maximum number of pages."""
    return math.ceil(total_jobs / jobs_per_page)

def extract_job_details(job):
    """Extract basic details from a job element."""
    try:
        job_title = job.find_element(By.CSS_SELECTOR, '.title').text
        company = job.find_element(By.CSS_SELECTOR, '.comp-name').text
        experience = job.find_element(By.CSS_SELECTOR, '.exp-wrap .exp').text if job.find_elements(By.CSS_SELECTOR, '.exp-wrap .exp') else "N/A"
        location = job.find_element(By.CSS_SELECTOR, '.locWdth').text if job.find_elements(By.CSS_SELECTOR, '.locWdth') else "N/A"
        salary_elements = job.find_elements(By.CSS_SELECTOR, '.sal-wrap .ni-job-tuple-icon span')
        salary = salary_elements[0].get_attribute('title').strip() if salary_elements else "Not disclosed"
        apply_url = job.find_element(By.CSS_SELECTOR, '.title').get_attribute('href')
        return job_title, company, experience, location, salary, apply_url
    except NoSuchElementException as e:
        print(f"Failed to extract job details: {e}")
        return "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"

def extract_walkin_details(driver, apply_url):
    """Extract walk-in details from a job listing."""
    try:
        driver.get(apply_url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.styles_jhc__walkin__57j_D'))
        )
        time_element = driver.find_element(By.CSS_SELECTOR, '.styles_jhc__walkin__57j_D').text.strip()
        venue_element = driver.find_element(By.CSS_SELECTOR, '.styles_jhc__venue__2cqi5').text.strip()

        try:
            read_more_button = driver.find_element(By.CLASS_NAME, "styles_read-more-link__dD_5h")
            read_more_button.click()
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "styles_JDC__dang-inner-html__h0K4t"))
            )
        except NoSuchElementException:
            pass

        job_desc_element = driver.find_element(By.CLASS_NAME, "styles_JDC__dang-inner-html__h0K4t")
        job_desc = job_desc_element.get_attribute('innerHTML').strip()

        return time_element, venue_element, job_desc

    except (NoSuchElementException, TimeoutException) as e:
        print(f"Failed to extract walk-in details: {e}")
        return "N/A", "N/A", "N/A"

def write_job_to_csv(writer, city_info, job_details):
    """Write job details to the CSV file."""
    job_data = {
        'City Key': city_info['City Key'],
        'City': city_info['City'],
        'INDUSTRY ID': city_info['INDUSTRY ID'],
        'Job Title': job_details[0],
        'Company': job_details[1],
        'Experience': job_details[2],
        'Location': job_details[3],
        'Salary': job_details[4],
        'Apply URL': job_details[5],
        'Walk-in': job_details[6],
        'Time': job_details[7],
        'Venue': job_details[8],
        'Job Description': job_details[9]
    }
    writer.writerow(job_data)

def process_job_listings(driver, jobs, city_info, writer):
    """Process and extract details from each job listing."""
    print(f"Processing {len(jobs)} job listings.")
    for job in jobs:
        try:
            job_details = extract_job_details(job)
            walkin = "Yes" if job.find_elements(By.CSS_SELECTOR, '.ttc__walk-in') else "No"
            time, venue, job_desc = ("N/A", "N/A", "N/A")
            if walkin == "Yes":
                time, venue, job_desc = extract_walkin_details(driver, job_details[5])
            write_job_to_csv(writer, city_info, job_details + (walkin, time, venue, job_desc))
        except Exception as e:
            print(f"Error processing job: {e}")

def scrape_jobs(driver, base_url, query_params, city_info, writer):
    """Scrape job listings from multiple pages."""
    total_jobs = get_total_jobs(driver, base_url, query_params)
    max_pages = calculate_max_pages(total_jobs)
    print(f"Total jobs: {total_jobs}, Max pages: {max_pages}")

    for page_number in range(1, max_pages + 1):
        page_url = f"{base_url}-{page_number}{query_params}"
        print(f"Opening {page_url}")

        try:
            driver.get(page_url)
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.srp-jobtuple-wrapper'))
            )
            job_listings = driver.find_elements(By.CSS_SELECTOR, '.srp-jobtuple-wrapper')
            print(f"Found {len(job_listings)} job listings on page {page_number}.")

            if not job_listings:
                print("No more jobs found. Exiting loop.")
                break

            process_job_listings(driver, job_listings, city_info, writer)
            print(f"Completed page {page_number}.")

        except TimeoutException as e:
            print(f"TimeoutException: Unable to load page {page_url}. Error: {e}")
            continue
        except Exception as e:
            print(f"Error while processing page {page_url}: {e}")
            continue

def main():
    with open('params1.csv', 'r') as file:
        reader = csv.DictReader(file)
        query_urls = list(reader)

    driver = setup_driver()

    try:
        base_url = 'https://www.naukri.com/walkin-jobs'

        with open('all_job_listings_params_test1page.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'City Key', 'City', 'INDUSTRY ID', 'Job Title', 'Company', 'Experience',
                'Location', 'Salary', 'Apply URL', 'Walk-in', 'Time', 'Venue', 'Job Description'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for query in query_urls:
                city_info = {
                    'City Key': query['City Key'],
                    'City': query['City'],
                    'INDUSTRY ID': query['INDUSTRY ID']
                }
                query_params = query['query']
                scrape_jobs(driver, base_url, query_params, city_info, writer)

    finally:
        driver.quit()
        print("Driver closed and process completed.")

if __name__ == "__main__":
    main()