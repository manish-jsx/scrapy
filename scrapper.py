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

def get_job_listings(driver, url):
    """Retrieve job listings from a given URL."""
    driver.get(url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.srp-jobtuple-wrapper'))
        )
        job_elements = driver.find_elements(By.CSS_SELECTOR, '.srp-jobtuple-wrapper')
        job_urls = []
        for job in job_elements:
            try:
                apply_url = job.find_element(By.CSS_SELECTOR, '.title').get_attribute('href')
                job_urls.append(apply_url)
                print(f"Captured apply URL: {apply_url}")
            except NoSuchElementException:
                continue
        return job_urls
    except TimeoutException as e:
        print(f"TimeoutException: Unable to load page {url}. Error: {e}")
        return []

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

def scrape_jobs(driver, base_url, query_params, city_key, city, industry_id, writer):
    """Scrape job URLs from multiple pages."""
    total_jobs = get_total_jobs(driver, base_url, query_params)
    max_pages = calculate_max_pages(total_jobs)
    
    for page_number in range(1, max_pages + 1):
        page_url = f"{base_url}-{page_number}{query_params}"
        print(f"Opening {page_url}")

        job_urls = get_job_listings(driver, page_url)
        for url in job_urls:
            writer.writerow({
                'City Key': city_key,
                'City': city,
                'INDUSTRY ID': industry_id,
                'Apply URL': url
            })

        print(f"Completed page {page_number}.")

def expand_read_more(driver):
    try:
        read_more = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "p.styles_read-more__MyWkb a.styles_read-more-link__dD_5h"))
        )
        read_more.click()
    except Exception as e:
        print(f"Read more link not found or clickable: {str(e)}")

def scrape_job_description(driver):
    """Scrape the job description after expanding 'read more'."""
    try:
        expand_read_more(driver)
        job_desc_element = driver.find_element(By.CSS_SELECTOR, "section.styles_job-desc-container__txpYf")
        job_description = job_desc_element.text
        print(job_description)
        return job_description
    except TimeoutException:
        print("Timed out waiting for job description element.")
        return "N/A"
    except NoSuchElementException as e:
        print(f"Job description element not found: {str(e)}")
        return "N/A"

def extract_job_details(driver, url):
    """Extract job details from the job URL."""
    driver.get(url)
    try:
        wait = WebDriverWait(driver, 3)
        
        try:
            banner_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div > main > div.styles_banner-container__bYQEf > img')))
            use_alternate_xpaths = True
            print("Banner element found, using alternate XPaths.")
        except TimeoutException:
            use_alternate_xpaths = False
            print("Banner element not found, using default XPaths.")

        if use_alternate_xpaths:
            title_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[1]/header/h1')))
            company_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[1]/div/a')))
            salary_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[2]/div[1]/div[2]')))
            time_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[2]/div[4]/span')))
            venue_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[2]/div[5]/span')))
            experience_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[2]/div[1]/section[1]/div[1]/div[2]/div[1]/div[1]/span')))
        else:
            title_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[1]/header/h1')))
            company_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[1]/div/a')))
            salary_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[2]/div[1]/div[2]/span')))
            time_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[2]/div[4]/span')))
            venue_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[2]/div[4]/span')))
            experience_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main/div[1]/div[1]/section[1]/div[1]/div[2]/div[1]/div[1]/span')))

        expand_read_more(driver)
        job_description = scrape_job_description(driver)
        
        job_details = {
            'Job Title': title_element.text,
            'Company': company_element.text,
            'Experience': experience_element.text,
            'Salary': salary_element.text,
            'Time': time_element.text,
            'Venue': venue_element.text,
            'Job Description': job_description,
        }
        print(f"Extracted details: {job_details}")
        return job_details
        
    except TimeoutException:
        print(f"Timed out waiting for elements on {url}")
        return {}
    except NoSuchElementException as e:
        print(f"Element not found on {url}: {e}")
        return {}

def main():
    driver = setup_driver()
    
    # Step 1: Extract job URLs
    try:
        with open('params_input.csv', 'r') as file:
            reader = csv.DictReader(file)
            query_urls = list(reader)

        base_url = 'https://www.naukri.com/walkin-jobs'

        with open('jobs_url_scrap.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['City Key', 'City', 'INDUSTRY ID', 'Apply URL']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for query in query_urls:
                query_params = query['query']
                city_key = query['City Key']
                city = query['City']
                industry_id = query['INDUSTRY ID']

                scrape_jobs(driver, base_url, query_params, city_key, city, industry_id, writer)
                print(f"Completed scraping URLs for city: {city}")

    except Exception as e:
        print(f"An error occurred during URL scraping: {e}")
    
    # Step 2: Extract job details
    try:
        with open('jobs_url_scrap.csv', 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            job_urls = list(reader)

        with open('output_scrap.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['City Key', 'City', 'INDUSTRY ID', 'Job Title', 'Company', 'Salary', 'Apply URL', 'Walk-in', 'Experience', 'Time', 'Venue', 'Job Description']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for job in job_urls:
                url = job['Apply URL']
                city_key = job['City Key']
                city = job['City']
                industry_id = job['INDUSTRY ID']

                job_details = extract_job_details(driver, url)
                if job_details:
                    job_details.update({
                        'City Key': city_key,
                        'City': city,
                        'INDUSTRY ID': industry_id,
                        'Apply URL': url,
                        'Walk-in': 'Yes'  # Assuming all scraped jobs are walk-ins
                    })
                    writer.writerow(job_details)

    except Exception as e:
        print(f"An error occurred during job details extraction: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
