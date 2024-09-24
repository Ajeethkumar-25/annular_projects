from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, OnSiteOrRemoteFilters
import pandas as pd
import re
import os
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from googlesearch import search
from random import choice
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Logging configuration
logging.basicConfig(level=logging.INFO)
# Set up Chrome options
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--remote-debugging-port=9222')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')
options.add_argument('--start-maximized')

# Initialize the WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://www.google.com/login")
# Store job data
jobs_data = []
scraped_companies = {}

# Regular expression patterns
email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
phone_pattern = r"\+91[-.\s]?\d{5}[-.\s]?\d{5}"
salary_pattern = r'₹\s?\d+(?:,\d{3})(?:\.\d{1,2})?|INR\s?\d+(?:,\d{3})*(?:\.\d{1,2})?'
experience_pattern = r'(\d+)[-\s](\d)\+?\s*years?'

# Skills list and pattern
skills_list = [
    'Python', 'Java', 'JavaScript', 'C#', 'Ruby', 'PHP', 'C\\+\\+', 'Go', 'Node\\.js', 'Scala',
    'Spring', 'Hibernate', 'Django', 'Flask', 'Express', 'ASP\\.NET', 'RESTful', 'GraphQL',
    'SQL', 'NoSQL', 'PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'ElasticSearch', 'Docker',
    'Kubernetes', 'AWS', 'Azure', 'Google Cloud', 'CI/CD', 'Jenkins', 'Git', 'GraphQL',
    'Apache Kafka', 'RabbitMQ', 'Nginx', 'Redis', 'Memcached', 'Microservices', 'DevOps', 'Selenium',
    
    # Mobile Development
    'Java', 'Kotlin', 'Swift', 'Objective-C', 'Flutter', 'React Native', 'Xamarin', 'Android SDK',
    'iOS SDK', 'UIKit', 'Jetpack Compose', 'Core Data', 'Realm', 'Firebase', 'Android Jetpack',
    'RxJava', 'Dagger', 'MVP', 'MVVM', 'RESTful APIs', 'GraphQL', 'Push Notifications',

    # Web Development
    'HTML', 'CSS', 'JavaScript', 'TypeScript', 'React', 'Angular', 'Vue\\.js', 'SASS', 'LESS',
    'Bootstrap', 'Tailwind CSS', 'Webpack', 'Gulp', 'Parcel', 'JQuery', 'Node\\.js', 'Express',
    'Next\\.js', 'Gatsby', 'Nuxt\\.js', 'Server-Side Rendering', 'Progressive Web Apps', 'Single Page Applications'
]

# Escape backslashes and create raw strings
escaped_skills_list = [skill.replace('\\', '\\\\') for skill in skills_list]

# Create the skills_pattern
skills_pattern = r'(?i)\b(?:' + '|'.join(escaped_skills_list) + r')\b'


# Random user agents for Google search
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

def remove_duplicates(lst):
    return list(dict.fromkeys(lst))

def get_email(text):
    return remove_duplicates(re.findall(email_pattern, text))

def get_phone(text):
    return remove_duplicates(re.findall(phone_pattern, text))

def find_contact_links(soup, base_url):
    contact_links = []
    for link in soup.find_all('a', href=True):
        if re.search(r"contact|about|support|team|connect|help", link.text, re.IGNORECASE):
            href = link['href']
            if href.startswith('/'):
                href = base_url.rstrip('/') + href
            contact_links.append(href)
    return contact_links

def make_request(url):
    for i in range(3):  # Retry up to 3 times
        try:
            headers = {'User-Agent': choice(USER_AGENTS)}
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                return res
        except Exception as e:
            logging.error(f"Error: {e}. Retrying in {2**i} seconds.")
            time.sleep(2**i)  # Exponential backoff
    return None

def scrape_contact_details(company_name):
    if company_name in scraped_companies:
        logging.info(f"Reusing details for {company_name}.")
        return scraped_companies[company_name]['emails'], scraped_companies[company_name]['phones']
    
    search_result_url = ''
    try:
        for url in search(company_name, num=1, stop=1, pause=2):
            search_result_url = url
            break
    except Exception as e:
        logging.error(f"Error searching for {company_name}: {e}")
        return None, None

    if search_result_url:
        res = make_request(search_result_url)
        if not res:
            logging.error(f"Failed to load {search_result_url}.")
            return None, None

        soup = BeautifulSoup(res.text, 'lxml')
        emails = get_email(soup.get_text())
        phones = get_phone(soup.get_text())
        
        # Scrape contact links
        contact_links = find_contact_links(soup, search_result_url)
        for contact_url in contact_links:
            contact_res = make_request(contact_url)
            if contact_res:
                contact_soup = BeautifulSoup(contact_res.text, 'lxml')
                emails += get_email(contact_soup.get_text())
                phones += get_phone(contact_soup.get_text())
        
        emails = remove_duplicates(emails)
        phones = remove_duplicates(phones)
        scraped_companies[company_name] = {'emails': emails, 'phones': phones}

        return emails, phones
    return None, None

# Event handler for job data
def on_data(data: EventData):
    logging.info(f"[ON_DATA] {data.title} at {data.company}")

    email = re.findall(email_pattern, data.description)
    phone = re.findall(phone_pattern, data.description)
    skills = re.findall(skills_pattern, data.description)
    experience = re.findall(experience_pattern, data.description)
    salary = re.findall(salary_pattern, data.description)

    # Combine extracted skills and experience
    skills_extracted = ', '.join(set(skills)) if skills else "N/A"
    experience_extracted = ', '.join(set(f"{match[0]}-{match[1]} years" for match in experience)) if experience else "N/A"

    # Fallback to Google search for contact details
    if not email or not phone:
        logging.info(f"Fetching additional contact details for {data.company}...")
        emails_google, phones_google = scrape_contact_details(data.company)
        email = emails_google if not email else email
        phone = phones_google if not phone else phone

    # Append job data
    jobs_data.append({
        'Company': data.company,
        'Title': data.title,
        'Skills': skills_extracted,
        'Experience': experience_extracted,
        'Date': data.date,
        'Email': ', '.join(email) if email else "N/A",
        'Phone': ', '.join(phone) if phone else "N/A",
        'Salary': ', '.join(salary) if salary else "N/A",
        'Job Link': data.link,
    })

# Event handler for metrics
def on_metrics(metrics: EventMetrics):
    logging.info(f"[ON_METRICS] {metrics}")

def on_error(error):
    logging.error(f"[ON_ERROR] {error}")
    # Retry logic for errors
    if "Failed to load container selector" in str(error):
        logging.info("Retrying due to failed container selector...")
        # Retry the query after a brief delay
        time.sleep(10)
        scraper.run(queries)

# Event handler for when scraping ends
def on_end():
    logging.info("Scraping complete")
    current_date = datetime.now().strftime('%Y-%m-%d')
    df = pd.DataFrame(jobs_data)
    df.drop_duplicates(subset='Job Link', keep='first', inplace=True)
    file_path = f'C:/Users/Intern/Desktop/linkedin_excel_/LinkedIn_jobs_today_2_{current_date}.xlsx'
    # send_email(file_path)
    df.to_excel(file_path, index=False)
    print(f"Data saved to '{file_path}'")


# LinkedIn Scraper setup
scraper = LinkedinScraper(
    chrome_executable_path=None,
    chrome_binary_location=None,
    chrome_options=None,
    headless=True,
    max_workers=1,
    slow_mo=0.5,
    page_load_timeout=40
)

scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
    Query(
        query='BACK-END DEVELOPMENT',
        options=QueryOptions( 
            locations=['India'],
            apply_link=False,
            skip_promoted_jobs=True,
            limit=10,
            filters=QueryFilters(
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT, TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE, ExperienceLevelFilters.MID_SENIOR]
            )
        )
    ),
    Query(
        query='BACKEND DEVELOPEMENT',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='BACK END DEVELOMENT',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='BACK-END DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.PART_TIME, TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='BACKEND DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='BACK END DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.PART_TIME, TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='JAVA BACKEND DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.PART_TIME, TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='CORE JAVA DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='JAVA CORE DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='JAVA DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='MICROSERVICES',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='SPRINGBOOT',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='SPRINGBOOT DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='SPRING',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    ),
    Query(
        query='SPRING DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
            skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
            page_offset=0,  # How many pages to skip
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,  # Filter by companies.                
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry = None
            )
        )
    )
]
scraper.run(queries) 

# Function to send email with job data as attachment
def send_email(file_path):
    # Email configuration
    smtp_server = 'smtp.gmail.com'
    smtp_port = 25 # Use 587 for TLS
    smtp_user = 'ajkmr2525@gmail.com'  # Replace with your email
    smtp_password = 'abgv wqyt vllf fclm'  # Replace with your app-specific password

    from_email = smtp_user       
    to_email = 'ajeethkumar@annulartechnologies.com'

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = f"LinkedIn Jobs Data - {datetime.now().strftime('%Y-%m-%d')}"

    body = """Hi Team, 
        Here I am attached is the LinkedIn jobs data, So kindly review it.
    
Thanks & Regards,
Ajeethkumar[Data Engineer], 
Praveen LC[Data Engineer]
    """
    msg.attach(MIMEText(body, 'plain'))

    # Attach the Excel file
    try:
        with open(file_path, 'rb') as file:
            part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)
    except Exception as e:
        print(f"Failed to attach the file: {e}")
        return

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
            print(f"Email sent to {to_email} with the attachment {os.path.basename(file_path)}")
    except Exception as e:
        print(f"Failed to send email: {e}")
if __name__ == "__main__":
    current_date = datetime.now().strftime('%Y-%m-%d')
    send_email(file_path = f'C:/Users/Intern/Desktop/linkedin_excel_/LinkedIn_jobs_today_2_{current_date}.xlsx')


