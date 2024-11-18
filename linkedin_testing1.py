from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, OnSiteOrRemoteFilters
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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
from io import BytesIO

# Counter to track completed queries
completed_queries = 0

# Chrome options setup
chrome_options = Options()
chrome_options.binary_location = "/usr/bin/google-chrome"  # Replace with the path to your Chrome
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-dev-shm-usage')

# Set up ChromeDriver service
service = Service("/usr/bin/chromedriver")

# Set up logging
logging.basicConfig(
    filename='script_log.txt',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)
logging.info('Script started')

# Create an empty list to store job data
jobs_data = []
scraped_companies = {}

# Regular expression patterns for extraction
email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
phone_pattern = r"\+91[-.\s]?\d{5}[-.\s]?\d{5}"
salary_pattern = r'₹\s?\d+(?:,\d{3})*(?:\.\d{1,2})?|Rs\.\s?\d+(?:,\d{3})*(?:\.\d{1,2})?'
experience_pattern = r'(\d+\.?\d*)\s*[-\s]*(\d*\.?\d*)\+?\s*years?'

# Expanded skills list and regex pattern (distinct skills)
skills_list = [
    # QA/Automation (Selenium)
    'Selenium WebDriver', 'Selenium Grid', 'Selenium IDE', 'TestNG', 'JUnit', 'Cucumber', 'Page Object Model',
    'Java', 'Python', 'C#', 'JavaScript', 'Jenkins', 'Git', 'Maven', 'Gradle', 'Appium', 'RestAssured',
    'Apache POI', 'JUnit', 'TestNG', 'BDD', 'TDD', 'Cross-Browser Testing', 'BrowserStack', 'Sauce Labs',
    'Continuous Integration', 'Continuous Delivery', 'Log4j', 'Test Reporting', 'Data-Driven Testing',
    'Postman', 'API Testing', 'Performance Testing', 'Regression Testing', 'Smoke Testing', 'Sanity Testing',
    'Docker', 'Kubernetes', 'Cloud Testing', 'CI/CD Pipelines', 'Error Handling', 'Exception Handling',

    # Back-End Development
    'Python', 'Java', 'JavaScript', 'C#', 'Ruby', 'PHP', 'C\+\+', 'Go', 'Node\.js', 'Scala',
    'Spring', 'Hibernate', 'Django', 'Flask', 'Express', 'ASP\.NET', 'RESTful', 'GraphQL',
    'SQL', 'NoSQL', 'PostgreSQL', 'MySQL', 'MongoDB', 'Redis', 'ElasticSearch', 'Docker',
    'Kubernetes', 'AWS', 'Azure', 'Google Cloud', 'CI/CD', 'Jenkins', 'Git', 'GraphQL',
    'Apache Kafka', 'RabbitMQ', 'Nginx', 'Redis', 'Memcached', 'Microservices', 'DevOps',
   
    # Data Engineering with Large Language Models (LLMs)
    'Python', 'TensorFlow', 'PyTorch', 'Hugging Face Transformers', 'BERT', 'GPT-3', 'GPT-4', 'LLM Fine-Tuning',
    'OpenAI API', 'Deep Learning', 'NLP', 'Natural Language Understanding', 'Natural Language Generation',
    'Text Preprocessing', 'Text Classification', 'Text Generation', 'Reinforcement Learning', 'Transfer Learning',
    'Word Embeddings', 'Transformer Models', 'Neural Networks', 'Model Deployment',
    'Model Optimization', 'Tokenization', 'Zero-Shot Learning', 'Fine-Tuning', 'Data Labeling', 'Data Augmentation',
    'Cloud AI Services (AWS Sagemaker, Google AI, Azure ML)', 'ML Ops', 'Kubeflow', 'Model Monitoring', 'Model Accuracy Metrics',

    # Mobile Development
    'Java', 'Kotlin', 'Swift', 'Objective-C', 'Flutter', 'React Native', 'Xamarin', 'Android',
    'iOS SDK', 'UIKit', 'Jetpack Compose', 'Core Data', 'Realm', 'Firebase', 'Android Jetpack',
    'RxJava', 'Dagger', 'MVP', 'MVVM', 'RESTful APIs', 'GraphQL', 'Push Notifications',

    # MERN Stack
    'MongoDB', 'Express.js', 'React.js', 'Node.js', 'JavaScript', 'TypeScript', 'Redux', 'JQuery',
    'HTML', 'CSS', 'Bootstrap', 'Tailwind CSS', 'Next.js', 'GraphQL', 'JWT Authentication', 'RESTful APIs',
    'Socket.IO', 'WebSockets', 'Mongoose', 'MongoDB Atlas', 'Passport.js', 'NPM', 'Webpack', 'Babel',
    'ES6+', 'Webpack', 'Gulp', 'Jest', 'Mocha', 'Chai', 'Docker', 'Kubernetes',

    # Web Development
    'HTML', 'CSS', 'JavaScript', 'TypeScript', 'React', 'Angular', 'Vue\.js', 'SASS', 'LESS',
    'Bootstrap', 'Tailwind CSS', 'Webpack', 'Gulp', 'Parcel', 'JQuery', 'Node\.js', 'Express',
    'Next\.js', 'Gatsby', 'Nuxt\.js', 'Server-Side Rendering', 'Progressive Web Apps', 'SSR'
]


skills_pattern = r'(?i)\b(?:' + '|'.join(set(skills_list)) + r')\b'

# Google search scraping helper functions
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def remove_duplicates(lst):
    return list(dict.fromkeys(lst))

def get_email(html):
    return remove_duplicates(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}", html))

def get_phone(html):
    return remove_duplicates(re.findall(r"\+91[-.\s]?\d{5}[-.\s]?\d{5}", html))

def find_contact_links(soup, base_url):
    contact_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if re.search(r"contact|contact us|about|team|support|connect|help", link.text, re.I):
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
            print(f"Error: {e}. Retrying in {2**i} seconds.")
            time.sleep(2**i)  # Exponential backoff
    return None

def scrape_contact_details(company_name):
    if company_name in scraped_companies:
        print(f"Reusing details for {company_name}.")
        return scraped_companies[company_name]['emails'], scraped_companies[company_name]['phones']

    search_result_url = ''
    try:
        for url in search(company_name, tld='com', num=1, stop=1, pause=2):
            search_result_url = url
            break
    except Exception as e:
        print(f"Error searching for {company_name}: {e}")
        return None, None

    if search_result_url:
        try:
            res = make_request(search_result_url)
            if not res:
                print(f"Failed to load {search_result_url}.")
                return None, None

            soup = BeautifulSoup(res.text, 'lxml')
            emails = get_email(soup.get_text())
            phones = get_phone(soup.get_text())

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
        except Exception as e:
            print(f"Error scraping {company_name}: {e}")
            return None, None
    return None, None

def send_email_with_attachment(recipients, subject, body, attachment_path):
    sender_email = "ajkmr2525@gmail.com"  # Replace with your email
    sender_password = "vbwu oaeq liby vmkh"  # Replace with your email password
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    filename = os.path.basename(attachment_path)
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {filename}")
        msg.attach(part)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipients, msg.as_string())
        server.quit()
        print(f"Email successfully sent to {', '.join(recipients)}")
    except Exception as e:
        print(f"Failed to send email. Error: {str(e)}")

def on_data(data: EventData):
    print('[ON_DATA]', data.title, data.company, data.company_link, data.date, data.link)
    
    email = re.findall(email_pattern, data.description)
    phone = re.findall(phone_pattern, data.description)
    skills = re.findall(skills_pattern, data.description)
    experience_matches = re.findall(experience_pattern, data.description)
    salary = re.findall(salary_pattern, data.description)
    
    skills_extracted = ', '.join(set(sorted(set(skills)))) if skills else " "
    
    experience_set = set()
    for match in experience_matches:
        min_years = match[0]
        max_years = match[1]
        
        if max_years:
            experience_set.add(f"{min_years} - {max_years} Years")
        else:
            experience_set.add(f"{min_years}+ Years")
    
    experience_extracted = ', '.join(experience_set) if experience_set else " "
    print(f"Extracted Experience: {experience_extracted}")

    if not email or not phone:
        print(f"Fetching additional contact details for {data.company} via Google search.")
        emails_google, phones_google = scrape_contact_details(data.company)
        email = emails_google if not email else email
        phone = phones_google if not phone else phone

    jobs_data.append({
        'Hiring Company': data.company,
        'Position': data.title,
        'Skills': skills_extracted,
        'Experince': experience_extracted,
        'Date': data.date,
        'Email': ', '.join(email) if email else " ",
        'Phone': ', '.join(phone) if phone else " ",
        'Cost': ', '.join(salary) if salary else " ",
        'Job Link': data.link,
    })

def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))

def on_error(error):
    print('[ON_ERROR]', error)

# LinkedIn Scraper setup
scraper = LinkedinScraper(
    chrome_executable_path=service.path,
    headless=True,
    max_workers=1,
    slow_mo=5,
    page_load_timeout=100
)

# Define common query options
COMMON_FILTERS = QueryFilters(
    company_jobs_url=None,
    relevance=RelevanceFilters.RELEVANT,
    time=TimeFilters.DAY,
    type=[TypeFilters.CONTRACT, TypeFilters.TEMPORARY],
    on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
    experience=[ExperienceLevelFilters.ASSOCIATE, ExperienceLevelFilters.MID_SENIOR],
    base_salary=None,
    industry=None
)

COMMON_OPTIONS = QueryOptions(
    locations=['INDIA'],
    apply_link=False,
    skip_promoted_jobs=False,
    page_offset=0,
    limit=500,
    filters=COMMON_FILTERS
)

# List of search queries
SEARCH_QUERIES = [
    'BACK-END DEVELOPMENT',
    'BACKEND DEVELOPMENT',  # Fixed typo
    'BACK END DEVELOPMENT',  # Fixed typo
    'BACK-END DEVELOPER',
    'BACKEND DEVELOPER',
    'BACK END DEVELOPER',
    'JAVA BACKEND DEVELOPER',
    'CORE JAVA DEVELOPER',
    'JAVA CORE DEVELOPER',
    'JAVA DEVELOPER',
    'MICROSERVICES',
    'SPRINGBOOT',
    'SPRINGBOOT DEVELOPER',
    'SPRING',
    'SPRING DEVELOPER',
    'FULL-STACK DEVELOPMENT',
    'FULL STACK DEVELOPER',
    'REACT DEVELOPER',
    'NODE.JS DEVELOPER',
    'DATA ENGINEER',
    'MERN STACK DEVELOPER',
    'JAVASCRIPT DEVELOPER',
    'ANGULAR DEVELOPER',
    'FRONT-END DEVELOPER',
    'TYPESCRIPT DEVELOPER'
]

# Generate queries list
queries = [
    Query(
        query=query,
        options=COMMON_OPTIONS
    )
    for query in SEARCH_QUERIES
]

total_queries = len(queries)

def on_end():
    global completed_queries
    completed_queries += 1
    
    if completed_queries == total_queries:
        print('[ON_END]')
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        file_path = r'/home/ubuntu/LinkedIn_jobs_today.xlsx'
        
        df = pd.DataFrame(jobs_data)
        df.drop_duplicates(subset='Job Link', keep='first', inplace=True)
        df.to_excel(file_path, index=False)
        print(f"Data saved to 'linkedin_jobs_today_{current_date}.xlsx'")
        
        recipients = ['ajeethkumar@annulartechnologies.com', 'ajeeth.kumarm23@gmail.com']
        subject = f'Scraped LinkedIn Job Data {current_date}'
        body = f"""
        Hi Team,

        Please find the LinkedIn job data for {current_date} attached.

        Best regards,
        Ajeethkumar
        Data Engineer
        """
        
        send_email_with_attachment(recipients, subject, body, file_path)
    else:
        print(f'[ON_END] Query completed, waiting for remaining {total_queries - completed_queries} queries.')

# Set up event handlers
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

# Run the scraper
scraper.run(queries)