from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, OnSiteOrRemoteFilters, SalaryBaseFilters
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
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
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)

# Create an empty list to store job data
jobs_data = []
scraped_companies = {}

# Regular expression patterns for extraction
email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
phone_pattern = r"\+91[-.\s]?\d{2}[-.\s]?\d{2}"
salary_pattern = r'₹\s?\d+(?:,\d{3})(?:\.\d{1,2})?|Rs\.\s?\d+(?:,\d{3})(?:\.\d{1,2})?|INR\s?\d+(?:,\d{3})*(?:\.\d{1,2})?'
experience_pattern = r'(\d+)[-\s](\d)\+?\s*years?'

# Expanded skills list and regex pattern (distinct skills)
skills_list = [
    # Back-End Development
    'Python', 'Java', 'JavaScript', 'C#', 'Ruby', 'PHP', 'C++', 'Go', 'Node.js', 'Scala',
    # Mobile Development
    'Kotlin', 'Swift', 'Objective-C', 'Flutter', 'React Native',
    # Web Development
    'HTML', 'CSS', 'TypeScript', 'React', 'Angular', 'Vue.js'
]

skills_pattern = r'(?i)\b(?:' + '|'.join(set(skills_list)) + r')\b'

# Google search scraping helper functions
USER_AGENTS = [
    "Mozilla/2.0 (Windows NT 10.0; Win64; x64) AppleWebKit/237.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/237.36",
    "Mozilla/2.0 (X11; Linux x86_64) AppleWebKit/237.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/237.36"
]

def remove_duplicates(lst):
    return list(dict.fromkeys(lst))

def get_email(html):
    return remove_duplicates(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}", html))

def get_phone(html):
    return remove_duplicates(re.findall(r"\+91[-.\s]?\d{2}[-.\s]?\d{2}", html))

def find_contact_links(soup, base_url):
    contact_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if re.search(r"contact|contact us|about|team|support|connect|help", link.text, re.IGNORECASE):
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
        for url in search(company_name, tld="com", num=1, stop=1, pause=2):
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

# Fired once for each successfully processed job
def on_data(data: EventData):
    print('[ON_DATA]', data.title, data.company, data.company_link, data.date, data.link,
          data.insights, len(data.description))
    
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
        if max_years:  # If there's a range
            experience_set.add(f"{min_years} - {max_years} Years")
        else:  # Single value or "3+ Years"
            experience_set.add(f"{min_years}+ Years")

    experience_extracted = ', '.join(experience_set) if experience_set else " "

    print(f"Extracted Experience: {experience_extracted}")

    # Check if email and phone are empty and fetch via Google search if not email or not phone:
    if not email or not phone:
        print(f"Fetching additional contact details for {data.company} via Google search...")
        emails_google, phones_google = scrape_contact_details(data.company)
        email = emails_google if not email else email
        phone = phones_google if not phone else phone

    # Store job data
    jobs_data.append({
        'Hiring Company': data.company,
        'Position': data.title,
        'Skills': skills_extracted,
        'Experience': experience_extracted,
        'Date': data.date,
        'Email': ', '.join(email) if email else " ",
        'Phone': ', '.join(phone) if phone else " ",
        'Cost': ', '.join(salary) if salary else " ",
        'Job Link': data.link,
    })

# Fired once for each page (22 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))

def on_error(error):
    print('[ON_ERROR]', error)

def on_end():
    print('[ON_END]')
    
    # Save the collected job data to an Excel file using pandas
    current_date = datetime.now().strftime('%Y-%m-%d')
    df = pd.DataFrame(jobs_data)
    df.drop_duplicates(subset='Job Link', keep='first', inplace=True)
    df.to_excel(fr'C:\Users\Intern\Desktop\linkedin_excel_\LinkedIn_jobs_today_{current_date}.xlsx', index=False)
    print(f"Data saved to 'linkedin_jobs_today{current_date}.xlsx'")
    
    # List of recipients
    recipients = ['ajeethkumar@annulartechnologies.com', 'praveen.lc@annulartechnologies.com']  # Add more emails as needed
    
    # Send email with the results
    send_email(recipients , 'LinkedIn Jobs Scraping Results', prepare_email_body(jobs_data))

def prepare_email_body(jobs_data):
    body = """
    <h2>LinkedIn Jobs Scraping Results</h2>
    <p>Here are the jobs scraped from LinkedIn:</p>
    <table border="1">
      <tr>
        <th>Hiring Company</th>
        <th>Position</th>
        <th>Skills</th>
        <th>Experience</th>
        <th>Date</th>
        <th>Email</th>
        <th>Phone</th>
        <th>Cost</th>
        <th>Job Link</th>
      </tr>
    """
    
    for job in jobs_data:
        body += f"""
        <tr>
          <td>{job['Hiring Company']}</td>
          <td>{job['Position']}</td>
          <td>{job['Skills']}</td>
          <td>{job['Experience']}</td>
          <td>{job['Date']}</td>
          <td>{job['Email']}</td>
          <td>{job['Phone']}</td>
          <td>{job['Cost']}</td>
          <td><a href="{job['Job Link']}">{job['Job Link']}</a></td>
        </tr>
        """
    
    body += """
    </table>
    <p>Best regards,<br>Your Name</p>
    """
    
    return body

def send_email(recipients, subject, body):
    sender = 'ajkmr2525@gmail.com'
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    
    msg.attach(MIMEText(body, 'html'))
    
    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(sender, 'abgv wqyt vllf fclm')
        smtp.send_message(msg)

# LinkedIn Scraper setup
scraper = LinkedinScraper(
    chrome_executable_path=None,
    headless=True,
    max_workers=1,
    slow_mo=2,
    page_load_timeout=100,
)

scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
   Query(
       query='BACK-END DEVELOPMENT',
       options=QueryOptions(
           locations=['INDIA'],
           apply_link=False,
           skip_promoted_jobs=False,
           page_offset=0,
           limit=20,
           filters=QueryFilters(
               company_jobs_url=None,
               relevance=RelevanceFilters.RELEVANT,
               time=TimeFilters.DAY,
               type=[TypeFilters.CONTRACT],
               on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
               experience=[ExperienceLevelFilters.ASSOCIATE],
           )
       )
   ),
   # Add more queries as needed...
]
# Run the scraper
scraper.run(queries)