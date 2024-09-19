import logging
import pandas as pd
from datetime import datetime
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters
import schedule
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)

# Create an empty list to store job data
jobs_data = []

def on_data(data: EventData):
    logging.info(f'[ON_DATA] {data.title}, {data.company}, {data.link}')
    
    # Append job data to the list
    jobs_data.append({
        'Job Title': data.title,
        'Location': data.location,
        'Company': data.company,
        'Date': data.date,
        'Job Link': data.link,
    })

def on_metrics(metrics: EventMetrics):
    logging.info(f'[ON_METRICS] {metrics}')

def on_error(error):
    logging.error(f'[ON_ERROR] {error}')

def on_end():
    logging.info('[ON_END] Scraping process finished.')
    
    # Save the collected job data to an Excel file using pandas
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = fr'C:\Users\Intern\Desktop\Project_Mob\linkedin_job_{current_date}.xlsx'
    
    df = pd.DataFrame(jobs_data)
    df.drop_duplicates(subset='Job Link', keep='first', inplace=True)
    
    df.to_excel(file_path, index=False)
    logging.info(f"Data saved to 'linkedin_job_{current_date}.xlsx'")
    
    # Send email with the file as an attachment
    try:
        send_email(file_path)
    except Exception as e:
        logging.error(f"Error sending email: {e}")

scraper = LinkedinScraper(
    chrome_executable_path=None,
    chrome_binary_location=None,
    chrome_options=None,
    headless=True,
    max_workers=1,
    slow_mo=3,
    page_load_timeout=60
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
    Query(
        query='BACKEND DEVELOPER',
        options=QueryOptions( 
            locations=['INDIA'],
            apply_link=False,
            skip_promoted_jobs=False,
            page_offset=0,
            limit=10,
            filters=QueryFilters(
                company_jobs_url=None,
                relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.DAY,
                type=[TypeFilters.CONTRACT,TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE,ExperienceLevelFilters.MID_SENIOR],
                base_salary=None,
                industry=None
            )
        )
    ),
]

def job_scraping_task():
    global jobs_data
    jobs_data = []  # Reset the jobs_data list before each run
    scraper.run(queries)
    logging.info("Job scraping task executed.")

# Schedule the job to run daily at a specific time 
# schedule.every().day.at("22:35").do(job_scraping_task)
schedule.every(10).minutes..do(job_scraping_task)

logging.info("Scheduler started. Job scraping will run daily .")

def send_email(file_path):
    # Email configuration
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Use 465 for SSL
    smtp_user = os.getenv('SMTP_USER', 'ajkmr2525@gmail.com')
    smtp_password = os.getenv('SMTP_PASSWORD', 'abgv wqyt vllf fclm')  # Use environment variable or app password

    from_email = smtp_user
    to_email = 'ajeeth@annulartechnologies.com'
    cc_email = 'praveen@annulartechnologies.com'

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Cc'] = cc_email
    msg['Subject'] = f"LinkedIn Jobs Data - {datetime.now().strftime('%Y-%m-%d')}"

    # Email body
    body = "Attached is the LinkedIn jobs data."
    msg.attach(MIMEText(body, 'plain'))

    # Attach the Excel file
    with open(file_path, 'rb') as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

    # Send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection with TLS
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except smtplib.SMTPException as e:
        logging.error(f"Error sending email: {e}")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
