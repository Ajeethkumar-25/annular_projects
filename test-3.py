import pandas as pd
from datetime import datetime
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

# Create an empty list to store job data
jobs_data = []

def send_email(file_path):
    # Email configuration
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Use 465 for SSL, 587 for TLS
    smtp_user = os.getenv('SMTP_USER', 'ajkmr2525@gmail.com')  # Sender email
    smtp_password = os.getenv('SMTP_PASSWORD', 'abgv wqyt vllf fclm')  # App-specific password

    from_email = smtp_user
    to_email = "ajeethkumar@annulartechnologies.com"
    # cc_email = ["praveen.lc@annulartechnologies.com", "selva@annulartechnologies.com", "sriram@annulartechnologies.com", "skumaran@annulartechnologies.com"]
    

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    # msg['Cc'] = ",".join(cc_email)
    msg['Subject'] = f"LinkedIn Jobs Data - {datetime.now().strftime('%Y-%m-%d')}"

    # Email body
    body = body = """
    Hi Team Annular,
        Here I am attached the LINKEDIN scraped jobs Excel file for 20-09-2024.
    Kindly review it.
    
    Thanks & Regards,
    Ajeethkumar Muruganandham,
    Data Engineer.
    """

    msg.attach(MIMEText(body, 'plain'))

    # Attach the Excel file
    with open(file_path, 'rb') as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

    # Send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()  # Identify yourself to the SMTP server
            server.starttls()  # Secure the connection with TLS
            server.ehlo()  # Re-identify yourself after starting TLS
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print("Email sent successfully.")
    except smtplib.SMTPException as e:
        print(f"Error sending email: {e}")

def on_data(data: EventData):
    print(f'[ON_DATA] {data.title}, {data.company}, {data.link}')
    
    # Append job data to the list
    jobs_data.append({
        'Job Title': data.title,
        'Location': data.location,
        'Company': data.company,
        'Date': data.date,
        'Job Link': data.link,
    })

def on_metrics(metrics: EventMetrics):
    print(f'[ON_METRICS] {metrics}')

def on_error(error):
    print(f'[ON_ERROR] {error}')

def on_end():
    print('[ON_END] Scraping process finished.')
    
    # Save the collected job data to an Excel file using pandas
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = fr'C:\Users\Intern\Desktop\Project_Mob\linkedin_job\linkedin_job_2_{current_date}.xlsx'
    
    df = pd.DataFrame(jobs_data)
    df.drop_duplicates(subset='Job Link', keep='first', inplace=True)
    
    df.to_excel(file_path, index=False)
    print(f"Data saved to 'linkedin_job_1{current_date}.xlsx'")

    # Send the email with the file attachment
    send_email(file_path)

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
            limit=5,
            filters=QueryFilters(
                company_jobs_url=None,
                relevance=RelevanceFilters.RELEVANT,
                type=[TypeFilters.CONTRACT, TypeFilters.TEMPORARY],
                on_site_or_remote=OnSiteOrRemoteFilters.REMOTE,
                experience=[ExperienceLevelFilters.ASSOCIATE, ExperienceLevelFilters.MID_SENIOR],
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
    print("Job scraping task executed.")

# Execute the job scraping task once and send email with results
job_scraping_task()
