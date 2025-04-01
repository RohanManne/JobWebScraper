# Standard library imports
import json
import os
import boto3
from email.message import EmailMessage
import smtplib
import ssl
from datetime import datetime, timedelta

# Third-party imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials

SECRETS = {
    "groq_api_key": "",
    "email_config": {
        "sender": "",
        "password": ""
    },
    "google_sheets_creds": {
        "type": "service_account",
        "project_id": "j",
        "private_key_id": "",
        "private_key": "",
        "client_email": "",
        "client_id": "",
        "auth_uri": "",
        "token_uri": "",
        "auth_provider_x509_cert_url": "",
        "client_x509_cert_url": "",
        "universe_domain": "googleapis.com"
    }
}

client = Groq(api_key=SECRETS["groq_api_key"])

class JobConfig:
    """Configuration class for job scraping settings."""
    def __init__(self, job_type, sheet_id, email_receivers, llm_model, exclusion_keywords):
        self.job_type = job_type
        self.sheet_id = sheet_id
        self.email_receivers = email_receivers
        self.llm_model = llm_model
        self.exclusion_keywords = exclusion_keywords
        # Use /tmp for Lambda storage
        self.base_dir = "/tmp/data"
        self.today_date = datetime.today().strftime("%Y-%m-%d")
        self.date_dir = os.path.join(self.base_dir, f"{job_type}{self.today_date}")
        
        # Create directory structure
        os.makedirs(self.date_dir, exist_ok=True)
        
        # Set up file paths for each data source
        self.filenames = {
            "yc": os.path.join(self.date_dir, f"{job_type}ycombinator_jobs_{self.today_date}.csv"),
            "sequoia": os.path.join(self.date_dir, f"{job_type}SequoiaJobs_{self.today_date}.csv"),
            "nextview": os.path.join(self.date_dir, f"{job_type}nextview_jobs_{self.today_date}.csv"),
            "greylock": os.path.join(self.date_dir, f"{job_type}greylock_jobs_{self.today_date}.csv"),
            "andreessen": os.path.join(self.date_dir, f"{job_type}andreessenhorowitz_jobs_{self.today_date}.csv"),
            "combined": os.path.join(self.date_dir, f"{job_type}combined_jobs_{self.today_date}.csv"),
            "microsoft": os.path.join(self.date_dir, f"{job_type}microsoft_jobs_{self.today_date}.csv"),
            "amazon": os.path.join(self.date_dir, f"{job_type}amazon_jobs_{self.today_date}.csv")
        }

def get_headers():
    """Get headers for different API requests."""
    return {
        'yc': {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        },
        'sequoia': {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
        },
        'html': {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    }

def get_urls():
    """Get base URLs for different job boards."""
    return {
        'yc': "https://www.ycombinator.com",
        'sequoia': "https://jobs.sequoiacap.com/api-boards/search-jobs",
        'nextview': "https://jobs.nextview.vc/api-boards/search-jobs",
        'greylock': "https://jobs.greylock.com/api-boards/search-jobs",
        'andreessen': "https://jobs.a16z.com/api-boards/search-jobs"
    }

def upload_to_google_sheet(config):
    """Upload combined CSV data to Google Sheets."""
    try:
        creds = Credentials.from_service_account_info(
            SECRETS["google_sheets_creds"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        
        # Connect to Google Sheets
        client = gspread.authorize(creds)
        workbook = client.open_by_key(config.sheet_id)
        worksheet_list = workbook.worksheets()
        
        # Handle existing worksheet
        if config.today_date in [sheet.title for sheet in worksheet_list]:
            print(f"Worksheet '{config.today_date}' already exists. Overwriting it...")
            worksheet = workbook.worksheet(config.today_date)
            workbook.del_worksheet(worksheet)

        # Create new worksheet
        worksheet = workbook.add_worksheet(title=config.today_date, rows="1000", cols="20")

        # Upload data if available
        if os.path.exists(config.filenames["combined"]):
            data = pd.read_csv(config.filenames["combined"])
            
            # Handle special values for Google Sheets compatibility
            data = data.replace([float('inf'), float('-inf')], None)
            data = data.replace({pd.NA: None})
            data = data.where(pd.notnull(data), None)
            
            # Convert to list format and clean data
            data_list = [data.columns.tolist()] + data.replace({pd.NA: None}).values.tolist()
            cleaned_data = []
            for row in data_list:
                cleaned_row = []
                for value in row:
                    if pd.isna(value) or pd.isnull(value):
                        cleaned_row.append(None)
                    elif isinstance(value, float) and (value == float('inf') or value == float('-inf')):
                        cleaned_row.append(None)
                    else:
                        cleaned_row.append(value)
                cleaned_data.append(cleaned_row)
            
            # Update worksheet with cleaned data
            worksheet.update(range_name="A1", values=cleaned_data)
            print(f"Data successfully uploaded to Google Sheets in worksheet '{config.today_date}'.")
        else:
            print(f"Combined CSV file '{config.filenames['combined']}' does not exist.")
    except Exception as e:
        print(f"An error occurred while uploading to Google Sheets: {e}")
        import traceback
        print("Detailed error trace:")
        print(traceback.format_exc())

def convert_to_date(timestamp_str):
    """Convert timestamp string to a date object."""
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",       # Format with timezone offset (e.g., 2024-12-06T15:06:00+00:00)
        "%Y-%m-%dT%H:%M:%S.%f%z",   # Format with fractional seconds and timezone (e.g., 2024-12-06T15:06:00.123+00:00)
        "%Y-%m-%dT%H:%M:%SZ",       # Format with 'Z' at the end (e.g., 2024-12-06T15:06:00Z)
        "%Y-%m-%dT%H:%M:%S.%fZ",    # Format with fractional seconds and 'Z' (e.g., 2024-12-06T15:06:00.123Z)
    ]

    try:
        # Handle Unix timestamp (e.g., 1731598080)
        return datetime.utcfromtimestamp(int(timestamp_str)).date()
    except (ValueError, TypeError):
        pass

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt).date()
        except ValueError:
            continue  # Try the next format

    return "Invalid Date"  # If no format matches, return this

    try:
        # Handle Unix timestamp (e.g., 1731598080)
        return datetime.utcfromtimestamp(int(timestamp_str)).date()
    except (ValueError, TypeError):
        pass

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt).date()
        except ValueError:
            continue  # Try the next format

    return "Invalid Date"  # If no format matches, return this

def scrape_yc_jobs(config):
    """Scrape jobs from Y Combinator's job board."""
    jobs_data = []
    urls = get_urls()
    headers = get_headers()
    
    # Get main jobs page
    response = requests.get(f"{urls['yc']}/jobs", headers=headers['yc'])
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        job_posts = soup.find_all("li", class_="my-2 flex h-auto w-full flex-col flex-nowrap rounded border border-[#ccc] bg-beige-lighter px-5 py-3")
        
        for job in job_posts:
            # Extract basic job information
            company_name = job.find("span", class_="block font-bold md:inline")
            company_name_text = company_name.text.strip() if company_name else "N/A"
            job_link = job.find("a", class_="font-semibold text-linkColor")
            job_name = job_link.text.strip() if job_link else "N/A"
            job_href = job_link.get("href") if job_link else None
            date_posted = job.find("span", class_="hidden text-sm text-gray-400 md:inline").text.strip()
            
            if job_href:
                # Get detailed job information
                job_url = f"{urls['yc']}{job_href}"
                job_response = requests.get(job_url, headers=headers['yc'])
                if job_response.status_code == 200:
                    job_soup = BeautifulSoup(job_response.text, "html.parser")
                    additional_details = []
                    details_container = job_soup.find("div", class_="flex flex-row flex-wrap justify-center md:justify-start")
                    if details_container:
                        detail_items = details_container.find_all("div")
                        additional_details = [item.text.strip() for item in detail_items]
                    
                    # Get experience requirements
                    experience_div = job_soup.find("div", string="Experience")
                    experience_text = "N/A"
                    if experience_div:
                        parent_div = experience_div.parent
                        span = parent_div.find("span")
                        experience_text = span.text.strip() if span else "N/A"
                    
                    # Use LLM to check job requirements
                    chat_completion = client.chat.completions.create(
                        messages=[{
                            "role": "user",
                            "content": f"Check the job :{job_name}, {', '.join(additional_details)}, {experience_text} and tell me if it is meant for: NEW GRADS or requires 2 or fewer years of experience. if it is Full Time and if it in the USA only. If it fits all three answer Yes, otherwise answer No. I want you to do a total one word answer: Yes or No, thats it."
                        }],
                        model=config.llm_model,
                    )

                    # Save job if it matches criteria
                    ai_response = chat_completion.choices[0].message.content.strip()
                    if "yes" in ai_response.lower():
                        jobs_data.append({
                            "Company": company_name_text,
                            "Job": job_name,
                            "Details": ", ".join(additional_details),
                            "Date/Time Posted": date_posted,
                            "Link": job_url,
                            "YC": "Y-Combinator"
                        })

    # Save results to CSV
    pd.DataFrame(jobs_data).to_csv(config.filenames["yc"], index=False)
    print(f"YC jobs saved to {config.filenames['yc']}")

def get_job_types(config):
    """Get job types based on configuration."""
    return ["software-engineer"] if config.job_type == "CS" else ["data-scientist", "data-analyst", "Analyst"]

def fetch_microsoft_jobs(config):

    jobs_data = []

    # API endpoint
    url = "https://gcsservices.careers.microsoft.com/search/api/v1/search"

    # Query parameters
    params = {
        "lc": "United States",  # Location
        "exp": "Students and graduates",  # Experience level
        "et": "Full-Time",  # Employment type
        "l": "en_us",  # Language
        "pg": 1,  # Page number
        "pgSz": 30,  # Page size
        "o": "Recent",  # Sort order
        "flt": "true",  # Additional filters
    }

    # Headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.5",
        "Origin": "https://jobs.careers.microsoft.com",
        "Referer": "https://jobs.careers.microsoft.com/",
    }

    # Send GET request
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        try:
            # Parse JSON response
            
            data = response.json()
            result = data.get("operationResult", {}).get("result", {})
            jobs = result.get("jobs", [])

            for job in jobs:
                job_title = job.get("title")
                posting_date = convert_to_date(job.get("postingDate"))
                props = job.get("properties")
                location = props.get("primaryLocation")
                link = "https://jobs.careers.microsoft.com/global/en/job/" + job.get("jobId")

                today = datetime.now().date()
                seven_days_ago = today - timedelta(days=7)
                if posting_date < seven_days_ago:
                    continue

                # Use LLM to check job requirements
                chat_completion = client.chat.completions.create(
                    messages=[{
                        "role": "user",
                        "content": f"Check the job posting:{job_title + location} and tell me if it is meant for: NEW GRADS or requires 2 or fewer years of experience. If it is Full Time and in the USA only and fits this: {get_job_types(config)}, answer Yes, otherwise No."
                    }],
                    model=config.llm_model,
                )
                
                # Save job if it matches criteria
                if "yes" in chat_completion.choices[0].message.content.strip().lower():
                    jobs_data.append({
                        "Company": "Microsoft",
                        "Job": job_title,
                        "Details": location,
                        "Date/Time Posted": posting_date,
                        "Link": link,
                        "YC": "N/A"
                    })
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print("Response content:", response.text)
        
        # Save results to CSV
        pd.DataFrame(jobs_data).to_csv(config.filenames["microsoft"], index=False)
        print(f"Microsoft jobs saved to {config.filenames['microsoft']}")

    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print("Response content:", response.text)

def fetch_amazon_jobs(config):
    url = "https://www.amazon.jobs/api/jobs/search"

    jobs_data = []
    # Headers
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Type": "text/plain;charset=UTF-8",
        "Origin": "https://www.amazon.jobs",
        "Referer": "https://www.amazon.jobs/content/en/career-programs/university/jobs-for-grads?country%5B%5D=US&employment-type%5B%5D=Full+time",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
        "x-api-key": "PbxxNwIlTi4FP5oijKdtk3IrBF5CLd4R4oPHsKNh"
    }

    # Payload (example, adapt as necessary)
    payload = {
        "country": ["US"],
        "employment_type": ["Full time"],
        "facets": ["location"],
        "offset": 0,
        "size": 30,  # Number of jobs to fetch
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        # Process and print job data
        jobs = data.get("searchHits", [])
        for job in jobs:
            
            fields = job.get("fields", {})
            job_title = fields.get("title", "N/A")
            
            location = fields.get("location", "N/A")
            if "us" not in location[0].lower():
                continue
            
            posted = fields.get("createdDate", "N/A")
            posted_timestamp = int(posted[0])  # Convert string to integer

            # Convert Unix timestamp to datetime
            posted_date = datetime.fromtimestamp(posted_timestamp).date()

            today = datetime.now().date()
            seven_days_ago = today - timedelta(days=7)
            if posted_date < seven_days_ago:
                continue
            
            link = fields.get("icimsJobId", "N/A")

            quals = fields.get("basicQualifications", "N/A")

            # Use LLM to check job requirements
            chat_completion = client.chat.completions.create(
                messages=[{
                    "role": "user",
                    "content": f"Check the job posting:{quals}, and tell me if it is meant for: NEW GRADS or requires 2 or fewer years of experience. If it is Full Time and fits job: {get_job_types(config)}, answer Yes, otherwise No."
                }],
                model=config.llm_model,
            )
            
            # Save job if it matches criteria
            if "yes" in chat_completion.choices[0].message.content.strip().lower():
                jobs_data.append({
                    "Company": "Amazon",
                    "Job": job_title[0],
                    "Details": location[0],
                    "Date/Time Posted": convert_to_date(posted[0]),
                    "Link": "https://www.amazon.jobs/en/jobs/" + link[0],
                    "YC": "N/A"
                })
            
        # Save results to CSV
        pd.DataFrame(jobs_data).to_csv(config.filenames["amazon"], index=False)
        print(f"Amazon jobs saved to {config.filenames['amazon']}")

    else:
        print(f"Failed to fetch jobs. Status code: {response.status_code}")
        print(response.text)

def scrape_venture_jobs(config, venture_name, url, payload_override=None):
    """
    Scrape jobs from venture capital job boards.
    
    Args:
        config (JobConfig): Configuration object
        venture_name (str): Name of the venture capital firm
        url (str): API URL for the job board
        payload_override (dict, optional): Additional payload parameters
    """
    jobs_data = []
    headers = get_headers()
    
    # Prepare API request payload
    base_payload = {
        "meta": {"size": 100},
        "board": {
            "id": f"{venture_name.lower()}-{'capital' if venture_name == 'sequoia' else 'ventures' if venture_name == 'nextview' else 'partners' if venture_name == 'greylock' else 'horowitz'}", 
            "isParent": True
        },
        "query": {
            "jobTypes": get_job_types(config),
            "locations": ["United States"],
            "postedSince": "P7D",
            "promoteFeatured": True,
        },
    }

    if payload_override:
        base_payload.update(payload_override)

    # Get job listings
    response = requests.post(url, headers=headers['sequoia'], json=base_payload)
    if response.status_code == 200:
        data = response.json()
        jobs = data["jobs"]
        
        # Handle nested jobs structure for Sequoia
        if venture_name == "sequoia" and "jobs" in jobs[0]:
            jobs = [job for parent_job in jobs for job in parent_job["jobs"]]

        # Process each job
        for job in jobs:
            job_url = job["url"]
            response_job = requests.get(job_url, headers=headers['html'])
            print(response_job.status_code)
            if response_job.status_code == 200:
                # Parse job description
                soup = BeautifulSoup(response_job.content, "html.parser")
                page_text = soup.get_text(separator=" ")
                cleaned_text = " ".join(page_text.split())
                keywords = ["necessary qualifications", "required qualifications", "technical skills", "you have", "about you", "requirement", "qualification", "required"]
                end_phrases = ["ready to apply?", "apply for this job"]
                found_keyword = None

                for keyword in keywords:
                    if keyword in cleaned_text.lower():
                        found_keyword = keyword
                        break

                if found_keyword:
                    # Extract text after the first occurrence of the found keyword
                    cleaned_text = cleaned_text.lower().split(found_keyword, 1)[1].strip()
                    for end_phrase in end_phrases:
                        if end_phrase.lower() in cleaned_text:
                            cleaned_text = cleaned_text.split(end_phrase.lower(), 1)[0].strip()
                            break
                else:
                    continue

                # Use LLM to check job requirements
                chat_completion = client.chat.completions.create(
                    messages=[{
                        "role": "user",
                        "content": f"Check the job posting:{cleaned_text} and tell me if it is meant for: NEW GRADS or requires 2 or fewer years of experience. If it is Full Time and in the USA only, answer Yes, otherwise No."
                    }],
                    model=config.llm_model,
                )
                
                # Save job if it matches criteria
                if "yes" in chat_completion.choices[0].message.content.strip().lower():
                    jobs_data.append({
                        "Company": job.get("companyName", "N/A"),
                        "Job": job.get("title", "N/A"),
                        "Details": job.get("locations", ["N/A"])[0] if isinstance(job.get("locations"), list) else job.get("locations", "N/A"),
                        "Date/Time Posted": convert_to_date(job["timeStamp"]),
                        "Link": job_url,
                        "YC": venture_name.title()
                    })

    # Save results to CSV
    filename = config.filenames[venture_name.lower()]
    pd.DataFrame(jobs_data).to_csv(filename, index=False)
    print(f"{venture_name} jobs saved to {filename}")

def combine_csv_files(config):
    """
    Combine and filter CSV files from different sources.
    Handles different file sets for DS and CS jobs.
    """
    # Select appropriate files based on job type
    files_to_combine = [config.filenames[key] for key in 
                       (["sequoia", "nextview", "greylock", "andreessen", "microsoft", "amazon"] if config.job_type == "DS" 
                        else ["yc", "sequoia", "nextview", "greylock", "andreessen", "microsoft", "amazon"])]
    
    combined_data = pd.DataFrame()

    # Read and combine individual CSV files
    for file in files_to_combine:
        if os.path.exists(file):
            try:
                data = pd.read_csv(file)
                if not data.empty:
                    combined_data = pd.concat([combined_data, data], ignore_index=True)
                else:
                    print(f"Warning: Empty data in file {file}")
            except Exception as e:
                print(f"Error reading file {file}: {str(e)}")
                continue
        else:
            print(f"Warning: File not found: {file}")

    # Process combined data if we have any
    if not combined_data.empty:
        if "Job" in combined_data.columns:
            # Filter out jobs with unwanted titles
            combined_data = combined_data[~combined_data["Job"].str.contains('|'.join(config.exclusion_keywords), case=False, na=False)]

            # Get historical log from S3
            history_df = get_historical_log_from_s3(config.job_type)
            
            # Filter out jobs that exist in history
            if not history_df.empty:
                new_jobs = combined_data[~combined_data["Link"].isin(history_df["Link"])]
            else:
                new_jobs = combined_data
            
            if not new_jobs.empty:
                # Add new jobs to history
                updated_history = pd.concat([
                    history_df,
                    new_jobs[["Company", "Job", "Link"]]
                ], ignore_index=True)
                
                # Update historical log in S3
                update_historical_log_in_s3(updated_history, config.job_type)
                
                # Update combined_data to only include new jobs
                combined_data = new_jobs
                print(f"Found {len(new_jobs)} new jobs")
            else:
                print("No new jobs found - all jobs already in historical log")

        
        # Save filtered results
        combined_data.to_csv(config.filenames["combined"], index=False)
        print(f"Filtered and combined CSV saved to {config.filenames['combined']}")
    else:
        print("Warning: No data to combine. Check individual source files.")

def get_historical_log_from_s3(job_type, bucket_name=""):
    """
    Retrieve historical log from S3. If it doesn't exist, create a new one.
    """
    s3_client = boto3.client('s3')
    history_key = f"{job_type}_historical_jobs.csv"
    
    try:
        # Try to get existing historical log
        response = s3_client.get_object(Bucket=bucket_name, Key=history_key)
        history_df = pd.read_csv(response['Body'])
        print(f"Retrieved historical log from S3 with {len(history_df)} entries")
        return history_df
    except s3_client.exceptions.NoSuchKey:
        # If file doesn't exist, create new DataFrame
        print(f"No existing historical log found for {job_type}, creating new one")
        history_df = pd.DataFrame(columns=["Company", "Job", "Link"])
        return history_df

def update_historical_log_in_s3(history_df, job_type, bucket_name=""):
    """
    Update historical log in S3
    """
    s3_client = boto3.client('s3')
    history_key = f"{job_type}_historical_jobs.csv"
    
    # Convert DataFrame to CSV buffer
    csv_buffer = history_df.to_csv(index=False)
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=history_key,
        Body=csv_buffer.encode()
    )
    print(f"Updated historical log in S3 with {len(history_df)} total entries")

def cleanup_tmp():
    for root, dirs, files in os.walk("/tmp"):
        for file in files:
            os.remove(os.path.join(root, file))
    print("/tmp cleaned up!")

def send_email_with_attachments(config):
    """
    Send email with job scraping results using credentials from secrets.
    
    Args:
        config (JobConfig): Configuration object with email settings
        secrets (dict): Dictionary containing email credentials
    """
    job_type_name = "Data Scientist/Analyst" if config.job_type == "DS" else "Software Engineering"
    subject = f"{job_type_name} Job Scraping Results"
    body = f"""
    Attached are the scraped job listings from YC, Sequoia, NextView, Greylock, Andreessen Horowitz, Amazon, Microsoft.
    Search criteria: {job_type_name.lower()}, 0-2 years experience, USA.
    Keep in mind the LLM can be wrong so double check the list.
    Thanks!
    """

    # Create and configure email message
    em = EmailMessage()
    em['From'] = SECRETS['email_config']['sender']
    em['To'] = ", ".join(config.email_receivers)
    em['Subject'] = subject
    em.set_content(body)

    # Attach the combined CSV file if it exists
    if os.path.exists(config.filenames["combined"]):
        with open(config.filenames["combined"], "rb") as f:
            file_data = f.read()
            em.add_attachment(
                file_data,
                maintype="application",
                subtype="octet-stream",
                filename=os.path.basename(config.filenames["combined"])
            )

    # Send the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SECRETS['email_config']['sender'], SECRETS['email_config']['password'])
        smtp.send_message(em)
    print("Email sent successfully.")

def main(config):
    """
    Main execution function that orchestrates the job scraping process.
    
    Args:
        config (JobConfig): Configuration object containing all settings
    """
    
    # Check if files already exist for today
    files_exist = all(os.path.exists(file) for file in config.filenames.values())
    
    if not files_exist:
        print("Files do not exist. Running scraping functions...")
        # Only scrape YC for CS jobs
        if config.job_type == "CS":
            scrape_yc_jobs(config)
        
        # Scrape venture capital job boards
        scrape_venture_jobs(config, "sequoia", get_urls()['sequoia'], {"grouped": True})
        scrape_venture_jobs(config, "nextview", get_urls()['nextview'])
        scrape_venture_jobs(config, "greylock", get_urls()['greylock'])
        scrape_venture_jobs(config, "andreessen", get_urls()['andreessen'])
        fetch_microsoft_jobs(config)
        fetch_amazon_jobs(config)

        combine_csv_files(config)
    else:
        print("Files already exist. Skipping scraping.")
    
    # Upload results to Google Sheets
    upload_to_google_sheet(config)
    
    # Send email notification with results
    send_email_with_attachments(config)
    cleanup_tmp()

def lambda_handler(event, context):
    """
    AWS Lambda handler function - Entry point for Lambda execution.
    
    Args:
        event (dict): AWS Lambda event object
        context (object): AWS Lambda context object
        
    Returns:
        dict: Response containing execution status and message
    """
    try:
        # Initialize secrets first - this gives us access to all necessary credentials
        
        # Create configurations for each job type
        DS_CONFIG = JobConfig(
            job_type="DS",
            sheet_id="",
            email_receivers=["", "", ""],
            llm_model="llama3-8b-8192",
            exclusion_keywords=["Sr.", "Sr", "Staff", "Principal", "lead", "Senior", "PHD", "intern", "india"]
        )

        CS_CONFIG = JobConfig(
            job_type="CS",
            sheet_id="",
            email_receivers=["", ""],
            llm_model="llama-3.1-8b-instant",
            exclusion_keywords=["Sr.", "Staff", "Principal", "lead", "Senior", "intern", "india"]
        )

        # Execute scraping for both configurations
        print("Starting Data Science job scraping...")
        main(DS_CONFIG)
        
        print("\nStarting Software Engineering job scraping...")
        main(CS_CONFIG)
        
        return {
            'statusCode': 200,
            'body': 'Job scraping completed successfully'
        }
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        import traceback
        print("Detailed error trace:")
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': f'Error during execution: {str(e)}'
        }

# For local testing
if __name__ == "__main__":
    lambda_handler(None, None)
