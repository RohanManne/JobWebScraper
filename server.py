import requests
import csv
from bs4 import BeautifulSoup
import pandas as pd
from groq import Groq

# Initialize the Groq client
client = Groq(
    api_key="ENTER YOUR KEY",
)

# Headers for web scraping
headers_common = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
}

# Sequoia Capital payload
payload_sequoia = {
    "meta": {"size": 100},
    "board": {"id": "sequoia-capital", "isParent": True},
    "query": {
        "jobTypes": ["software-engineer"],
        "locations": ["United States"],
        "postedSince": "P7D",
        "promoteFeatured": True,
    },
    "grouped": True,
}

def analyze_job_with_groq(job_name, job_details, job_text):
    """Analyze job suitability using Groq API."""
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"Check the job: {job_name}, {', '.join(job_details)}, {job_text} and tell me if it is meant for NEW GRADS or requires 2 or fewer years of experience. If it is Full Time and if it is in the USA only. If it fits all three, answer Yes, otherwise answer No. Your response should be one word: Yes or No.",
            }
        ],
        model="llama3-8b-8192",
    )
    ai_response = chat_completion.choices[0].message.content.strip()
    return "yes" in ai_response.lower()

def scrape_sequoia_jobs():
    """Scrape jobs from Sequoia Capital API."""
    sequoia_url = "https://jobs.sequoiacap.com/api-boards/search-jobs"
    jobs_data = []

    response = requests.post(sequoia_url, headers=headers_common, json=payload_sequoia)

    if response.status_code == 200:
        data = response.json()
        for job_group in data["jobs"]:
            for job in job_group["jobs"]:
                job_url = job['url']
                response2 = requests.get(job_url, headers=headers_common)

                if response2.status_code == 200:
                    soup2 = BeautifulSoup(response2.content, "html.parser")
                    page_text = " ".join(soup2.get_text(separator=" ").split())
                    if analyze_job_with_groq(job['title'], [job['locations'][0]], page_text):
                        jobs_data.append({
                            "Company": job['companyName'],
                            "Job": job['title'],
                            "Details": job['locations'][0],
                            "Link": job['url']
                        })
    else:
        print(f"Failed to fetch Sequoia Capital jobs. Status code: {response.status_code}")

    # Save data to CSV
    csv_file = "SequoiaJobs.csv"
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["Company", "Job", "Details", "Link"])
        writer.writeheader()
        writer.writerows(jobs_data)
    print(f"Sequoia Capital jobs saved to {csv_file}")


def scrape_yc_jobs():
    """Scrape jobs from Y Combinator."""
    yc_url = "https://www.ycombinator.com/jobs"
    jobs_data = []

    response = requests.get(yc_url, headers=headers_common)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        job_posts = soup.find_all("li", class_="my-2 flex h-auto w-full flex-col flex-nowrap rounded border border-[#ccc] bg-beige-lighter px-5 py-3")

        for job in job_posts:
            company_name = job.find("span", class_="block font-bold md:inline")
            company_name_text = company_name.text.strip() if company_name else "N/A"

            job_link = job.find("a", class_="font-semibold text-linkColor")
            job_name = job_link.text.strip() if job_link else "N/A"
            job_href = job_link.get("href") if job_link else None

            additional_details = []
            details_container = job.find("div", class_="flex flex-row flex-wrap justify-center md:justify-start")
            if details_container:
                detail_items = details_container.find_all("div")
                additional_details = [item.text.strip() for item in detail_items]

            experience_text = "N/A"
            if job_href:
                job_url = f"https://www.ycombinator.com{job_href}"
                response2 = requests.get(job_url, headers=headers_common)

                if response2.status_code == 200:
                    job_soup = BeautifulSoup(response2.text, "html.parser")
                    experience_div = job_soup.find("div", string="Experience")
                    if experience_div:
                        parent_div = experience_div.parent
                        span = parent_div.find("span")
                        experience_text = span.text.strip() if span else "N/A"

                    if analyze_job_with_groq(job_name, additional_details, experience_text):
                        jobs_data.append({
                            "Company": company_name_text,
                            "Job": job_name,
                            "Details": ", ".join(additional_details),
                            "Experience": experience_text,
                            "Link": job_url
                        })
    else:
        print(f"Failed to fetch Y Combinator jobs. Status code: {response.status_code}")

    # Save data to CSV
    csv_file = "YCJobs.csv"
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["Company", "Job", "Details", "Experience", "Link"])
        writer.writeheader()
        writer.writerows(jobs_data)
    print(f"Y Combinator jobs saved to {csv_file}")


if __name__ == "__main__":
    scrape_sequoia_jobs()
    scrape_yc_jobs()
