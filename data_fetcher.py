import requests
from bs4 import BeautifulSoup
import pandas as pd
import io


class qld_amns_data_fetcher:
    def __init__(self, years, location):
        self.years = years
        self.location = location

    def fetch(self):

        # Base URL for constructing full download links
        base_url = 'https://www.data.qld.gov.au'

        # Initialize a list to store DataFrames
        df = pd.DataFrame([])

        for year in self.years:

            # Step 1: Fetch the main page
            main_url = 'https://www.data.qld.gov.au/dataset/air-quality-monitoring-' + str(
                year)  # Replace with the actual URL
            response = requests.get(main_url)

            # Step 2: Check if the request was successful
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Step 3: Find all links to download pages
                download_links = []
                for link in soup.find_all('a', href=True):
                    # Check if the link text contains the specified keywords
                    if self.location in link.text:
                        # Construct the full URL for the download page
                        full_link = base_url + link['href'] if link['href'].startswith('/') else link['href']
                        download_links.append(full_link)

                # Step 4: Follow each link to find the CSV download link
                for download_link in download_links:
                    # Fetch the download page
                    download_page_response = requests.get(download_link)
                    if download_page_response.status_code == 200:
                        download_page_soup = BeautifulSoup(download_page_response.content, 'html.parser')

                        # Find the actual CSV link
                        csv_link = download_page_soup.find('a', href=True,
                                                           class_='btn btn-primary resource-url-analytics resource-type-None resource-btn')
                        if csv_link:
                            # Construct the full CSV URL
                            csv_url = csv_link['href']

                            # Step 5: Fetch the CSV content and store it in a DataFrame
                            csv_response = requests.get(csv_url)
                            if csv_response.status_code == 200:
                                # Use StringIO to read the CSV content into a DataFrame
                                sub_df = pd.read_csv(io.StringIO(csv_response.text))
                                df = pd.concat([df, sub_df], ignore_index=True)
                                print(f"Stored CSV from {csv_url} in DataFrame.")

                            else:
                                print(f"Failed to fetch CSV from {csv_url}. Status code: {csv_response.status_code}")
                    else:
                        print(f"Failed to retrieve download page. Status code: {download_page_response.status_code}")
            else:
                print(f"Failed to retrieve the main page. Status code: {response.status_code}")

        return df