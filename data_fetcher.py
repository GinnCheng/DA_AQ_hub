import requests
from bs4 import BeautifulSoup
import pandas as pd
import io


class qld_amns_data_fetcher:
    def __init__(self, years, location):
        self.years = years
        self.location = location

    def fetch(self, print_result=False, output=False):

        # Base URL for constructing full download links
        base_url = 'https://www.data.qld.gov.au'

        # Initialize a list to store DataFrames
        self.df = pd.DataFrame([])

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
                                self.df = pd.concat([self.df, sub_df], ignore_index=True)
                                if print_result == True:
                                    print(f"Stored CSV from {csv_url} in DataFrame.")

                            else:
                                print(f"Failed to fetch CSV from {csv_url}. Status code: {csv_response.status_code}")
                    else:
                        print(f"Failed to retrieve download page. Status code: {download_page_response.status_code}")
            else:
                print(f"Failed to retrieve the main page. Status code: {response.status_code}")
        if output == True:
            return self.df
        else:
            return self

    def format(self, output=False):
        self.df['Datetime'] = pd.to_datetime(self.df['Date'] + ' ' + self.df['Time'], format='%d/%m/%Y %H:%M')
        # self.df['Date'] = pd.to_datetime(self.df['Date'], format='%d/%m/%Y')
        # self.df['Time'] = pd.to_datetime(self.df['Time'], format='%H')
        # self.df['hour'] = self.df['Time'].str.split(':').str[0]
        if output == True:
            return self.df
        else:
            return self

    def wrangle(self, quantity_group=[], output=False):
        # rename all column names to be better called
        ## this is a wrangling process the format the column names and convert all units into the standard ones
        org_names = ['Wind Direction (degTN)', 'Wind Speed (m/s)', 'Wind Sigma Theta (deg)', 'Wind Speed Std Dev (m/s)',
                     'Air Temperature (degC)', 'Relative Humidity (%)', 'Nitrogen Oxide (ppm)',
                     'Nitrogen Dioxide (ppm)',
                     'Nitrogen Oxides (ppm)', 'Sulfur Dioxide (ppm)', 'PM10 (ug/m^3)', 'PM2.5 (ug/m^3)',
                     'Visibility-reducing Particles (Mm^-1)',
                     'Nitric Oxide (ppm)', 'Barometric Pressure (hPa)'] + \
                    ['Ozone (ppm)', 'Benzene (ppb)',
                     'Toluene (ppb)', 'Xylenes (total) (ppb)', 'Formaldehyde (ppb)']

        wgl_names = ['wind_dir', 'wind_spd', 'wind_dir_std', 'wind_spd_std', 'T_air', 'relative_humid', 'NO', 'NO2',
                     'NOx', 'SO2', 'PM10', 'PM2.5',
                     'vis_reducing_particle', 'nitric_oxide', 'pressure', 'O_3', 'Benzene', 'Toluene', 'Xylenes',
                     'Fromaldehyde']

        for i, name in enumerate(org_names):
            try:
                self.df.rename(columns={name: wgl_names[i]}, inplace=True)
            except:
                pass

        ####################################################################################################################################
        ## convert the unit into standard
        # convert the unit for NO from ppm to ug/m3
        try:
            self.df['NO'] = self.df['NO'] * 1248
        except:
            pass
        # convert the unit for NO2 from ppm to ug/m3
        try:
            self.df['NO2'] = self.df['NO2'] * 1880
        except:
            pass
        # convert the unit for NOx from ppm to ug/m3
        try:
            self.df['NOx'] = self.df['NOx'] * 1311.2  # based on the proportion in the air
        except:
            pass
        # SO2
        try:
            self.df['SO2'] = self.df['SO2'] * 2620
        except:
            pass
            # 'nitric_oxide'
        try:
            self.df['nitric_oxide'] = self.df['nitric_oxide'] * 1248
        except:
            pass
            # Ozone
        try:
            self.df['O_3'] = self.df['O_3'] * 214
        except:
            pass
            # Benzene
        try:
            self.df['Benzene'] = self.df['Benzene'] * 3.2
        except:
            pass
            # Toluene
        try:
            self.df['Toluene'] = self.df['Toluene'] * 3.77
        except:
            pass
            # Xylenes
        try:
            self.df['Xylenes'] = self.df['Xylenes'] * 4.34
        except:
            pass
            # 'Fromaldehyde'
        try:
            self.df['Fromaldehyde'] = self.df['Fromaldehyde'] * 1.23
        except:
            pass

            ############################################################################################################################

        # check the columns that need to be included in the wrangled data
        # create a quantity group
        dust_assess_items_grp = ['NO2', 'SO2', 'PM10', 'PM2.5']
        voc_assess_items_grp = ['Benzene', 'Toluene', 'Xylenes']
        wind_rose_items_grp = ['wind_dir', 'wind_spd']

        quantity_dict = {'dust': dust_assess_items_grp, 'voc': voc_assess_items_grp, 'wind': wind_rose_items_grp}

        # select the quantity group that we want to see
        if quantity_group == []:
            pass
        else:
            quantity_interest = ['Datetime', 'Date', 'Time']
            for quantity in quantity_group:
                try:
                    quantity_interest += quantity_dict[quantity]
                except KeyError as e:
                    print(f"Error: The key '{e}' does not exist in the dictionary.\
                            must choose 1-3 selections from 'dust','voc' and 'wind',\
                            or you can leave it empty to keep all columns.")
            quantity_interest = [x for x in quantity_interest if x in self.df.columns]

            self.df = self.df[quantity_interest]

        # drop any NA for the columns that we care about
        self.df.dropna(subset=self.df.select_dtypes(include=['float']).columns)

        # wrangle the data
        self.df['year'] = self.df.Datetime.dt.year
        self.df['month'] = self.df.Datetime.dt.month
        self.df['day'] = self.df.Datetime.dt.day
        self.df['hour'] = self.df.Datetime.dt.hour
        # df = df.sort_values(by=['Datetime'])
        self.df = self.df.set_index('Datetime')
        self.df.sort_index(inplace=True)

        if output == True:
            return self.df
        else:
            return self