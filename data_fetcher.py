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

    def average(self, period='year'):
        if period == '24hr':
            df_avg = self.df.groupby(['year', 'month', 'day'])[self.df.select_dtypes(include='float').columns].mean().reset_index()
            df_avg_yr100 = df_avg.groupby(['year']).max().reset_index()
            df_avg_yr90 = df_avg.groupby(['year']).quantile(0.90).reset_index()

            # Rename columns of df2 with suffix '_90'
            df_avg_yr90 = df_avg_yr90[self.df.select_dtypes(include='float').columns].rename(columns=lambda x: f"{x}_90")

            # Merge DataFrames side by side
            df_avg = pd.concat([df_avg_yr100, df_avg_yr90], axis=1)

            # Interleave the columns
            # Create the interleaved column order
            cols = []
            for col in df_avg_yr100.columns:
                cols.append(col)
                cols.append(f"{col}_90")
            cols = [col for col in cols if (col in df_avg_yr100.columns or col in df_avg_yr90.columns)]
            # Reorder the merged DataFrame with the new column order
            df_avg = df_avg[cols]

            return df_avg.round(2)

        elif period == '1hr':
            df_avg_yr100 = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].max().reset_index()
            df_avg_yr90 = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].quantile(0.90).reset_index()

            # Rename columns of df2 with suffix '_90'
            df_avg_yr90 = df_avg_yr90[self.df.select_dtypes(include='float').columns].rename(columns=lambda x: f"{x}_90")

            # Merge DataFrames side by side
            df_avg = pd.concat([df_avg_yr100, df_avg_yr90], axis=1)

            # Interleave the columns
            # Create the interleaved column order
            cols = []
            for col in df_avg_yr100.columns:
                cols.append(col)
                cols.append(f"{col}_90")
            cols = [col for col in cols if (col in df_avg_yr100.columns or col in df_avg_yr90.columns)]
            # Reorder the merged DataFrame with the new column order
            df_avg = df_avg[cols]

            return df_avg.round(2)

        elif period == 'year':

            df_avg = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].mean().reset_index()
            return df_avg.round(2)

        else:
            print("period should be one of ('year','24hr','1hr')")
            return None


import requests
from bs4 import BeautifulSoup
import pandas as pd
import io


class nsw_amns_data_fetcher:
    def __init__(self, years, data_dir, aqms_name):
        self.years = years
        self.data_dir = data_dir
        self.aqms_name = aqms_name
        self.df = pd.DataFrame()
        ######### this part need to be upgraded in the future to web scrapping
        for year in years[:-1]:
            temp_data_source = data_dir + aqms_name + '_' + str(year) + '-' + str(year + 1) + '.xls'
            temp_data = pd.read_excel(temp_data_source, sheet_name='worksheet1', header=2)
            self.df = pd.concat([self.df, temp_data], axis=0, join='outer')
        self.df.drop_duplicates(inplace=True)
        # format time
        self.df['Time'] = self.df['Time'].replace('24:00', '00:00')

    # def fetch(self, print_result=False, output=False):

    #     if output == True:
    #         return self.df
    #     else:
    #         return self

    def wrangle(self, quantity_group=[], output=False):
        # rename all column names to be better called
        ## this is a wrangling process the format the column names and convert all units into the standard ones
        ## ALBION PARK SOUTH WDR 1h average [°]
        ## ALBION PARK SOUTH TEMP 1h average [°C]
        ## ALBION PARK SOUTH WSP 1h average [m/s]
        ## ALBION PARK SOUTH NO2 1h average [pphm]
        ## ALBION PARK SOUTH PM10 1h average [µg/m³]
        ## ALBION PARK SOUTH PM2.5 1h average [µg/m³]
        ## ALBION PARK SOUTH HUMID 1h average [%]
        ## ALBION PARK SOUTH RAIN 1h average [mm/m²]

        # Dictionary for replacements
        replacements = {
            'WDR': 'wdr',
            'WSP': 'wsp',
            'TEMP': 'temp',
            'NO2': 'NO2',
            'PM10': 'PM10',
            'PM2.5': 'PM2.5',
            'HUMID': 'hum',
            'RAIN': 'rain'
        }

        # Replace column names
        new_col_names = []
        for col in self.df.columns:
            new_col = col  # Start with the original column name
            for old, new in replacements.items():
                if old in col:
                    new_col = new  # Update to the new name
                    break  # Exit the loop once a match is found
            new_col_names.append(new_col)  # Add the new name to the list

        # Assign the new column names back to the DataFrame
        self.df.columns = new_col_names
        ####################################################################################################################################
        ## convert the unit into standard
        # convert the unit for WSP from m/s to km/h
        try:
            self.df['wsp'] = self.df['wsp'] * 3.6
        except:
            pass
        # convert the unit for NO from ppm to ug/m3
        try:
            self.df['NO'] = self.df['NO'] * 1248
        except:
            pass
        # convert the unit for NO2 from pphm to ug/m3
        try:
            self.df['NO2'] = self.df['NO2'] * 1
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
        wind_rose_items_grp = ['wdr', 'wsp']

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

        # Function to determine the quarter based on the month
        def get_season(month):
            if month in [3, 4, 5]:
                return 'autumn'
            elif month in [6, 7, 8]:
                return 'summer'
            elif month in [9, 10, 11]:
                return 'spring'
            elif month in [12, 1, 2]:
                return 'winter'

        # wrangle the data
        self.df['Datetime'] = pd.to_datetime(self.df['Date'] + ' ' + self.df['Time'], format='%d/%m/%Y %H:%M')
        self.df.loc[self.df['Time'] == '00:00', 'Datetime'] += pd.Timedelta(days=1)
        self.df.drop(columns=['Date','Time'], inplace=True)
        self.df['year'] = self.df.Datetime.dt.year
        self.df['month'] = self.df.Datetime.dt.month
        self.df['day'] = self.df.Datetime.dt.day
        self.df['hour'] = self.df.Datetime.dt.hour
        # Apply the function to create a new column for quarters
        self.df['season'] = self.df['month'].apply(get_season)
        # df = df.sort_values(by=['Datetime'])
        self.df = self.df.set_index('Datetime')
        self.df.sort_index(inplace=True)

        if output == True:
            return self.df
        else:
            return self

    def average(self, period='year'):
        if period == '24hr':
            df_avg = self.df.groupby(['year', 'month', 'day'])[
                self.df.select_dtypes(include='float').columns].mean().reset_index()
            df_avg_yr100 = df_avg.groupby(['year']).max().reset_index()
            df_avg_yr90 = df_avg.groupby(['year']).quantile(0.90).reset_index()

            # Rename columns of df2 with suffix '_90'
            df_avg_yr90 = df_avg_yr90[self.df.select_dtypes(include='float').columns].rename(
                columns=lambda x: f"{x}_90")

            # Merge DataFrames side by side
            df_avg = pd.concat([df_avg_yr100, df_avg_yr90], axis=1)

            # Interleave the columns
            # Create the interleaved column order
            cols = []
            for col in df_avg_yr100.columns:
                cols.append(col)
                cols.append(f"{col}_90")
            cols = [col for col in cols if (col in df_avg_yr100.columns or col in df_avg_yr90.columns)]
            # Reorder the merged DataFrame with the new column order
            df_avg = df_avg[cols]

            return df_avg.round(2)

        elif period == '1hr':
            df_avg_yr100 = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].max().reset_index()
            df_avg_yr90 = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].quantile(
                0.90).reset_index()

            # Rename columns of df2 with suffix '_90'
            df_avg_yr90 = df_avg_yr90[self.df.select_dtypes(include='float').columns].rename(
                columns=lambda x: f"{x}_90")

            # Merge DataFrames side by side
            df_avg = pd.concat([df_avg_yr100, df_avg_yr90], axis=1)

            # Interleave the columns
            # Create the interleaved column order
            cols = []
            for col in df_avg_yr100.columns:
                cols.append(col)
                cols.append(f"{col}_90")
            cols = [col for col in cols if (col in df_avg_yr100.columns or col in df_avg_yr90.columns)]
            # Reorder the merged DataFrame with the new column order
            df_avg = df_avg[cols]

            return df_avg.round(2)

        elif period == 'year':

            df_avg = self.df.groupby(['year'])[self.df.select_dtypes(include='float').columns].mean().reset_index()
            return df_avg.round(2)

        else:
            print("period should be one of ('year','24hr','1hr')")
            return None

    def climate(self, select_years = [2019,2020,2021,2022,2023]):

        # Function to color the max and min values
        def highlight_max_min(s):
            is_max = s == s.max()
            is_min = s == s.min()
            return ['color: red' if v else 'color: blue' if m else '' for v, m in
                    zip(is_max, is_min)]

        month_name_dict = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr',
                           5:'May', 6:'Jun', 7:'Jul', 8:'Aug',
                           9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec', 0:'annual'}

        df_temp = self.df[['temp', 'year', 'month', 'day', 'hour', 'season']]
        df_rain = self.df[['rain', 'year', 'month', 'day', 'hour', 'season']]

        df_temp = df_temp[df_temp.year.isin(select_years)]
        df_rain = df_rain[df_rain.year.isin(select_years)]

        df_temp.dropna(axis=0)
        df_rain.dropna(axis=0)

        stats_max_temp_month = df_temp.groupby(by=['year', 'month','day']).temp.max().reset_index().groupby(by='month').mean().T.round(2)
        stats_max_temp_year = df_temp.groupby(by=['year','month','day']).temp.max().reset_index().groupby(by='month').mean().mean().T.round(2)
        stats_max_temp = pd.concat([stats_max_temp_month,stats_max_temp_year],axis=1)
        stats_max_temp = stats_max_temp.T.drop(columns=['year','day'])
        stats_max_temp = stats_max_temp.T
        stats_max_temp.rename(columns=month_name_dict, inplace=True)

        stats_min_temp_month = df_temp.groupby(by=['year', 'month','day']).temp.min().reset_index().groupby(by='month').mean().T.round(2)
        stats_min_temp_year = df_temp.groupby(by=['year', 'month','day']).temp.min().reset_index().groupby(by='month').mean().mean().T.round(2)
        stats_min_temp = pd.concat([stats_min_temp_month, stats_min_temp_year], axis=1)
        stats_min_temp = stats_min_temp.T.drop(columns=['year','day'])
        stats_min_temp = stats_min_temp.T
        stats_min_temp.rename(columns=month_name_dict, inplace=True)

        stats_rain_month = df_rain.groupby(by=['year', 'month']).rain.sum().reset_index().groupby(by='month').mean().T.round(2)
        stats_rain_year = df_rain.groupby(by=['year']).rain.sum().reset_index().mean().T.round(2)
        stats_rain = pd.concat([stats_rain_month, stats_rain_year], axis=1)
        stats_rain = stats_rain.T.drop(columns=['year'])
        stats_rain = stats_rain.T
        stats_rain.rename(columns=month_name_dict, inplace=True)

        # Apply the function to the DataFrame
        stats_max_temp = stats_max_temp.style.apply(highlight_max_min, axis=1,subset=stats_max_temp.columns.difference(['annual'])).format(precision=2)
        stats_min_temp = stats_min_temp.style.apply(highlight_max_min, subset=stats_min_temp.columns.difference(['annual']), axis=1).format(precision=2)
        stats_rain = stats_rain.style.apply(highlight_max_min, axis=1, subset=stats_rain.columns.difference(['annual'])).format(precision=2)

        return stats_max_temp, stats_min_temp, stats_rain

        # df_temp.groupby(by=['year', 'month']).temp.max().reset_index().groupby(by='month').mean().T
