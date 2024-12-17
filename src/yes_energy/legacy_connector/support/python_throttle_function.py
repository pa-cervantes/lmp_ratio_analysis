# Throttle function

# Import libraries
import pandas as pd
from io import StringIO
import re
from datetime import datetime
import time
from src.yes_energy.legacy_connector.support.yes_energy_utilities import call_yes_energy_api_csv, format_date_parameter


def fetch_yes_service_respecting_throttles(username, password, url, json=False, verbose=True):
    """ Function for timeseries and five minute constraint endpoints to obviate the row and frequency limit throttles.
        Returns a pandas DataFrame or csv string of API results that can be set to a variable for further analysis.
        NOTE: This will not obviate the concurrency throttle.
        DEPENDENCIES: yes_energy_utilities.py must be in the same directory as this file.
        Function call_yes_energy_api_csv - takes an API call with .csv extension and returns a csv string
                                           with the results or raises an exception if there is an error.
        Function format_date_parameter   - takes a relative or absolute date and formats it to be used to
                                           create a date range over which to iterate API calls
		Args:
			username (str): DSAPI username
			password (str): DSAPI password
			url (str):      DSAPI url to call
			json (boolean): If True, object returned will be a JSON array
						    If False, object returned will be a pandas dataframe 
			verbose (boolean): If True, feedback about the execution will be printed to stdout
		Returns:
			Either pandas dataframe or array of JSON objects, depending on the json arg
    """
    DT_FREQUENCY = 'D'
    SLEEP_SECONDS = 6
    token = (username, password)

    # Convert url to .csv
    url_pattern = r'^(https:\/\/services\.yesenergy\.com\/PS\/rest\/)(.*)(\?)(.*)$'
    ext_pattern = r'(.*)(\.)(.*)'
    no_ext_pattern = r'(.*)'

    url_match = re.search(url_pattern, url, re.I | re.S)

    if url_match:
        ext = url_match.group(2)
        if '.' in ext:
            ext_match = re.search(ext_pattern, ext, re.I | re.S)
            url = re.sub(ext_match.group(3), 'csv', url, flags=re.I)
        else:
            no_ext_match = re.search(no_ext_pattern, ext, re.I | re.S)
            url = re.sub(no_ext_match.group(1), no_ext_match.group(
                1) + '.csv', url, flags=re.I)
    else:
        raise Exception('Invalid URL. Please review your URL and run function again.')

    # Extract start and end dates from url
    url_parts = url.split('?')
    url_base = url_parts[0]
    url_args = url_parts[1].split('&')
    final_args = []
    startdate = None
    enddate = None
    if url_args:
        for a in url_args:
            if 'startdate=' in a.lower():
                startdate = a.split('=')[1]
            elif 'enddate=' in a.lower():
                enddate = a.split('=')[1]
            else:
                final_args.append(a)

    if len(final_args) == 0:
        non_date_args = ''
    else:
        non_date_args = '&'.join(final_args)

    # Format start and end dates
    if not startdate:
        startdate = datetime.strftime(datetime.now(), '%m/%d/%Y')
        if verbose: 
            print(f"No start date provided. Defaulting to {startdate}")
    else:
        startdate = format_date_parameter(token, startdate)
    if not enddate:
        enddate = datetime.strftime(datetime.now(), '%m/%d/%Y')
        if verbose: 
            print(f"No end date provided. Defaulting to {enddate}")
    else:
        enddate = format_date_parameter(token, enddate)

    return_df = pd.DataFrame()
    # Create date range to iterate over
    dt_range = pd.date_range(start=startdate, end=enddate, freq=DT_FREQUENCY)
    if verbose: 
        print(f"Process dates from {startdate} to {enddate}")

    # Iterate over date chunks and append data to the returned dataframe while staying below the frequency and row limit throttles
    first = True
    for dt in dt_range:
        dt_str = datetime.strftime(dt, '%m/%d/%Y')
        # don't sleep before the first call
        if first:
            first = False
        else:
            if verbose: 
                print('pausing...')
            time.sleep(SLEEP_SECONDS)
        if verbose: 
            print('Fetching', dt_str)
        url = f"{url_base}?startdate={dt_str}&enddate={dt_str}"
        if len(non_date_args) > 0:
            url = url + f"&{non_date_args}"
        apiResult = call_yes_energy_api_csv(token, url)
        temp_df = pd.read_csv(StringIO(apiResult))
        return_df = return_df.append(temp_df)
        if verbose: 
            print('Length of dataframe:', str(len(return_df)))

    # If json format specified, return json array of data instead of pandas dataframe
    if json:
        return_df = return_df.to_json(orient='records', date_format='iso')

    return(return_df)


if __name__ == "__main__":

    # set username and password
    uname = 'username'
    pword = 'password'

    # Calling the function and setting the resulting dataframe to a variable
    ye_api_url = 'https://services.yesenergy.com/PS/rest/constraint/fivemin/PJMISO?startdate=01/01/2020&enddate=01/03/2020'
    try:
        df = fetch_yes_service_respecting_throttles(username=uname, password=pword,
                                                    url=ye_api_url, json=False, verbose=True)

        # View the new dataframe
        if not df.empty:
            print('This is the printed df:')
            print(' ')
            print(df.head())
    except Exception as e:
        print(f"API call failed: {e}")
