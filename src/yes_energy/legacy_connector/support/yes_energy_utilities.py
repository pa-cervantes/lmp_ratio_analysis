import csv
import requests
import re
from datetime import datetime
from json import loads


def call_yes_energy_api_json(auth, url):
    """ Function to take a Yes Energy DataSignals API JSON request and evaluate the results of the call.
		Arguments:
			auth (tuple): DSAPI username and password ('username','password')
			url (str):    DSAPI url to call
		Raises
			Runtimeerror: API status code that is not 200
		Returns
			JSON string with results of the DSAPI call
    """
    if url.lower().find('json') <= 0:
        raise Exception('URL is not a JSON request')
    response = requests.get(url, auth=auth)
    responseCode = response.status_code
    if responseCode == 200:
        responseJSON = loads(response.text)
        if len(responseJSON) > 0:
            firstKey = responseJSON[0].keys()
            if list(firstKey)[0] == 'error':
                errorMessage = responseJSON[0]['error']
                raise Exception(errorMessage)
            else:
                return response.text
        else:
            raise Exception('API returned no data')
    else:
        raise Exception('API returned error code: ' + str(responseCode))


def call_yes_energy_api_csv(auth, url):
    """ Function to take a Yes Energy DataSignals API CSV request and evaluate the results of the call.
		Arguments:
			auth (tuple): DSAPI username and password ('username','password')
			url (str):    DSAPI url to call
		Raises
			Runtimeerror: API status code that is not 200
		Returns
			CSV string with results of the DSAPI call
    """
    if url.lower().find('csv') <= 0:
        raise Exception('URL is not a CSV request')
    response = requests.get(url, auth=auth)
    responseCode = response.status_code
    if responseCode == 200:
        rawCSV = response.text.split('\r\n')
        responseCSV = csv.reader(rawCSV, delimiter=',')
        responseCSV = list(responseCSV)
        if len(responseCSV) > 0:
            firstKey = responseCSV[0][0]
            if firstKey == 'error':
                errorMessage = responseCSV[1][0]
                raise Exception(errorMessage)
            else:
                return response.text
        else:
            raise Exception('API returned no data')
    else:
        raise Exception('API returned error code: ' + str(responseCode))


def format_date_parameter(auth, dateString):
    """ Function to take the date parameters and make a call to the Yes Energy DataSignals API date parsing function to standardize
        date formats and convert relative dates to actual dates.
		Args:
			auth (tuple):     DSAPI username and password ('username','password')
			dateString (str): A date string to try to resolve
        Returns
			String with date in mm/dd/yyyy format (12/31/2019)
    """
    checkDateURL = f"https://services.yesenergy.com/PS/rest/testdate.json?date={dateString}"
    rePattern = "^([a-zA-Z]{3})\s([a-zA-Z]{3})\s(\d{2})\s(\d{2}):(\d{2}):(\d{2})\s([a-zA-Z]{3})\s(\d{4})$"
    dateResponse = call_yes_energy_api_json(auth, checkDateURL)
    raw = loads(dateResponse)
    rawDate = raw[0].get('string')
    parsedDate = re.search(rePattern, rawDate)
    formattedDate = datetime.strptime(f"{parsedDate.group(8)} {parsedDate.group(2)} {parsedDate.group(3)}", "%Y %b %d")
    return datetime.strftime(formattedDate, '%m/%d/%Y')
