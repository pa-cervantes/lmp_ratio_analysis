import numpy as np
import pandas as pd
import requests
# from payesapi.python_throttle_function import fetch_yes_service_respecting_throttles as yes_throttle
# from src.yes.payesapi.python_throttle_function import fetch_yes_service_respecting_throttles as yes_throttle
from src.yes_energy.legacy_connector.support.python_throttle_function import fetch_yes_service_respecting_throttles as yes_throttle


class YesEnergyAPI:
    def __init__(self, authtokens):
        """The structure for a Yes Energy api url follows the general structure of (base endpoint) + (file format) + (parameters)."""
        self.authtokens = authtokens
        self.baseURL = "https://services.yesenergy.com/PS/rest"
        self.URL = None
        self.file_format = 'json'
        
    def make_request(self, URL):

            self.URL = URL
            response = requests.get(URL, auth=self.authtokens)
            df = pd.read_json(response.text)
            if df.columns[0] =='error':
                raise Exception(f"{df.iloc[0,0]} URL:{URL}")
            else:
                if 'DATETIME' in df.columns:
                    df.index = pd.to_datetime(df['DATETIME'])
                    df.drop(columns= ['DATETIME'], inplace=True)
                    #df = df.tz_localize("UTC")
                    #df = df.tz_localize("US/Eastern", ambiguous="infer") #.tz_localize('EST') #local time zone and removes daylight savings time shifts.
                return df

    def timeseries(self, objectid, datatype, paramdict):
        """Method for single object requests."""
        URL = f"{self.baseURL}/timeseries/{datatype}/{objectid}.{self.file_format}?" + make_params(paramdict)
        return self.make_request(URL=URL)

    def multitimeseries(self, objectids, datatypes, paramdict, yesthrottle=False):
        """Endpoint for pulling multiple objectids and datatypes with a single request."""
        if yesthrottle:
            URL = f"{self.baseURL}/timeseries/multiple.{self.file_format}" + '?items=' + ','.join([f"{d}:{r}" for r in objectids for d in datatypes]) + f"&{make_params(paramdict)}"
            df = yes_throttle(username=self.authtokens[0], password=self.authtokens[1], url=URL, json=False, verbose=True)
            return df
        else:
            new_dates = check_api_time_limits(paramdict)
            if new_dates is None:
                URL = f"{self.baseURL}/timeseries/multiple.{self.file_format}" + '?items=' + ','.join([f"{d}:{r}" for r in objectids for d in datatypes]) + f"&{make_params(paramdict)}"
                return self.make_request(URL=URL)
            else:
                frames = []
                og_start_date=paramdict['startdate']; og_end_date=paramdict['enddate']
                for i in range(0, len(new_dates)-1): #add parallization/concurrency here to speed large pulls up
                    paramdict.update({'startdate':new_dates[i],'enddate':new_dates[i+1]})
                    URL = f"{self.baseURL}/timeseries/multiple.{self.file_format}" + '?items=' + ','.join([f"{d}:{r}" for r in objectids for d in datatypes]) + f"&{make_params(paramdict)}"
                    frames.append(self.make_request(URL=URL))
                paramdict.update({'startdate':og_start_date,'enddate':og_end_date})
                return pd.concat(frames)#.reset_index().drop_duplicates('DATETIME').set_index('DATETIME')

    def timeseries_objectid_lookup(self,datatype,paramdict):
        URL = f"{self.baseURL}/timeseries/{datatype}.{self.file_format}?" + make_params(paramdict)
        return self.make_request(URL=URL)

def make_params(dict):
    """Constructs parameter portion of API call
    Example Output: ?agglevel={aggTiming}&startdate={startdate}&enddate={enddate}
    """
    if dict.get('agglevel', False) in ['5mins']:
        dict['agglevel'] = '5min' #handle common error that would make api return daily values instead of 5 mins
    # if not dict.get('timezone', False): #if timezone is not passed
    #     dict['timezone'] = 'UTC'
    return '&'.join([f"{key}={value}" for key, value in dict.items()])



def check_api_time_limits(paramdict):
    """Breaks down the startdate and enddate into intervals that can be queried in separate requests to not hit API throttles.
    API Throttles
    * A single request cannot exceed 5 years at hourly aggregation. The implementation uses 364 days.
    * A single request cannot exceed 90 days at 5min/raw aggregation. The implementation uses 89 days.
    """
    if (paramdict.get('startdate', False) is not False) and (paramdict.get('enddate', False) is not False):
        delta = pd.to_datetime(paramdict['enddate']) - pd.to_datetime(paramdict['startdate'])
        if (paramdict['agglevel'].lower() == 'hour') and (delta/np.timedelta64(1,'D') > 5*364):
            dates = pd.date_range(start=paramdict['startdate'], end=paramdict['enddate'], freq=f"{5*364}D").tolist()
            return [d.strftime('%m/%d/%Y %H:%M:%S') for d in dates] + [paramdict['enddate']]
        elif (paramdict['agglevel'].lower() in ['5min','5mins']) and (delta/np.timedelta64(1,'D') > 90):
            dates = pd.date_range(start=paramdict['startdate'], end=paramdict['enddate'], freq="89D").tolist()
            return [d.strftime('%m/%d/%Y %H:%M:%S') for d in dates] + [paramdict['enddate']]
        else:
            return None
    else:
        return None #no changes needed to dates
