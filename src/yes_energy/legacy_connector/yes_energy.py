import requests
import pandas as pd
from io import StringIO


class ConnectorYESEnergy:
    def __init__(self,
                 ISO: str == None,
                 node: str == None,
                 datestart: str,
                 dateend: str,
                 to_tz: str = 'UTC'):

        self.ISO = ISO
        self.node = node
        self.datestart = datestart
        self.dateend = dateend
        self.to_tz = to_tz
        self.url_base = 'https://services.yesenergy.com/PS/rest/timeseries/'
        self.keys = ('jonathan.roth@paconsulting.com', 'Thinkpad2021!')

    def run(self, mkt):

        if len(mkt) > 1:
            url = self.build_url_all(mkt=mkt, agg='raw')
            response = requests.get(url, auth=self.keys)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                x = 1
                # return df
            else:
                print(f"Failed to retrieve data: {response.status_code}")
                return None


        else:
            url = self.build_url_one(node=self.node, mkt=mkt)  # Build the URL for multiple markets
            response = requests.get(url, auth=self.keys)  # Send request
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))  # Process the response
                x = 1
                # return df
            else:
                print(f"Failed to retrieve data: {response.status_code}")
                return None


    def build_url_one(self, mkt, agg):
        url = self.url_base + mkt + '/' + self.node + '.csv?agglevel=' + agg + '&startdate=' + self.datestart + '&enddate=' + self.dateend

        return url

    def build_url_all(self, mkt, agg):
        items = []

        for i in mkt:
            items.append(f'{i}:{self.node}')
        items_str = ','.join(items)

        url = f"{self.url_base}multiple.csv?items={items_str}&stat=MAX" + '&agglevel=' + agg + '&startdate=' + self.datestart + '&enddate=' + self.dateend

        return url

