# from src.yes.payesapi.main import YesEnergyAPI
from src.yes_energy.legacy_connector.support.main import YesEnergyAPI
import pandas as pd
import numpy as np


class LMP_YES:

    def __init__(self,
                 iso,
                 nodes,
                 startdate,
                 enddate,
                 file_path,
                 file_name):

        # Inputs
        self.iso = iso
        self.nodes = [(n[0].upper(), n[1].upper()) for n in nodes]
        self.startdate = startdate
        self.enddate = enddate
        self.file_path = file_path
        self.file_name = file_name

        self.yes_api_user = 'jonathan.roth@paconsulting.com'
        self.yes_api_pass = 'Thinkpad2021!'
        self.file_format = 'json'
        self.dart = ['DA', 'RT']
        self.yes_list = ['LMP', 'CONG', 'LOSS']
        self.yes_list_ERCOT = ['LMP', 'CONG_ESTIMATED']
        self.dart_list = [dart + y for dart in self.dart for y in self.yes_list] if iso != 'ERCOT' else [dart + y for
                                                                                                         dart in
                                                                                                         self.dart for y
                                                                                                         in
                                                                                                         self.yes_list_ERCOT]

        self.timezone = 'PST' if self.iso == 'CAISO' else 'CST' if self.iso in ["ERCOT", "SPP"] else 'EST'
        self.month_dict = {'JANUARY': 1,
                           'FEBRUARY': 2,
                           'MARCH': 3,
                           'APRIL': 4,
                           'MAY': 5,
                           'JUNE': 6,
                           'JULY': 7,
                           'AUGUST': 8,
                           'SEPTEMBER': 9,
                           'OCTOBER': 10,
                           'NOVEMBER': 11,
                           'DECEMBER': 12}

    def pull_data(self):

        yes_input_dict = {'objectids': list(sum(self.nodes, ())),
                          'datatypes': self.dart_list,
                          'paramdict': {'agglevel': 'hour', #,'5min', #'hour',
                                        'timezone': self.timezone,
                                        'startdate': self.startdate,
                                        'enddate': self.enddate}}

        df = YesEnergyAPI(authtokens=(self.yes_api_user, self.yes_api_pass)).multitimeseries(**yes_input_dict, yesthrottle=False)

        df['MONTH'] = df.MONTH.map(self.month_dict)
        df = df.reset_index()
        df['DATETIME'] = df['DATETIME'] + pd.Timedelta(hours=-1)
        df['YEARMONTH'] = df['DATETIME'].dt.strftime('%Y-%m')
        df['DATE'] = df['DATETIME'].dt.strftime('%Y-%m-%d')
        df['TIMEZONE'] = self.timezone
        df['DAY'] = df['DATETIME'].dt.day
        df['PEAK'] = df['PEAKTYPE'].map(lambda x: "ON" if "ON" in x else "OFF")
        df = df.set_index('DATETIME')
        cols = ['DATE', 'TIMEZONE', 'YEARMONTH', 'YEAR', 'MONTH', 'DAY', 'HOURENDING', 'PEAKTYPE', 'PEAK']
        df = df[cols + [c for c in df.columns if c not in cols]]
        df = df.drop(['MARKETDAY'], axis=1)
        if self.iso == "ERCOT":
            df['PEAK'] = df['PEAKTYPE'].map(lambda x: "ON" if "WDPEAK" in x else "OFF")
            for n in self.nodes:
                source, sink = n
                for dart in self.dart:
                    # # Ratios
                    df[source + '-- DA LMP Ratio'] = df[source + ' (DALMP)'] / df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Ratio'] = df[source + ' (DACONG_ESTIMATED)'] / df[
                        sink + ' (DACONG_ESTIMATED)']
                    df[source + '-- RT LMP Ratio'] = df[source + ' (RTLMP)'] / df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Ratio'] = df[source + ' (RTCONG_ESTIMATED)'] / df[
                        sink + ' (RTCONG_ESTIMATED)']
                    # # Basis
                    df[source + '-- DA LMP Basis'] = df[source + ' (DALMP)'] - df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Basis'] = df[source + ' (DACONG_ESTIMATED)'] - df[
                        sink + ' (DACONG_ESTIMATED)']
                    df[source + '-- RT LMP Basis'] = df[source + ' (RTLMP)'] - df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Basis'] = df[source + ' (RTCONG_ESTIMATED)'] - df[
                        sink + ' (RTCONG_ESTIMATED)']
                    df.replace([np.inf, -np.inf], np.nan, inplace=True)

            df = df.set_index('DATETIME')
            return df

        if self.iso == "NYISO":
            for n in self.nodes:
                source, sink = n
                for dart in self.dart:
                    # Components
                    df[source + '-- DA Cong Comp'] = df[source + ' (DALMP)'] - df[source + ' (DALOSS)']
                    df[source + '-- DA Loss Comp'] = df[source + ' (DALMP)'] + df[source + ' (DACONG)']
                    df[source + '-- RT Cong Comp'] = df[source + ' (RTLMP)'] - df[source + ' (RTLOSS)']
                    df[source + '-- RT Loss Comp'] = df[source + ' (RTLMP)'] + df[source + ' (RTCONG)']
                    df[sink + '-- DA Cong Comp'] = df[sink + ' (DALMP)'] - df[sink + ' (DALOSS)']
                    df[sink + '-- DA Loss Comp'] = df[sink + ' (DALMP)'] + df[sink + ' (DACONG)']
                    df[sink + '-- RT Cong Comp'] = df[sink + ' (RTLMP)'] - df[sink + ' (RTLOSS)']
                    df[sink + '-- RT Loss Comp'] = df[sink + ' (RTLMP)'] + df[sink + ' (RTCONG)']
                    # # Ratios
                    df[source + '-- DA LMP Ratio'] = df[source + ' (DALMP)'] / df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Ratio'] = df[source + '-- DA Cong Comp'] / df[sink + '-- DA Cong Comp']
                    df[source + '-- DA Loss Ratio'] = df[source + '-- DA Loss Comp'] / df[sink + '-- DA Loss Comp']
                    df[source + '-- RT LMP Ratio'] = df[source + ' (RTLMP)'] / df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Ratio'] = df[source + '-- RT Cong Comp'] / df[sink + '-- RT Cong Comp']
                    df[source + '-- RT Loss Ratio'] = df[source + '-- RT Loss Comp'] / df[sink + '-- RT Loss Comp']
                    # # Basis
                    df[source + '-- DA LMP Basis'] = df[source + ' (DALMP)'] - df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Basis'] = df[source + '-- DA Cong Comp'] - df[sink + '-- DA Cong Comp']
                    df[source + '-- DA Loss Basis'] = df[source + '-- DA Loss Comp'] - df[sink + '-- DA Loss Comp']
                    df[source + '-- RT LMP Basis'] = df[source + ' (RTLMP)'] - df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Basis'] = df[source + '-- RT Cong Comp'] - df[sink + '-- RT Cong Comp']
                    df[source + '-- RT Loss Basis'] = df[source + '-- RT Loss Comp'] - df[sink + '-- RT Loss Comp']
                    df.replace([np.inf, -np.inf], np.nan, inplace=True)

            df = df.set_index('DATETIME')

            return df
        else:
            for n in self.nodes:
                source, sink = n
                ### Use only when
                source = source.split(':')[0]
                sink = sink.split(':')[0]

                for dart in self.dart:
                    # Components
                    df[source + '-- DA Cong Comp'] = df[source + ' (DALMP)'] - df[source + ' (DALOSS)']
                    df[source + '-- DA Loss Comp'] = df[source + ' (DALMP)'] - df[source + ' (DACONG)']
                    df[source + '-- RT Cong Comp'] = df[source + ' (RTLMP)'] - df[source + ' (RTLOSS)']
                    df[source + '-- RT Loss Comp'] = df[source + ' (RTLMP)'] - df[source + ' (RTCONG)']
                    df[sink + '-- DA Cong Comp'] = df[sink + ' (DALMP)'] - df[sink + ' (DALOSS)']
                    df[sink + '-- DA Loss Comp'] = df[sink + ' (DALMP)'] - df[sink + ' (DACONG)']
                    df[sink + '-- RT Cong Comp'] = df[sink + ' (RTLMP)'] - df[sink + ' (RTLOSS)']
                    df[sink + '-- RT Loss Comp'] = df[sink + ' (RTLMP)'] - df[sink + ' (RTCONG)']
                    # # Ratios
                    df[source + '-- DA LMP Ratio'] = df[source + ' (DALMP)'] / df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Ratio'] = df[source + '-- DA Cong Comp'] / df[sink + '-- DA Cong Comp']
                    df[source + '-- DA Loss Ratio'] = df[source + '-- DA Loss Comp'] / df[sink + '-- DA Loss Comp']
                    df[source + '-- RT LMP Ratio'] = df[source + ' (RTLMP)'] / df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Ratio'] = df[source + '-- RT Cong Comp'] / df[sink + '-- RT Cong Comp']
                    df[source + '-- RT Loss Ratio'] = df[source + '-- RT Loss Comp'] / df[sink + '-- RT Loss Comp']
                    # # Basis
                    df[source + '-- DA LMP Basis'] = df[source + ' (DALMP)'] - df[sink + ' (DALMP)']
                    df[source + '-- DA Cong Basis'] = df[source + '-- DA Cong Comp'] - df[sink + '-- DA Cong Comp']
                    df[source + '-- DA Loss Basis'] = df[source + '-- DA Loss Comp'] - df[sink + '-- DA Loss Comp']
                    df[source + '-- RT LMP Basis'] = df[source + ' (RTLMP)'] - df[sink + ' (RTLMP)']
                    df[source + '-- RT Cong Basis'] = df[source + '-- RT Cong Comp'] - df[sink + '-- RT Cong Comp']
                    df[source + '-- RT Loss Basis'] = df[source + '-- RT Loss Comp'] - df[sink + '-- RT Loss Comp']
                    df.replace([np.inf, -np.inf], np.nan, inplace=True)

            df = df.set_index('DATETIME')

            return df

