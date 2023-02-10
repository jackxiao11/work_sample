import sys
sys.path.append('..')
from pathlib import Path
sys.path.insert(0, Path(__file__).parent)
import datetime as dt
import settings
import pandas as pd
from utilities_cls import LogDecorator, timeit, Utilities
from multiprocessing import Process, Manager
import numpy as np
import math
import json
import os
import yfinance as yf
from pandas_datareader import data


class Portfolio:

    def __init__(self, *args):
        self.symbols = []
        self.symbols = list(args)
        self.mp_year_return = []

    @LogDecorator(LOGGING_PROGRAM)
    def load_stock_data(self, symbols, start, end):

        df = pd.DataFrame()
        for symbol in symbols[:]:

            try:
                df_here = Stock.get_stock_data_web(symbol, 'yahoo2', start, end, 'compact')
                if df_here is not None:
                    df_here.insert(loc=0, column='symbol', value=symbol)
                    df = df.append(df_here)
                else:
                    continue
            except:
                continue

#        print(df.head())
        self.mp_stock_quote.append(df)

    @timeit
    @LogDecorator(LOGGING_PROGRAM)
    def get_stock_quote_last_multiprocessing(self):
        # import utilities
        """get portfolio stock last quote using multiprocessing
        need to run after market open to get data back
        """

        df = pd.DataFrame()
        start = Stock.get_last_trading_date()
        end = start

        with Manager() as manager:
            self.mp_stock_quote = manager.list()  # <-- can be shared between processes.
            procs = []
            # instantiating process with arguments
            for x in Utilities.iterator_slice(self.symbols[:], 25):
                # print(name)
                proc = Process(target=self.load_stock_data, args=(x, start, end))
                procs.append(proc)
                proc.start()

            # complete the processes
            for proc in procs:
                proc.join()

#            print(list(self.mp_stock_quote))
            df = pd.concat(list(self.mp_stock_quote))  # , ignore_index=True

        return df
