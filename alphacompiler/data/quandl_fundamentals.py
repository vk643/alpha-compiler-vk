import datetime
import quandl
import os
from zipline.data.bundles.core import load
import pandas as pd
import numpy as np
from zipline.utils.paths import zipline_root
from logbook import Logger, StderrHandler
from alphacompiler.util import quandl_tools
from alphacompiler.util.sparse_data import SparseDataFactor
import pickle

KERNEL_BUNDLE = 'sharadar-prices'
DATA_FILE = zipline_root() + '/data/SF1.npy'  # the file name to be used when storing this in ~/.zipline/data
FUNDAMENTAL_FIELDS_FILE = zipline_root() + '/data/SF1.pkl'

log = Logger('quandl_fundamentals.py')

class Fundamentals(SparseDataFactor):
    try:
        with open(FUNDAMENTAL_FIELDS_FILE, 'rb') as f:
            outputs = pickle.load(f)
    except:
        outputs = []

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = len(get_tickers_from_bundle(KERNEL_BUNDLE))
        self.data_path = DATA_FILE

def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle"""
    bundle_data = load(bundle_name, os.environ, None)

    # we can request equities or futures separately changing the filters parameter
    all_sids = bundle_data.asset_finder.sids

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)

    return [a.symbol for a in all_assets]

def download_fundamendals_data (bundle,
                                start_date = '2007-01-01',
                                end_date = datetime.datetime.today().strftime('%Y-%m-%d'),
                                tickers = None,
                                dataset = 'SHARADAR/SF1',
                                fields = None,
                                dimensions = None,
                                drop_dimensions = ('MRT', 'MRQ', 'MRY'),
                                data_file = DATA_FILE,
                                ):
    tickers_universe = get_tickers_from_bundle(bundle)
    N = len (tickers_universe)

    tickers = tickers if tickers else tickers_universe

    log.info (f"Downloading data for {len(tickers) if tickers else 'ALL'} tickers")

    header_columns = ['ticker',
                     'dimension',
                     'datekey',
                     'reportperiod',
                     'lastupdated',
                     'calendardate']

    df = quandl.get_table(dataset,
                          calendardate={'gte': start_date, 'lte': end_date},
                          ticker=tickers,
                          qopts={'columns': header_columns + fields} if fields else None,
                          paginate=True)

    df = df.rename(columns={'datekey': 'Date'}).set_index('Date')

    dfs = [None] * N

    fields = [f.upper() for f in df.columns if f not in header_columns]
    dimensions = dimensions if dimensions \
        else [d for d in df['dimension'].unique() if not d in drop_dimensions]

    max_len = -1

    for i, ticker in enumerate (tickers):
        log.info (f"Pre-processing {ticker} ({i+1} / {len (tickers)})...")
        ticker_df = df[df.ticker==ticker]
        ticker_series = []
        for field in fields:
            for dim in dimensions:
                field_dim_series = ticker_df[ticker_df.dimension==dim][field.lower()]
                field_dim_series.name = field + '_' + dim
                ticker_series.append (field_dim_series)

        ticker_processed_df = pd.concat(ticker_series, axis=1)
        max_len = max(max_len, ticker_processed_df.shape[0])
        dfs[tickers_universe.index(ticker)] = ticker_processed_df

    log.info ("Packing data...")
    dtypes = [('date', '<f8')]

    fundamental_fields = [f'{f}_{d}' for f in fields for d in dimensions]

    with open(FUNDAMENTAL_FIELDS_FILE, 'wb') as f:
        pickle.dump(fundamental_fields, f)

    buff = np.full((len(fundamental_fields)+1, N, max_len), np.nan)

    for field in fundamental_fields:
        dtypes.append((field, '<f8'))

    data = np.recarray(shape=(N, max_len), buf=buff, dtype=dtypes)

    for i, df in enumerate(dfs):
        if df is None:
            continue
        else:
            df = pd.DataFrame(df)

        ind_len = df.index.shape[0]
        data.date[i, :ind_len] = df.index
        for field in fundamental_fields:
            data[field][i, :ind_len] = df[field]

    log.info (f"Saving to {data_file}...")
    data.dump(data_file)  # can be read back with np.load()

    log.info ("Done!")
    return data

def download (bundle = KERNEL_BUNDLE,
              start_date = '2007-01-01',
              tickers = None,
              fields = None,
              dimensions = None,
              ):
    """
    this method is a top-level executor of the download
    download volume could be reduced by setting start_date, tickers, fields, dimensions parameters
    with all parameters set as default will need couple of hours to complete the task
    for each field it gets each dimension available - thus returns fields X dimension values
    :param bundle: bundle which to be used to get the universe of tickers, sharadar-prices by default
    :param start_date: first date of the set
    :param tickers: list of tickers, all tickers by default
    :param fields: list of fields, all fields by default
    :param dimensions: list of dimensions, all dimensions by default (skipping MRs)
    """
    quandl_tools.set_api_key()
    data = download_fundamendals_data(bundle = bundle,
                                      start_date = start_date,
                                      tickers = tickers,
                                      fields = fields,
                                      dimensions = dimensions,
                                      )
    return data

def test ():
    start_date = '2015-01-01'

    fields = [
        'netinc',
        'marketcap',
              ]

    dimensions = [
        'ARQ',
                  ]

    tickers = [
        'AAPL',
        'MSFT',
        'PAAS',
              ]

    data = download(tickers = tickers,
                    fields = fields,
                    start_date = start_date,
                    dimensions = dimensions,
                  )
    return data

def download_all ():
    """
    this is the top-level executor of the fundamentals download - just downloads everything since 2007
    you may want to schedule download_all to be executed daily within out-of-market hours
    """
    StderrHandler().push_application()
    data = download()
    return data

if __name__ == '__main__':
    download_all()