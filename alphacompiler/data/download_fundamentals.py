import datetime
import quandl
import os
from zipline.data.bundles.core import load
import pandas as pd
import numpy as np
from zipline.utils.paths import zipline_root
from logbook import Logger, StderrHandler
from alphacompiler.util import quandl_tools

log = Logger('Download Quandl Fundamentals')
StderrHandler().push_application()

quandl_tools.set_api_key()

ZIPLINE_DATA_DIR = zipline_root() + '/data/'
FN = "SF1.npy"  # the file name to be used when storing this in ~/.zipline/data

def get_tickers_from_bundle(bundle_name, filters=None):
    """Gets a list of tickers from a given bundle"""
    bundle_data = load(bundle_name, os.environ, None)

    # we can request equities or futures separately changing the filters parameter
    if filters:
        all_sids = tuple()
        if 'equities' in filters:
            all_sids += bundle_data.asset_finder.equities_sids
        if 'futures' in filters:
            all_sids += bundle_data.asset_finder.futures_sids
    else:
        all_sids = bundle_data.asset_finder.sids

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)
    # return only tickers
    return [a.symbol for a in all_assets]

def download_fundamendals_data (tickers = None,
                                start_date = '2007-01-01',
                                end_date = datetime.datetime.today().strftime('%Y-%m-%d'),
                                dataset = 'SHARADAR/SF1',
                                fields = None,
                                dimensions = None,
                                drop_dimensions = ('MRT', 'MRQ', 'MRY'),
                                data_file = ZIPLINE_DATA_DIR + FN,
                                ):

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

    dfs = []

    fields = [f for f in df.columns if f not in header_columns]
    dimensions = dimensions if dimensions \
        else [d for d in df['dimension'].unique() if not d in drop_dimensions]

    max_len = -1

    for i, ticker in enumerate (tickers):
        log.info (f"Pre-processing {ticker} ({i+1} / {len (tickers)})...")
        ticker_df = df[df.ticker==ticker]
        ticker_series = []
        for field in fields:
            for dim in dimensions:
                field_dim_series = ticker_df[ticker_df.dimension==dim][field]
                field_dim_series.name = field + '_' + dim
                ticker_series.append (field_dim_series)

        ticker_processed_df = pd.concat(ticker_series, axis=1)
        max_len = max(max_len, ticker_processed_df.shape[0])
        dfs.append(ticker_processed_df)

    N = len (dfs)

    log.info ("Packing data...")
    buff = np.full((len(fields) + 1, N, max_len), np.nan)

    dtypes = [('date', '<f8')]
    fundamental_fields = ['{}_{}'.format(i, j) for i, j in zip(fields, dimensions)]
    for field in fundamental_fields:
        dtypes.append((field, '<f8'))

    data = np.recarray(shape=(N, max_len), buf=buff, dtype=dtypes)

    for i, df in enumerate(dfs):
        ind_len = df.index.shape[0]
        data.date[i, :ind_len] = df.index
        for field in fundamental_fields:
            data[field][i, :ind_len] = df[field]

    log.info (f"Saving to {data_file}...")
    data.dump(data_file)  # can be read back with np.load()

    log.info ("Done!")
    return data

def test (bundle = 'sharadar-prices'):
    start_date = '2015-01-01'

    fields = [
        'netinc',
        'marketcap',
              ]

    dimensions = [
        'ARQ',
                  ]

    # tickers = [
    #     'AAPL',
    #     'PAAS',
    #           ]

    all_assets = get_tickers_from_bundle(bundle,
                                         filters=None)
    tickers = all_assets[:20]

    fields = None
    dimensions = None

    data = download_fundamendals_data(tickers = tickers,
                                      fields = fields,
                                      start_date = start_date,
                                      dimensions = dimensions,
                                      )

def download_all (bundle = 'sharadar-prices'):
    all_assets = get_tickers_from_bundle(bundle,
                                         filters=None,
                                         )

    fields = None
    dimensions = None

    data = download_fundamendals_data(tickers = all_assets,
                                      fields = fields,
                                      dimensions = dimensions,
                                      )
if __name__ == '__main__':
    test('sharadar-prices')
    #download_all('sharadar-prices')