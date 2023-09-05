#%%
import warnings
warnings.filterwarnings("ignore")
def action_with_warnings():
    warnings.warn("should not appear")
with warnings.catch_warnings(record=True):
    action_with_warnings()
import os
import requests
import json
import concurrent.futures
import pandas             as pd
import numpy              as np
import datetime           as dt
import streamlit          as st
import matplotlib.pyplot  as plt
import seaborn            as sns


#%%


#%%


#%%
# Helper functions

def fetch_history(symbol, interval, start_dt, end_dt):
    url = 'https://api.binance.com/api/v3/klines'
    df = pd.DataFrame(json.loads(requests.get(url, params={
        'symbol'    : symbol.upper(),
        'interval'  : interval,
        'startTime' : str(int(dt.datetime.strptime(start_dt, "%Y-%m-%d").timestamp()*1000)),
        'endTime'   : str(int(dt.datetime.strptime(end_dt  , "%Y-%m-%d").timestamp()*1000))
    }).text))
    df.columns = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume','close_time', 'qav', 'num_trades','taker_base_vol', 'taker_quote_vol', 'ignore']
    df = df.astype({'timestamp':'datetime64[ms]', 'Open':float, 'High':float, 'Low':float, 'Close':float, 'Volume':float})
    df = df.set_index('timestamp')
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]

def fetch_latest(symbol, interval, limit=1000):
    url = 'https://api.binance.com/api/v3/klines'
    df = pd.DataFrame(json.loads(requests.get(url, params={
        'symbol'  : symbol.upper(),
        'interval': interval,
        'limit'   : limit
        }).text))
    df.columns = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume','close_time', 'qav', 'num_trades','taker_base_vol', 'taker_quote_vol', 'ignore']
    df = df.astype({'timestamp':'datetime64[ms]', 'Open':float, 'High':float, 'Low':float, 'Close':float, 'Volume':float})
    df = df.set_index('timestamp')
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]

def fetch_all(symbol, interval):
    csv_file = f"./data/crypto_history/{symbol.lower()}-{interval}.csv"
    df = None
    if not os.path.exists(csv_file):
        print(f"caching {symbol.lower()}")
        today = dt.date.today()

        before_01 = str(today - dt.timedelta(days=5))
        before_00 = str(today - dt.timedelta(days=50))

        before_11 = str(today - dt.timedelta(days=45))
        before_10 = str(today - dt.timedelta(days=90))

        before_21 = str(today - dt.timedelta(days=85))
        before_20 = str(today - dt.timedelta(days=130))

        before_31 = str(today - dt.timedelta(days=125))
        before_30 = str(today - dt.timedelta(days=170))

        before_41 = str(today - dt.timedelta(days=165))
        before_40 = str(today - dt.timedelta(days=210))

        before_51 = str(today - dt.timedelta(days=205))
        before_50 = str(today - dt.timedelta(days=250))

        before_61 = str(today - dt.timedelta(days=245))
        before_60 = str(today - dt.timedelta(days=290))

        df0 = fetch_history(symbol=symbol, interval=interval, start_dt=before_00, end_dt=before_01)
        df1 = fetch_history(symbol=symbol, interval=interval, start_dt=before_10, end_dt=before_11)
        df2 = fetch_history(symbol=symbol, interval=interval, start_dt=before_20, end_dt=before_21)
        df3 = fetch_history(symbol=symbol, interval=interval, start_dt=before_30, end_dt=before_31)
        df4 = fetch_history(symbol=symbol, interval=interval, start_dt=before_40, end_dt=before_41)
        df5 = fetch_history(symbol=symbol, interval=interval, start_dt=before_50, end_dt=before_51)
        df6 = fetch_history(symbol=symbol, interval=interval, start_dt=before_60, end_dt=before_61)

        df = pd.concat([df0, df1, df2, df3, df4, df5, df6])
        df = df[~df.index.duplicated(keep='last')]
        df = df.dropna()
    else:
        df = pd.read_csv(csv_file, parse_dates=True, index_col=0)

    df_  = fetch_latest(symbol=symbol, interval=interval)

    df = pd.concat([df, df_])
    df = df[~df.index.duplicated(keep='last')]
    df = df.dropna()
    df = df.sort_index()
    df = df.iloc[-10000:]
    df.to_csv(csv_file, index=True, header=True)

    return df



#%%


#%%
def load_asset(params):
    asset       = params[0]
    buffer_days = 30

    try:
        df = fetch_all(symbol=asset, interval='30m')
        df = df[df.index[-1]-pd.Timedelta(days=buffer_days):]
        df = df[:-1] # only officially closed bars should be calculated
        df['ema'] = df['Close'].ewm(span=3, adjust=False).mean()
        return (asset, df['ema'])
    except Exception as e:
        print(f"exception at loading {asset} : ")
        print(e)
        return None
    return None


#%%


#%%
def main():

    os.makedirs("./data/crypto_history/", exist_ok=True)
    asset_list = []
    with open("./data/crypto_asset_list.txt") as f:
        asset_list= f.read().strip().split()
    #asset_list = asset_list[:5]

    results = None

    with st.spinner("Loading all crypto history, please wait..."):
        concurrency_count = 16
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_count) as executor:
            params  = [(asset,) for asset in asset_list]
            results = [r for r in executor.map(load_asset, params) if r]

    st.header('Correlation with BNB', divider='rainbow')

    all_df = None
    if results is not None:
        all_df = pd.DataFrame(index=results[0][1].index)
        for item in results:
            if item is not None:
                all_df[item[0]] = item[1]
    all_df.dropna(inplace=True)
    asset_list = list(all_df.columns)


    col01, col02, col03 = st.columns(3)
    with col01:
        heatmap_window = st.number_input("Heatmap plotting window", value=5000)
    with col02:
        hours = st.number_input('Correlation by hours', value=120)
    with col03:
        show_datetime = st.checkbox('Show datetime', value=False)
        days  = round(hours/24.0, 2)
        st.text(f"{days} days")

    corr_window = 2*hours;
    corr_df     = pd.DataFrame(index=all_df.index)
    for asset in list([item for item in asset_list if not item=="bnbusdt"]):
        corr_df[asset] = all_df[asset].rolling(corr_window).corr(all_df['bnbusdt'])

    #st.write(f"### BNBUSDT correlation with {hours} hours.")
    corr_df.dropna(inplace=True)


    heatmap_fig = plt.figure(figsize=(28, 22))
    sns.heatmap(corr_df.iloc[-heatmap_window:], yticklabels=show_datetime)
    st.pyplot(heatmap_fig)

    asset_names               = list(corr_df.columns)
    latest_correlation_values = list(corr_df.iloc[-1])

    col11, col12 = st.columns(2)
    with col11:
        st.write("### Most correlated to BNB")
        most_correlation_threshold  = st.number_input(label="Most correlation threshold", value=0.9, step=0.01, min_value=0.0, max_value=1.0)
    with col12:
        st.write("### Least correlated to BNB")
        least_correlation_threshold = st.number_input(label="Least correlation upper threshold", value=0.5, step=0.01, min_value=0.0, max_value=1.0)

    filterable_correlations_df  = pd.DataFrame(index=asset_names)
    filterable_correlations_df['correlation'] = latest_correlation_values

    most_correlated_df     = filterable_correlations_df[filterable_correlations_df['correlation']>=most_correlation_threshold]
    least_correlated_df    = filterable_correlations_df[filterable_correlations_df['correlation']<least_correlation_threshold]

    most_correlated_df.sort_values (by="correlation", ascending=False, inplace=True)
    least_correlated_df.sort_values(by="correlation", ascending=True , inplace=True)

    most_correlated_names  = str(" ".join(list(most_correlated_df.index ))).strip()
    least_correlated_names = str(" ".join(list(least_correlated_df.index))).strip()

    col21, col22 = st.columns(2)
    with col21:
        st.write(f"""{most_correlated_names}""")
    with col22:
        st.write(f"""{least_correlated_names}""")
    
    col31, col32 = st.columns(2)
    with col31:
        st.dataframe(most_correlated_df.sort_values(by="correlation", ascending=False))
    with col32:
        st.dataframe(least_correlated_df.sort_values(by="correlation", ascending=True))



    pass

#%%


#%%
if __name__ == '__main__':
    main()


#%%


