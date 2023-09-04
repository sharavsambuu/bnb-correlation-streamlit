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
    df  = fetch_latest(symbol=symbol, interval=interval)
    df = df[~df.index.duplicated(keep='last')]
    df = df.dropna()
    df = df.sort_index()

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
        hours = st.number_input('Correlation by hours', value=168)
    with col02:
        heatmap_window = st.number_input("Heatmap plotting window", value=100)
    with col03:
        show_datetime = st.checkbox('Show datetime', value=True)

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


