import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import datetime
from datetime import date
import multiprocessing
from binance.client import Client
import json
import websocket
import numpy as np
import coinbasepro as cbp
from plotly.subplots import make_subplots

def binance_charts(input_symbol):
    currency = f'{input_symbol}usdt'
    symbol = currency.upper()
    def chart_1d():
        # Connects to the Binance API and takes the daily candles since the start_time. (https://binance-docs.github.io/apidocs/spot/en/#general-info)
        client = Client()
        granularity = Client.KLINE_INTERVAL_1DAY
        start_time = "18 August, 2021"
        today = datetime.datetime.now(datetime.timezone.utc)
        system_time = today.strftime("%Y-%m-%d %H:%M")
        end_time = system_time
        klines = np.array(client.get_historical_klines(symbol, granularity, start_time, end_time))
        binance_api_df = pd.DataFrame(klines.reshape(-1, 12), dtype=float, columns=(
            'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'))
        binance_client_df = binance_api_df[['Time', 'Open', 'High', 'Low', 'Close']].copy()
        binance_client_df['Time'] = pd.to_datetime(binance_client_df['Time'], unit='ms')
        binance_client_df['Time'] = binance_client_df['Time'].dt.strftime("%Y-%m-%d %H:%M")
        binance_client_df['Ticker'] = '1 Day'
        binance_client_df['Exchange'] = 'Binance'
        binance_client_df['Currency'] = input_symbol
        binance_client_df = binance_client_df.iloc[-300:]
        binance_client_df.to_csv(f'binance_1d_{input_symbol}.csv', index=False)
    def chart_1m():
        # Connects to the Binance API and takes the 1 minute candles since the start_time. This is used to pre-load the candles so the dataframe is populated
        # before the websocket starts receiving new information
        opens = []
        closes = []
        highs = []
        lows = []
        times = []
        client = Client()
        granularity = Client.KLINE_INTERVAL_1MINUTE
        start_time = "20 August, 2022"
        today = datetime.datetime.now(datetime.timezone.utc)
        system_time = today.strftime("%Y-%m-%d %H:%M")
        end_time = system_time
        # get historical data from binance API
        klines = np.array(client.get_historical_klines(symbol, granularity, start_time, end_time))
        # reshape date to pandas
        binance_api_df = pd.DataFrame(klines.reshape(-1, 12), dtype=float, columns=(
            'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades',
            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'))
        # uniform data processing
        binance_client_df = binance_api_df[['Time', 'Open', 'High', 'Low', 'Close']].copy()
        binance_client_df['Time'] = pd.to_datetime(binance_client_df['Time'], unit='ms')
        binance_client_df['Time'] = binance_client_df['Time'].dt.strftime("%Y-%m-%d %H:%M")
        binance_client_df['Ticker'] = '1 Minute'
        binance_client_df['Exchange'] = 'Binance'
        binance_client_df['Currency'] = input_symbol
        binance_client_df = binance_client_df.iloc[:-1, :]
        binance_client_df = binance_client_df.iloc[-300:]
        binance_client_df.to_csv(f'binance_1m_{input_symbol}.csv', index=False)
        def on_message(wsapp, message):
            # When a ticker message is received, the ticker data is temporarily processed in the below variables.
            stream = json.loads(message)
            candle_data = stream['k']
            start_time = candle_data['t']
            candle_closed = candle_data['x']
            binance_high = candle_data['h']
            binance_low = candle_data['l']
            binance_open = candle_data['o']
            binance_close = candle_data['c']
            volume = candle_data['v']
            timestamp_to_datetime = pd.to_datetime(start_time, unit='ms')
            binance_time = timestamp_to_datetime.strftime("%Y-%m-%d %H:%M")
            if candle_closed:
                # When candle_closed == True, the previous 1minute candle data is stored in a dataframe and save to CSV.
                opens.append(float(binance_open))
                closes.append(float(binance_close))
                highs.append(float(binance_high))
                lows.append(float(binance_low))
                times.append(binance_time)
                binance_websocket_df = pd.DataFrame({'Time': times, 'Open': opens, 'High': highs, 'Low': lows, 'Close': closes, 'Ticker': '1 Minute', 'Exchange': 'Binance', 'Currency' : input_symbol})
                frames = [binance_client_df, binance_websocket_df]
                binance_df = pd.concat(frames)
                df_csv = pd.DataFrame(binance_df)
                df_csv.to_csv(f'binance_1m_{input_symbol}.csv', index=False)
        wsapp = websocket.WebSocketApp(f'wss://stream.binance.com:9443/ws/{currency}@kline_1m', on_message=on_message)
        wsapp.run_forever()
    chart_1d()
    chart_1m()

def coinbase_charts(input_symbol):
    currency = f'{input_symbol.upper()}-USD'
    today = datetime.datetime.now(datetime.timezone.utc)
    def chart_1d():
        # Coinbase pro client wrapper (https://github.com/danpaquin/coinbasepro-python)
        Client = cbp.PublicClient()
        coinbase_api_df = pd.DataFrame(
            Client.get_product_historic_rates(product_id=f'{input_symbol.upper()}-USD', stop=today, granularity=86400))
        coinbase_api_df.columns = ["Time", "Low", "High", "Open", "Close", "Volume"]
        coinbase_client_df = coinbase_api_df[['Time', 'Open', 'High', 'Low', 'Close']].copy()
        coinbase_client_df['Ticker'] = '1 Day'
        coinbase_client_df['Exchange'] = 'Coinbase'
        coinbase_client_df['Currency'] = input_symbol
        coinbase_client_df['Time'] = pd.to_datetime(coinbase_client_df['Time'], unit='ms')
        coinbase_client_df['Time'] = coinbase_client_df['Time'].dt.strftime("%Y-%m-%d %H:%M")
        coinbase_client_df.sort_values(by='Time', ascending=True, inplace=True)
        coinbase_client_df.to_csv(f'coinbase_1d_{input_symbol}.csv', index=False)
    def chart_1m():
        # The coinbase websocket requires a different set up to Binance, including a subscription message:
        # https://docs.cloud.coinbase.com/prime/docs/websocket-feed
        def on_open(wsapp):
            subscription = {
                'type': 'subscribe',
                'channels':[
                    {'name':'ticker',
                    'product_ids':[currency]}
                    ]
            }
            wsapp.send(json.dumps(subscription))
        times = []
        prices = []
        opens = []
        closes = []
        highs = []
        lows = []
        system_time = today.strftime("%Y-%m-%d %H:%M")
        times.append(system_time)
        Client = cbp.PublicClient()
        coinbase_api_df = pd.DataFrame(Client.get_product_historic_rates(product_id=f'{input_symbol.upper()}-USD', stop=today, granularity=60))
        coinbase_api_df.columns = ["Time", "Low", "High", "Open", "Close", "Volume"]
        coinbase_client_df = coinbase_api_df[['Time', 'Open', 'High', 'Low', 'Close']].copy()
        coinbase_client_df['Ticker'] = '1 Minute'
        coinbase_client_df['Exchange'] = 'Coinbase'
        coinbase_client_df['Currency'] = input_symbol
        coinbase_client_df['Time'] = pd.to_datetime(coinbase_client_df['Time'], unit='ms')
        coinbase_client_df['Time'] = coinbase_client_df['Time'].dt.strftime("%Y-%m-%d %H:%M")
        coinbase_client_df.sort_values(by='Time', ascending=True, inplace=True)
        coinbase_client_df = coinbase_client_df.iloc[:-1, :]
        coinbase_client_df.to_csv(f'coinbase_1m_{input_symbol}.csv', index=False)
        def on_message(wsapp, message):
            stream = json.loads(message)
            stream_datetime = stream['time']
            coinbase_time = stream_datetime[:-11]
            coinbase_time = coinbase_time.replace("T", " ")
            coinbase_price = stream['price']
            prices.append(float(coinbase_price))
            if coinbase_time not in times:
                # The coinbase websocket does not relay OCLC data. Therefore, all of the prices and times are stored to calculate the data.
                closes.append(float(prices[-2]))
                opens.append(float(prices[-1]))
                highest_price = max(prices)
                lowest_price = min(prices)
                highs.append(float(highest_price))
                lows.append(float(lowest_price))
                prices.clear()
                coinbase_websocket_df = pd.DataFrame({'Time': times, 'Open': opens, 'High': highs, 'Low': lows, 'Close': closes, 'Ticker': '1 Minute', 'Exchange': 'Coinbase', 'Currency' : input_symbol})
                times.append(coinbase_time)
                frames = [coinbase_client_df, coinbase_websocket_df]
                coinbase_df = pd.concat(frames)
                df_csv = pd.DataFrame(coinbase_df)
                df_csv.to_csv(f'coinbase_1m_{input_symbol}.csv', index=False)
        wsapp = websocket.WebSocketApp('wss://ws-feed.exchange.coinbase.com', on_message=on_message, on_open=on_open)
        wsapp.run_forever()
    chart_1d()
    chart_1m()


# https://dash.plotly.com/
# Dash is a framework for integrating live plotly charts with a flask webserver.
app = dash.Dash()
app.layout = html.Div(children=[
        html.H1('Real-time Crypto Chart'),
        dcc.Dropdown(['BTC Binance 1m', 'BTC Coinbase 1m', 'ETH Binance 1m', 'ETH Coinbase 1m', 'BTC Binance 1d', 'BTC Coinbase 1d', 'ETH Binance 1d', 'ETH Coinbase 1d'], 'BTC Coinbase 1m', id='chart_selector'),
        html.Div(id='output-graph'),
        dcc.Interval(
            id='interval-component',
            interval=1 * 60000,  # in milliseconds
            n_intervals=0,
        )
        ])

@app.callback(
        Output(component_id='output-graph', component_property = 'children'),
        [Input('interval-component', 'n_intervals'), Input('chart_selector', 'value')])

def update_graph(n, value):
    # Minute charts:
    if value == 'BTC Binance 1m':
        full_df = pd.read_csv('binance_1m_btc.csv')
        arbitrage_df = pd.read_csv('coinbase_1m_btc.csv')
    elif value == 'BTC Coinbase 1m':
        full_df = pd.read_csv('coinbase_1m_btc.csv')
        arbitrage_df = pd.read_csv('binance_1m_btc.csv')
    elif value == 'ETH Binance 1m':
        full_df = pd.read_csv('binance_1m_eth.csv')
        arbitrage_df = pd.read_csv('coinbase_1m_eth.csv')
    elif value == 'ETH Coinbase 1m':
        full_df = pd.read_csv('coinbase_1m_eth.csv')
        arbitrage_df = pd.read_csv('binance_1m_eth.csv')
    # Daily charts:
    elif value == 'BTC Binance 1d':
        full_df = pd.read_csv('binance_1d_btc.csv')
        arbitrage_df = pd.read_csv('coinbase_1d_btc.csv')
    elif value == 'BTC Coinbase 1d':
        full_df = pd.read_csv('coinbase_1d_btc.csv')
        arbitrage_df = pd.read_csv('binance_1d_btc.csv')
    elif value == 'ETH Binance 1d':
        full_df = pd.read_csv('binance_1d_eth.csv')
        arbitrage_df = pd.read_csv('coinbase_1d_eth.csv')
    elif value == 'ETH Coinbase 1d':
        full_df = pd.read_csv('coinbase_1d_eth.csv')
        arbitrage_df = pd.read_csv('binance_1d_eth.csv')
    full_df['Price_difference'] = full_df['Close'] - arbitrage_df['Close']
    full_df['MA30'] = full_df['Close'].rolling(window=30).mean()
    full_df['MA5'] = full_df['Close'].rolling(window=5).mean()
    full_df['EMA'] = full_df['Close'].ewm(com=0.5).mean()
    candlestick = go.Candlestick(x=full_df['Time'],
                                         open=full_df['Open'],
                                         high=full_df['High'],
                                         low=full_df['Low'],
                                         close=full_df['Close'], name = 'Candlestick Chart (USD$)')
    price_difference = go.Bar(x=full_df['Time'], y=full_df['Price_difference'], showlegend=True, name = f'Price +/- vs {arbitrage_df["Exchange"].iat[-1]}')
    MA5 = go.Scatter(x=full_df['Time'], y=full_df['MA5'], opacity=0.7, line=dict(color='blue', width=2), name='MA5')
    MA30 = go.Scatter(x=full_df['Time'], y=full_df['MA30'], opacity=0.7, line=dict(color='purple', width=2), name='MA30')
    EMA = go.Scatter(x=full_df['Time'], y=full_df['EMA'], opacity=0.7, line=dict(color='cyan', width=2), name='EMA')
    fig = make_subplots(rows=2, cols=1, row_heights=[2, 1])
    fig.add_trace(candlestick, row=1, col=1)
    fig.add_trace(MA5, row=1, col=1)
    fig.add_trace(MA30, row=1, col=1)
    fig.add_trace(EMA, row=1, col=1)
    fig.add_trace(price_difference, row=2, col=1)
    return dcc.Graph(id='example-graph', figure= fig, style={'height': '92vh'})
# if __name__ == '__main__':
#     app.run_server(debug=True)

if __name__ == '__main__':
    p = multiprocessing.Process(target=binance_charts, args=('btc',))
    p.start()
    p2 = multiprocessing.Process(target=coinbase_charts, args=('btc',))
    p2.start()
    p3 = multiprocessing.Process(target=binance_charts, args=('eth',))
    p3.start()
    p4 = multiprocessing.Process(target=coinbase_charts, args=('eth',))
    p4.start()
    app.run_server(debug=False)



