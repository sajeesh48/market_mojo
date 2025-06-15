import yfinance as yf

def get_nse_stock_price(ticker_symbol):
    """
    Returns the current price of the given NSE stock ticker (e.g., 'RELIANCE.NS').
    """
    stock = yf.Ticker(ticker_symbol)
    stock_info = stock.info
    return stock_info.get("currentPrice")
