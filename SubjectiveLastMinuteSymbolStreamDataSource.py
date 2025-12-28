import time
import json
import os
from datetime import datetime, timedelta
from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveLastMinuteSymbolStreamDataSource(SubjectiveDataSource):
    connection_type = "ExchangeStream"
    connection_fields = ["exchange", "api_key", "api_secret", "minutes_ago", "symbols"]

    def get_icon(self):
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        try:
            with open(icon_path, 'r') as f:
                return f.read()
        except Exception as e:
            BBLogger.log(f"Error reading icon file: {str(e)}")
            return ""

    def get_connection_data(self):
        return {"connection_type": self.connection_type, "fields": list(self.connection_fields)}

    def notify(self, data):
        self.update(data)

    def _get_param(self, key, default=None):
        return self.params.get(key, default)

    def _write_output_to_file(self, data):
        """Write symbol data to timestamped output file"""
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        filename = f"{timestamp}_{self.get_name()}_output.txt"

        try:
            with open(filename, 'w') as f:
                f.write(json.dumps(data, indent=2, default=str))
            BBLogger.log(f"Output written to {filename}")
        except Exception as e:
            BBLogger.log(f"Error writing to file {filename}: {str(e)}")

    def _fetch_symbol_data_from_exchange(self, exchange_client, symbols, minutes_ago):
        """Fetch recent data for all symbols from the exchange"""
        all_symbol_data = []

        try:
            # Get current time and calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=minutes_ago)

            # If no specific symbols provided, get all available symbols
            if not symbols or symbols == "all":
                try:
                    tickers = exchange_client.get_all_tickers()
                    if isinstance(tickers, list):
                        symbols = [ticker.get('symbol') for ticker in tickers if ticker.get('symbol')]
                    else:
                        BBLogger.log("Could not retrieve symbol list from exchange")
                        symbols = []
                except Exception as e:
                    BBLogger.log(f"Error getting all symbols: {str(e)}")
                    symbols = []
            elif isinstance(symbols, str):
                symbols = [s.strip() for s in symbols.split(',')]

            # Fetch data for each symbol
            for symbol in symbols:
                try:
                    # Get recent klines/candles data for this symbol
                    symbol_data = {
                        'symbol': symbol,
                        'data': [],
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat()
                    }

                    # Try to get klines data if available
                    try:
                        klines = exchange_client.get_klines(symbol, start_time, end_time)
                        if klines:
                            symbol_data['data'] = klines
                    except Exception as e:
                        BBLogger.log(f"Could not get klines for {symbol}: {str(e)}")

                    # If no klines, try to get current price at least
                    if not symbol_data['data']:
                        try:
                            current_price = exchange_client.get_current_price(symbol)
                            symbol_data['data'] = [{'price': current_price, 'timestamp': datetime.now().isoformat()}]
                        except Exception as e:
                            BBLogger.log(f"Could not get price for {symbol}: {str(e)}")

                    if symbol_data['data']:
                        all_symbol_data.append(symbol_data)

                except Exception as e:
                    BBLogger.log(f"Error fetching data for symbol {symbol}: {str(e)}")
                    continue

        except Exception as e:
            BBLogger.log(f"Error in fetch_symbol_data_from_exchange: {str(e)}")

        return all_symbol_data

    def start(self):
        """Start the periodic data streaming"""
        minutes_ago = int(self._get_param("minutes_ago", 30))
        exchange_name = self._get_param("exchange", "binance")
        api_key = self._get_param("api_key", "")
        api_secret = self._get_param("api_secret", "")
        symbols = self._get_param("symbols", "all")

        BBLogger.log(f"Starting Last {minutes_ago} minutes symbol stream for exchange: {exchange_name}")

        # Initialize exchange client (you would normally use ccxt or similar)
        try:
            import ccxt
            exchange_class = getattr(ccxt, exchange_name.lower())
            exchange_client = exchange_class({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True
            })
        except Exception as e:
            BBLogger.log(f"Error initializing exchange client: {str(e)}")
            return

        while True:
            current = datetime.now()
            BBLogger.log(f"Fetching last {minutes_ago} minutes of data for all symbols...")

            # Fetch data from exchange
            data = self._fetch_symbol_data_from_exchange(exchange_client, symbols, minutes_ago)

            after_query = datetime.now()
            time_query_took = after_query - current

            # Write data to timestamped file
            self._write_output_to_file(data)

            # Notify this data source's listeners with the data
            self.update(data)

            # Calculate wait time before next query
            if (time_query_took < timedelta(minutes=minutes_ago)):
                wait_time = timedelta(minutes=minutes_ago).total_seconds() - time_query_took.total_seconds()
                BBLogger.log(f"Obtained last {minutes_ago} minutes sequences successfully, now waiting {wait_time} seconds")
                print("finished..")
                time.sleep(wait_time)
                print("Executing again")

    def fetch(self):
        if self.status_callback:
            self.status_callback(self.get_name(), "stream_started")
        self.start()
        self.set_fetch_completed(True)
        BBLogger.log(f"Stream started for {self.get_name()}")
