import asyncio
import aiohttp
import logging 
import json
from collections import defaultdict, deque
from typing import Optional, Set
import time
import datetime

from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream
logging.basicConfig(level=logging.INFO , format='%(asctime)s - %(levelname)s - %(message)s')

FILL = "fill"
PARTIAL_FILL = "partial_fill"
FILL_EVENT = [FILL, PARTIAL_FILL]

CANCELED = "canceled"
ORDER_CYLE_END_EVENT = [FILL, CANCELED]


SIDE_BUY = 'buy'
SIDE_SELL = 'sell'
ALL_SIDES= [SIDE_BUY, SIDE_SELL]

ORDER_TYPE_LIMIT = 'limit'
ORDER_TYPE_IOC = 'ioc'
ORDER_TYPE_DAY  = 'day'
ORDER_TYPE_GTC  = 'gtc'
ORDER_TYPE_DAY = "day"
ALL_ORDER_TYPES = [ORDER_TYPE_LIMIT, ORDER_TYPE_IOC,ORDER_TYPE_DAY, ORDER_TYPE_GTC, ORDER_TYPE_DAY]


## TODO Finish on_trade_update 

class Credentials:
    key_id = None
    secret_key = None
    headers = None
    _credentials_loaded = False

    @staticmethod
    def load_credentials(credentials_file='key.txt'):
        if not Credentials._credentials_loaded:  # Only load if not already loaded
            try:
                with open(credentials_file, 'r') as file:
                    Credentials.headers = json.loads(file.read())
                    Credentials.key_id = Credentials.headers.get('APCA-API-KEY-ID')
                    Credentials.secret_key = Credentials.headers.get('APCA-API-SECRET-KEY')
                    Credentials._credentials_loaded = True  # Set flag to True after loading
            except Exception as e:
                print(f"Error loading credentials: {e}")

    @classmethod
    def KEY_ID(cls):
        cls.load_credentials()  # Will load only if not already loaded
        return cls.key_id

    @classmethod
    def SECRET_KEY(cls):
        cls.load_credentials()  # Will load only if not already loaded
        return cls.secret_key

    @classmethod
    def HEADERS(cls):
        cls.load_credentials()  # Will load only if not already loaded
        return cls.headers


class Client:
    session = None

    @classmethod
    async def start_session(cls):
        if not cls.session:
            cls.session = aiohttp.ClientSession(headers=Credentials.HEADERS())

    @classmethod
    async def close_session(cls):
        if cls.session:
            await cls.session.close()
            cls.session = None

class DataClient(): 
    def __init__(self, max_nr_trade_history: int = 100, max_nr_bar_history: int = 100, symbols : Optional[Set[str]] = None):   
        self._max_trade_history = max_nr_trade_history
        self._max_bar_history = max_nr_bar_history
        self._symbols = symbols if symbols is not None else set()
        self._base_url = URL('https://paper-api.alpaca.markets')
        self._data_feed = "iex"
        self._last_trade_price = {}
        #self._trade_tick_hist = defaultdict(deque)
        self._trade_tick_hist = defaultdict(lambda: deque(maxlen=self._max_trade_history))
        #self._last_quote = defaultdict(deque) ## TODO: modify it to self._last_quote = {}
        self._last_quote = {}
        self._last_mid_price = {}
        self._last_bar = {}
        #self._bar_hist = defaultdict(deque)
        self._bar_hist = defaultdict(lambda: deque(maxlen=self._max_bar_history))
        self._trade_update = defaultdict(dict)
        self._position_manager = PositionManager()
                      
    async def start(self):
        self._position_manager = await PositionManager.create()
        stream = Stream(Credentials.KEY_ID(), Credentials.SECRET_KEY(), base_url=self._base_url, data_feed=self._data_feed)
        stream.subscribe_trades(self.on_trade, *self._symbols)
        stream.subscribe_quotes(self.on_quote, *self._symbols)
        stream.subscribe_bars(self.on_bar, *self._symbols)
        stream.subscribe_trade_updates(self.on_trade_update) 
        if asyncio.get_event_loop().is_running():
            await stream._run_forever()
        else:
            await stream.run()
    
    async def on_trade(self,trade_tick ) -> None:    
        symbol = trade_tick.symbol
        self._last_trade_price[symbol] = trade_tick.price
        _trade_hist_to_update = self._trade_tick_hist[symbol]
        _trade_hist_to_update.append(trade_tick)
        #while len(_trade_hist_to_update) > self._max_trade_history:
        #    _trade_hist_to_update.popleft()
        #logging.info(trade_tick)

    def get_last_trade_price(self, symbol : str) ->  Optional[float]:
        return self._last_trade_price.get(symbol, None)
                                                  
    async def on_quote(self, quote) -> None:
        symbol = quote.symbol
        self._last_quote[symbol] = quote
        if quote.ask_price != 0 and quote.bid_price != 0:
            midprice = (quote.ask_price + quote.bid_price) * 0.5 
            self._last_mid_price[symbol] = midprice
        else: 
            pass        
        #logging.info(quote)
      
    def get_last_mid_price(self, symbol : str) ->  Optional[float]:
        return self._last_mid_price.get(symbol, None)
    
    def get_last_quote(self, symbol : str) -> Optional[dict]:
        return self._last_quote.get(symbol, None) 

    async def on_bar(self, bar) -> None:
        symbol = bar.symbol
        self._last_bar[symbol] = bar
        _bar_hist_to_update = self._bar_hist[symbol]
        _bar_hist_to_update.append(bar)
    
    def get_last_bar(self, symbol):
        return self._last_bar.get(symbol, None)
    
    def get_bar_hist(self, symbol):
        return self._bar_hist.get(symbol, None)

    async def on_trade_update(self, trade_update) -> None:
        symbol = trade_update.order["symbol"]
        id = trade_update.order["id"]      
        filled_qty = trade_update.order["filled_qty"]
        side = trade_update.order["side"]
        #logging.info(f"Symbol: {symbol}, ID: {id}, Type of _trade_update[symbol]: {type(self._trade_update[symbol])}")  
        if trade_update.event in FILL_EVENT:   ## TODO order status update : fill, cancel, rejected 
            #print(f"update position for {symbol}")
            position_qty = float(trade_update.position_qty) 
            await self._position_manager.update_position(symbol, position_qty)
        self._trade_update[symbol][id] = trade_update
        #logging.info(trade_update)
        if (trade_update.event == PARTIAL_FILL):
            logging.info(f"PARTIAL FILL: {side} order for {symbol}, filled {filled_qty}.")
        if (trade_update.event == FILL):
            logging.info(f"FILL: {side} order for {symbol}, filled {filled_qty}.")


    #def get_trade_update(self, symbol : str, id :str):
    #        return self._trade_update.get(symbol, None)
        
    def get_trade_update(self, symbol: str, id: str = None):
        symbol_trades = self._trade_update.get(symbol, {})
        if id is None:
            return symbol_trades  # Return all trade updates for the symbol
        else:
            return symbol_trades.get(id)  # Return a specific trade update for the id

    
    def get_position_by_symbol(self, symbol) -> float:
        position_info = self._position_manager._positions_by_symbol.get(symbol, None)
        if position_info is not None:
            return position_info.get("position", 0.0)
        else:
            return 0.0  # Return 0.0 if the symbol is not found
        
    def get_all_positions(self) -> dict:
        return self._position_manager._positions_by_symbol
    
    async def get_position_object_by_symbol(self, symbol) -> dict:
        """Fetch the position object for a specific symbol."""
        # Check if the position is cached and refresh only if necessary
        position_object = self._position_manager._position_objects_by_symbol.get(symbol, None)
        if position_object is None:
            # If it's not cached, fetch from the API
            await self._position_manager.get_positions(symbol=symbol)
            position_object = self._position_manager._position_objects_by_symbol.get(symbol, None)
        return position_object


class PositionManager():
    def __init__(self):
        #self.session = None 
        self._pos_url = "https://paper-api.alpaca.markets/v2/positions"
        self._position_objects_by_symbol = {}
        self._positions_by_symbol = {}
        self._last_update_time = 0

    @classmethod
    async def create(cls):
        instance = cls()
        await Client.start_session()
        await instance.get_positions()
        return instance
    
    async def get_positions(self, symbol=None, force_refresh=False):
        """Fetch positions from Alpaca API, with optional force refresh."""
        current_time = time.time()

        # Check if we need to refresh the positions (e.g., every 60 seconds or if forced)
        if not force_refresh and (current_time - self._last_update_time < 5):
            logging.info("Using cached positions data")
            return

        url = f"{self._pos_url}/{symbol}" if symbol else self._pos_url
        try:
            async with Client.session.get(url) as result:
                if result.status == 200:
                    response = await result.json()
                    if symbol:
                        response = [response]  # Ensure uniform list format

                    # Update positions by symbol and position objects
                    for individual_response in response:
                        symbol = individual_response.get("symbol", "Unknown").strip()
                        qty = float(individual_response.get("qty", 0))
                        self._position_objects_by_symbol[symbol] = individual_response
                        self._positions_by_symbol[symbol] = {"position": qty}

                    # Update the last time positions were fetched
                    self._last_update_time = current_time
                else:
                    response_text = await result.text()
                    logging.warning(f"Failed to get positions: Status {result.status}, Details: {response_text}")
        except Exception as e:
            logging.warning(f"Error to get positions: {e}")

    async def update_position_objects(self):
        """Force update of all position objects from the Alpaca API."""
        await self.get_positions(force_refresh=True)  # Force refresh of positions

    async def update_position(self, symbol, position_qty):
        # Check if the symbol already exists in the dictionary
        if symbol not in self._positions_by_symbol:
            self._positions_by_symbol[symbol] = {"position": 0}
        self._positions_by_symbol[symbol]["position"] = position_qty


class OrderManager():
    def __init__(self):
        self._order_url = "https://paper-api.alpaca.markets/v2/orders"
        self._pos_url = "https://paper-api.alpaca.markets/v2/positions"
        self.session = None  # We'll initialize this in an async context
        #self._submitted_order_by_order_id = defaultdict(dict)
           
    async def start(self):
        if not Client.session:
            await Client.start_session()

    async def insert_order(self, symbol: str, price: float, quantity: int, side: str, order_type: str):
        assert side in ALL_SIDES, f"side must be one of {ALL_SIDES}"
        assert order_type in ALL_ORDER_TYPES, f"order_type must be one of {ALL_ORDER_TYPES}"
        params = {"symbol": symbol,"qty": quantity, "side": side, "type": ORDER_TYPE_LIMIT, "limit_price": price,"time_in_force": order_type}
        await Client.start_session()
        try:
            async with Client.session.post(self._order_url, json=params) as result:
            #async with self.session.post(self._order_url, json=params) as result:
                response_text = await result.text()
                if result.status == 200:
                    logging.info(f"Succesful Order Insertion - Symbol : {symbol}, Qty : {quantity}, Side : {side}, Price : {price}")
                    order_response = await result.json()
                    #logging.info(f"Successful Order Insertion: {order_response}")
                    symbol = order_response["symbol"]
                    id = order_response["id"]
                    #self._submitted_order_by_order_id[symbol][id] = id ## record id instead of order_response
                    return InsertOrderResponse(success=True, order_id=id, error=None)
                else:
                    logging.warning(f"Order Insertion Error (Status {result.status}): {response_text}")
                    return InsertOrderResponse(success=False, order_id=None, error= f"Error (Status {result.status}): {response_text}" )
                    
        except Exception as e:
            logging.warning(f"Error inserting order: {e}")
            return InsertOrderResponse(success=False, order_id=None, error=e)

    async def close_all_positions(self, cancel_orders: bool = True):
        params = {"cancel_orders": cancel_orders}
        responses = []
        try:
            async with Client.session.delete(self._pos_url, json=params) as result:
                response_text = await result.text()
                if result.status == 207:  # Handle Multi-Status responses
                    logging.info("Close All Positions Request Received Multi-Status Response")
                    response = await result.json()

                    # Process each individual response in the multi-status response
                    for individual_response in response:
                        symbol = individual_response.get("symbol", "Unknown")
                        status = individual_response.get("status")
                        body = individual_response.get("body", {})
                        #id = individual_response.get("id")

                        if status == 200:
                            logging.info(f"Closed position for {symbol}: {body}")
                            #self._submitted_order_by_order_id[symbol][id] = individual_response
                            responses.append(ClosePositionResponse(symbol=symbol, success=True, status=status))
                        else:
                            logging.warning(f"Failed to close position for {symbol}: Status {status}, Details: {body}")
                            responses.append(ClosePositionResponse(symbol=symbol, success=False, status=status, error=body))

                else:
                    logging.warning(f"Unexpected status {result.status} received: {response_text}")
                    responses.append(ClosePositionResponse(symbol="Unknown", success=False, status=result.status, error=response_text))

        except Exception as e:
            logging.error(f"Error closing all positions: {e}")
            responses.append(ClosePositionResponse(symbol="Unknown", success=False, status=500, error=str(e)))

        return responses

    async def close_position(self, symbol: str, qty: float = None, percentage: float = None): ##TODO check close_position method and if pos in pos manager adjust accordingly
        # Check if neither qty nor percentage is provided, implying a full close of the position.
        if qty is None and percentage is None:
            logging.info(f"Closing full position for {symbol}.")
            
        # Ensure that qty and percentage are not both provided at the same time.
        assert not (qty is not None and percentage is not None), "Specify either qty or percentage, but not both."  ##TODO check this logic

        # Construct the URL for the specific symbol's position.
        pos_url_with_symbol = f"{self._pos_url}/{symbol}"

        # Prepare the parameters for the request.
        params = {}
        if qty is not None:
            params["qty"] = qty
        if percentage is not None:
            params["percentage"] = percentage

        try:
            # Send the DELETE request to the API.
            async with Client.session.delete(pos_url_with_symbol, params=params) as result:
                response_text = await result.text()

                # Check for a successful response.
                if result.status == 200:
                    logging.info(f"Closed position for {symbol}.")
                    order_response = await result.json()
                    logging.info(f"Order response: {order_response}")
                    return ClosePositionResponse(symbol=symbol, success=True, status=result.status)
                else:
                    logging.warning(f"Failed to close position for {symbol} (Status {result.status}): {response_text}")
                    return ClosePositionResponse(symbol=symbol, success=False, status=result.status, error=response_text)

        except Exception as e:
            logging.error(f"Error closing position for {symbol}: {e}")
            return ClosePositionResponse(symbol=symbol, success=False, status=500, error=str(e))


    async def cancel_all_orders(self):
        try:
            # Send DELETE request to cancel all orders
            async with Client.session.delete(self._order_url) as result:

                if result.status == 207:  # Multi-Status
                    logging.info("Received multi-status response for canceling all orders.")
                    order_statuses = await result.json()  # Parse the JSON response

                    # Process each order's cancellation status
                    success = True
                    order_results = {}
                    for order in order_statuses:
                        order_id = order["id"]
                        status = order["status"]
                        order_results[order_id] = status

                        if status == 200:
                            logging.info(f"Order {order_id} successfully canceled.")
                        elif status == 500:
                            logging.warning(f"Failed to cancel order {order_id}. Status: {status}")
                            success = False

                    return CancelAllOrdersResponse(success=success, order_statuses=order_results)

                else:
                    logging.warning(f"Unexpected response status: {result.status}")
                    return CancelAllOrdersResponse(success=False, order_statuses={}, error=f"Unexpected status {result.status}")

        except Exception as e:
            logging.error(f"Error occurred while canceling all orders: {e}")
            return CancelAllOrdersResponse(success=False, order_statuses={}, error=str(e))

    async def cancel_order(self, order_id: str):
        assert order_id is not None, "Order ID must be specified"   
        # Add the order ID to the URL path
        cancel_url_with_id = f"{self._order_url}/{order_id}"  
        try:
            # Send the DELETE request to cancel the order
            async with Client.session.delete(cancel_url_with_id) as result:
                response_text = await result.text()

                # Check for the success code (204)
                if result.status == 204:
                    logging.info(f"Successfully canceled Order ID : {order_id}")
                    return CancelOrderResponse(success=True)
                elif result.status == 404:
                    logging.warning(f"Order {order_id} not found: {response_text}")
                    return CancelOrderResponse(success=False, error="Order not found")
                elif result.status == 422:
                    logging.warning(f"Order {order_id} is no longer cancelable (Status {result.status}): {response_text}")
                    return CancelOrderResponse(success=False, error="Order no longer cancelable")
                else:
                    logging.warning(f"Failed to cancel order {order_id} (Status {result.status}): {response_text}")
                    return CancelOrderResponse(success=False, error=f"Failed with status {result.status}")
        except Exception as e:
            logging.warning(f"Error cancelling order {order_id}: {e}")
            return CancelOrderResponse(success=False, error=str(e))
    

    
    ## TODO Get orders and get order by id

    ## Todo 
    async def replace_order(self):
        pass


class InsertOrderResponse:
    def __init__(self, success: bool, order_id: Optional[int], error: Optional[str]):
        self.success: bool = success
        self.order_id: Optional[int] = order_id
        self.error: Optional[str] = error

    def __str__(self):
        return f"InsertOrderResponse(success={self.success}, order_id={self.order_id}, error='{self.error}')"

class ClosePositionResponse:
    def __init__(self, symbol: str, success: bool, status: int, error: Optional[str] = None):
        self.symbol: str = symbol
        self.success: bool = success
        self.status: int = status
        self.error: Optional[str] = error

    def __str__(self):
        return f"ClosePositionResponse(symbol={self.symbol}, success={self.success}, status={self.status}, error={self.error})"

class CancelOrderResponse:
    def __init__(self, success: bool, error: Optional[str] = None):  # <-- Fix __init__
        self.success: bool = success
        self.error: Optional[str] = error

    def __str__(self):
        return f"CancelOrderResponse(success={self.success}, error='{self.error}')"
    
class CancelAllOrdersResponse:
    def __init__(self, success: bool, order_statuses: dict, error: Optional[str] = None):
        self.success: bool = success
        self.order_statuses: dict = order_statuses  # Maps order ID to status code
        self.error: Optional[str] = error

    def __str__(self):
        return f"CancelAllOrdersResponse(success={self.success}, order_statuses={self.order_statuses}, error='{self.error}')"
    
class MarketClockCalendar:

    def __init__(self):
        self._clock_url = "https://paper-api.alpaca.markets/v2/clock"
        self._calendar_url = "https://paper-api.alpaca.markets/v2/calendar"
        self.session = None  # We'll initialize this in an async context

    async def start(self):
        if not Client.session:
            await Client.start_session()

    async def get_market_clock_info(self) -> Optional[dict]:
        try:
            async with Client.session.get(self._clock_url) as result:
                response_text = await result.text()
                if result.status == 200:
                    market_clock_info = await result.json()   
                    return market_clock_info
                else:
                    logging.warning(f"Failed to get market clock info. Error (Status {result.status}): {response_text} ")
        except Exception as e:
            logging.warning(f"Error getting market clock info: {e}")
        return None
    
    async def get_market_calendar_info(self, start : Optional[str] = None, end : Optional[str] = None, date_type : str = "TRADING"):
        allowed_date_types = ["TRADING" , "SETTLEMENT"]
        assert date_type in allowed_date_types, f"date_type must in {allowed_date_types}"
        _calendar_url_params = self._calendar_url + f"?start={start}T00%3A00%3A00Z&end={end}T23%3A59%3A59Z&date_type={date_type}"
        try:
            async with Client.session.get(_calendar_url_params) as result:
                response_text = await result.text()
            if result.status == 200:
                market_calendar_info = await result.json()   
                return market_calendar_info
            else:
                logging.warning(f"Failed to get market calendar info. Error (Status {result.status}): {response_text} ")
        except Exception as e:
            logging.warning(f"Error getting market calendar info: {e}")
        return None
    
    async def is_open(self) -> bool:
        clock_info = await self.get_market_clock_info()
        is_open = clock_info["is_open"]
        return is_open
    
    async def time_left_before_next_open(self) -> Optional[datetime.datetime]: # Check time left before next open (outside market hours)
        clock_info = await self.get_market_clock_info()
        is_open = clock_info["is_open"]
        if is_open:
            return None
        timestamp = datetime.datetime.strptime(clock_info["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
        next_open = datetime.datetime.strptime(clock_info["next_open"][:19], "%Y-%m-%dT%H:%M:%S")
        time_left_before_next_open = next_open - timestamp
        return time_left_before_next_open

    async def time_left_before_next_close(self) -> Optional[datetime.datetime]: # Check time left before next open (during market hours)
        clock_info = await self.get_market_clock_info()
        is_open = clock_info["is_open"]
        if not is_open:
            return None
        timestamp = datetime.datetime.strptime(clock_info["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
        next_close = datetime.datetime.strptime(clock_info["next_close"][:19], "%Y-%m-%dT%H:%M:%S")
        time_left_before_next_close = next_close - timestamp
        return time_left_before_next_close




## Todo
class ModifyOrderResponse:
    pass


class RiskManager():
    pass

