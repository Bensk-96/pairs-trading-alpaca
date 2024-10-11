import asyncio
import logging 
import numpy as np
from typing import Optional 
from collections import deque
from core import DataClient, OrderManager, Client
from core import ORDER_TYPE_IOC, ORDER_TYPE_GTC, SIDE_BUY, SIDE_SELL

class PairTrade:

    def __init__(self, 
                dataclient: DataClient, 
                ordermanager: OrderManager,
                asset1 : Optional[str] = None,
                asset2 : Optional[str] = None,
                capital : Optional[float] = None,
                downsample : Optional[int] = None,
                hedge_ratio : Optional[float] = None,
                const : Optional[float] = None,
                k : int = 2):
        self._dataclient: DataClient = dataclient
        self._ordermanager: OrderManager = ordermanager
        self._asset1 : Optional[str] = asset1
        self._asset2 : Optional[str] = asset2
        self._asset1_max_position : Optional[float] = None
        self._asset2_max_position : Optional[float] = None
        self._capital : Optional[float] = capital 
        self._downsample : Optional[int] = downsample
        self._hedge_ratio : Optional[float] = hedge_ratio
        self._const : Optional[float] = const
        self._k : int = k
        self._length_of_spread : int = int(1200 / self._downsample) 
        self._spread_list : deque = deque(maxlen=self._length_of_spread)
        self._spread_position : int = 0 
        self._pertb_list : deque = deque(maxlen= 2)
        self._signal : Optional[int] = None 
        self._order_type : str = ORDER_TYPE_IOC if self._downsample <= 5 else ORDER_TYPE_GTC
    
    async def _calculate_max_position(self) -> None: ## TODO tiny hedge ratio issue 
        mid_price_asset1 = self._dataclient.get_last_mid_price(self._asset1)
        mid_price_asset2 = self._dataclient.get_last_mid_price(self._asset2)
        if (mid_price_asset1 is not None) and (mid_price_asset2  is not None):
            self._asset2_max_position = (self._capital) / (self._hedge_ratio * mid_price_asset1 + mid_price_asset2)   
            self._asset1_max_position = self._asset2_max_position * self._hedge_ratio 
            self._asset2_max_position = round(self._asset2_max_position) 
            self._asset1_max_position = round(self._asset1_max_position) 
        else:
            pass

    def _calculate_spread(self) -> None:
        mid_price_asset1 = self._dataclient.get_last_mid_price(self._asset1)
        mid_price_asset2 = self._dataclient.get_last_mid_price(self._asset2)
        if (mid_price_asset1 is not None) and (mid_price_asset2 is not None):
            spread = mid_price_asset2 - (self._hedge_ratio * mid_price_asset1 + self._const)
            self._spread_list.append(spread)
            logging.info(f"spread of {self._asset1}-{self._asset2} is {spread}")
        else:
            return

    def _calculate_pertb(self) -> None:
        if len(self._spread_list) < self._length_of_spread:
            len_spread_list = len(self._spread_list)
            #logging.info(f"length of spread list of {self._asset1}-{self._asset2} is {len_spread_list}. Required length is {self._length_of_spread}")
            if len_spread_list % 10 == 0:
                logging.info(f"fetching spread data for {self._asset1}-{self._asset2}. Number of data points is {len_spread_list}, requires {self._length_of_spread}")
            return
        else:
            spread_rolling_mean = np.mean(self._spread_list)
            spread_rolling_std = np.std(self._spread_list)
            upper_band = spread_rolling_mean + self._k * spread_rolling_std
            lower_band = spread_rolling_mean - self._k * spread_rolling_std
            pertb = (self._spread_list[-1] - lower_band) / (upper_band - lower_band) ##TODO try backtest signal (self._spread_list[-1] - spread_rolling_mean) / (upper_band - lower_band) (z-score)     
            self._pertb_list.append(pertb)  
            logging.info(f"%b of {self._asset1}-{self._asset2} pairs is {pertb}")

    def _generate_signal(self) -> None:
        if len(self._pertb_list) < 2:
            return
        else:
            if self._spread_position == 0:
                if self._pertb_list[-1] < 0:
                    self._spread_position = 1
                    self._signal = -1 # Long the spread 
                    logging.info(f"Long the spread for {self._asset1}-{self._asset2} pair.")
                elif self._pertb_list[-1] > 1:
                    self._spread_position = -1
                    self._signal = 1 # Short the spread
                    logging.info(f"Short the spread for {self._asset1}-{self._asset2} pair")
            elif self._spread_position == 1:
                if (self._pertb_list[0] < 0.5) and  (self._pertb_list[-1] >= 0.5):
                    self._spread_position = 0
                    self._signal = 0 # Close Position
                    logging.info(f"Close Position for {self._asset1}-{self._asset2} pair")
                else:
                    pass
            elif self._spread_position == -1:
                if (self._pertb_list[0] > 0.5) and  (self._pertb_list[-1] <= 0.5): 
                    self._spread_position = 0
                    self._signal = 0 # Close Position
                    logging.info(f"Close Position for {self._asset1}-{self._asset2} pair")
                else:
                    pass

    async def _trader(self) -> None:
        while (self._asset1_max_position == None) or (self._asset2_max_position == None):
            await self._calculate_max_position()
            await asyncio.sleep(1)
        logging.info(f"Max Positions of {self._asset1} is {self._asset1_max_position} and {self._asset2} is {self._asset2_max_position}")

        while True:
            self._calculate_spread()
            self._calculate_pertb()
            self._generate_signal()

            if self._signal == None:
                logging.info(f"No signal generated for {self._asset1}-{self._asset2} pair, awaiting more data or waiting for the next opportunity.")
                await asyncio.sleep(self._downsample)
                continue 

            asset1_position = self._dataclient.get_position_by_symbol(self._asset1)
            asset2_position = self._dataclient.get_position_by_symbol(self._asset2)
            
            if self._signal == - 1 :
                asset1_order_qty = - self._asset1_max_position - asset1_position
                asset2_order_qty = self._asset2_max_position - asset2_position
            elif self._signal == 1:
                asset1_order_qty = self._asset1_max_position - asset1_position
                asset2_order_qty = - self._asset2_max_position - asset2_position
            elif self._signal == 0:
                asset1_order_qty = 0 - asset1_position
                asset2_order_qty = 0 - asset2_position

            Order_IDs = []
            if asset1_order_qty != 0:
                if (asset1_order_qty < 0):
                    side_asset1 = SIDE_SELL
                elif (asset1_order_qty > 0):
                    side_asset1 = SIDE_BUY
                try:
                    mid_price_asset1 = self._dataclient.get_last_mid_price(self._asset1)
                    price_1 = round(mid_price_asset1 / 0.01) * 0.01 
                    logging.info(f"Placing {side_asset1} order for {self._asset1}, Qty={asset1_order_qty}, Price={price_1}, Type={self._order_type}")
                    Order_asset1 = await self._ordermanager.insert_order(symbol= self._asset1, price=price_1, quantity= abs(asset1_order_qty), side=side_asset1,order_type= self._order_type)
                    if Order_asset1.success:
                        Order_IDs.append(Order_asset1.order_id)
                except Exception as e:
                    logging.error(f"Error placing order for {self._asset1}: Qty={asset1_order_qty}, Price={mid_price_asset1}, Error: {e}")             

            if asset2_order_qty != 0:
                if (asset2_order_qty < 0):
                    side_asset2 = SIDE_SELL
                elif (asset2_order_qty > 0):
                    side_asset2 = SIDE_BUY
                try:
                    mid_price_asset2 = self._dataclient.get_last_mid_price(self._asset2)
                    price_2 = round(mid_price_asset2 / 0.01) * 0.01
                    logging.info(f"Placing {side_asset2} order for {self._asset2}, Qty={asset2_order_qty}, Price={price_2}, Type={self._order_type}")
                    Order_asset2 = await self._ordermanager.insert_order(symbol= self._asset2,  price=price_2,  quantity= abs(asset2_order_qty),  side=side_asset2, order_type= self._order_type)
                    if Order_asset2.success:
                        Order_IDs.append(Order_asset2.order_id)
                except Exception as e:
                    logging.error(f"Error placing order for {self._asset2}: Qty={asset2_order_qty}, Price={mid_price_asset2}, Error: {e}")  
            
            await asyncio.sleep(self._downsample)

            if self._order_type == ORDER_TYPE_GTC:
                for order in Order_IDs:
                    try:
                        logging.info(f"Cancelled GTC order {order}")
                        await self._ordermanager.cancel_order(order)
                    except Exception as e:
                        logging.error(f"Error cancelling take-profit order {order}: {e}")

##TODO Ideas for execution 1. GTC + Cancel Order 2. Submit order every 1s until target volume when there is signal for trade