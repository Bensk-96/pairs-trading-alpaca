import asyncio
import logging 
import numpy as np
from typing import Optional 
from collections import deque
from core import DataClient, OrderManager, Client
from core import ORDER_TYPE_IOC, SIDE_BUY, SIDE_SELL

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
        self._perb_list : deque = deque(maxlen= 2)
        self._signal : Optional[int] = None 
    
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
        if len(self._spread_list) != self._length_of_spread:
            len_spread_list = len(self._spread_list)
            logging.info(f"length of spread list of {self._asset1}-{self._asset2} is {len_spread_list}. Required length is {self._length_of_spread}")
            return
        else:
            spread_rolling_mean = np.mean(self._spread_list)
            spread_rolling_std = np.std(self._spread_list)
            upper_band = spread_rolling_mean + self._k * spread_rolling_std
            lower_band = spread_rolling_mean - self._k * spread_rolling_std
            pertb = (self._spread_list[-1] - lower_band) / (upper_band - lower_band) ##TODO try backtest signal (self._spread_list[-1] - spread_rolling_mean) / (upper_band - lower_band) (z-score)
            self._perb_list.append(pertb)  
            #logging.info(f"rolling mean of {self._asset1}-{self._asset2} is {spread_rolling_mean}")
            #logging.info(f"rolling std of {self._asset1}-{self._asset2} is {spread_rolling_std}")
            #logging.info(f"upper band of {self._asset1}-{self._asset2} is {upper_band}")
            #logging.info(f"lower band of {self._asset1}-{self._asset2} is {lower_band}")
            logging.info(f"pertb of {self._asset1}-{self._asset2} is {pertb}")
            #logging.info(f"pertb list is {self._perb_list}")

    def _generate_signal(self) -> None:
        #len_pertb_list = len(self._perb_list)
        #logging.info(f"len of pertb list of {self._asset1}-{self._asset2} is {len_pertb_list}")
        if len(self._perb_list) < 2:
            #logging.info(f"Less than 2 values of pertb for {self._asset1}-{self._asset2}")
            return
        else:
            if self._spread_position == 0:
                if self._perb_list[-1] < 0:
                    self._spread_position = 1
                    self._signal = -1 # Long the spread 
                    logging.info(f"Long the spread for {self._asset1}-{self._asset2} pair")
                elif self._perb_list[-1] > 1:
                    self._spread_position = -1
                    self._signal = 1 # Short the spread
                    logging.info(f"Short the spread for {self._asset1}-{self._asset2} pair")
            elif self._spread_position == 1:
                if (self._perb_list[0] < 0.5) and  (self._perb_list[-1] >= 0.5):
                    self._spread_position = 0
                    self._signal = 0 # Close Position
                    logging.info(f"Close Position for {self._asset1}-{self._asset2} pair")
                else:
                    pass
            elif self._spread_position == -1:
                if (self._perb_list[0] > 0.5) and  (self._perb_list[-1] <= 0.5): 
                    self._spread_position = 0
                    self._signal = 0 # Close Position
                    logging.info(f"Close Position for {self._asset1}-{self._asset2} pair")
                else:
                    pass

    async def _trader(self) -> None:
        while (self._asset1_max_position == None) or (self._asset2_max_position == None):
            await self._calculate_max_position()
            await asyncio.sleep(1)
        logging.info(f"Desired Positions of {self._asset1} is {self._asset1_max_position} and {self._asset2} is {self._asset2_max_position}")

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
            mid_price_asset1 = round(self._dataclient.get_last_mid_price(self._asset1), 2)
            mid_price_asset2 = round(self._dataclient.get_last_mid_price(self._asset2), 2)

            if self._signal == - 1 :
                asset1_order_qty = - self._asset1_max_position - asset1_position
                asset2_order_qty = self._asset2_max_position - asset2_position
            elif self._signal == 1:
                asset1_order_qty = self._asset1_max_position - asset1_position
                asset2_order_qty = - self._asset2_max_position - asset2_position
            elif self._signal == 0:
                asset1_order_qty = 0 - asset1_position
                asset2_order_qty = 0 - asset2_position

            if asset1_order_qty != 0:
                if (asset1_order_qty < 0):
                    side_asset1 = SIDE_SELL
                elif (asset1_order_qty > 0):
                    side_asset1 = SIDE_BUY
                try:
                    Order_asset1 = await self._ordermanager.insert_order(symbol= self._asset1, price=mid_price_asset1, quantity= abs(asset1_order_qty), side=side_asset1,order_type= ORDER_TYPE_IOC)
                except Exception as e:
                    logging.error(f"Error placing order for {self._asset1}: Qty={asset1_order_qty}, Price={mid_price_asset1}, Error: {e}")             

            if asset2_order_qty != 0:
                if (asset2_order_qty < 0):
                    side_asset2 = SIDE_SELL
                elif (asset2_order_qty > 0):
                    side_asset2 = SIDE_BUY
                try:
                    Order_asset2 = await self._ordermanager.insert_order(symbol= self._asset2,  price=mid_price_asset2,  quantity= abs(asset2_order_qty),  side=side_asset2, order_type= ORDER_TYPE_IOC)
                except Exception as e:
                    logging.error(f"Error placing order for {self._asset2}: Qty={asset2_order_qty}, Price={mid_price_asset2}, Error: {e}")  
            
            await asyncio.sleep(self._downsample)

##TODO Ideas for execution 1. GTC + Cancel Order 2. Submit order every 1s until target volume when there is signal for trade