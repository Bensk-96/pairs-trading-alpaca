import os
import json
import datetime
import asyncio
import logging
import numpy as np
import pandas as pd
import statsmodels.api as sm 
from typing import Optional, List , Tuple
from itertools import combinations 
from core import MarketClockCalendar, Client, Credentials
from alpaca_trade_api.rest import REST


class PairsTradeParamsCalculation():

    def __init__(self, 
                 symbols : List[str] = None,
                 date : str = None, 
                 lookback : int = None,
                 downsample : int = None):     
        assert len(symbols) > 1, "Must have at least 1 symbol"
        self._symbols : Optional[List[str]] = symbols
        self._date : str = datetime.datetime.strptime(date, '%Y-%m-%d').date() if date else datetime.datetime.today().date()
        self._lookback : int = lookback
        self._downsample : int = downsample
        self._two_week_ago : str = self._date - datetime.timedelta(days=14)
        self._market_open : Optional[bool] = None
        self._formation_days : Optional[List[str]] = None
        self._parameters_calculated : Optional[bool] = None
        self._pairs_parameters : Optional[List[dict]] = None
        self._marketclockcalendar : MarketClockCalendar = MarketClockCalendar()
        self._market_calendar : pd.DataFrame = None
        self._data_folder_name = "data"
        self._params_folder_name = "params"
        self._api = REST(Credentials.KEY_ID(), Credentials.SECRET_KEY(), base_url='https://data.alpaca.markets/v2')
        self._data_coverage : Optional[bool] = None
        self._pairs : Optional[list] = None
        self._cointPairsParams : Optional[List[dict]] = None
        self._cointPairsParams_no_repeat : Optional[List[dict]] = None
        self._paramsFilename : Optional[str] = None

    async def start(self) -> None:
        await self._marketclockcalendar.start()

    async def get_market_calendar(self) -> None:

        market_calendar = await self._marketclockcalendar.get_market_calendar_info(start=self._two_week_ago, end=self._date)
        if market_calendar:
            df_market_calendar = pd.DataFrame(market_calendar)
        self._market_calendar = df_market_calendar

    def check_market_open(self) -> None: 
        if str(self._date) in self._market_calendar["date"].unique():
            self._market_open = True
        else:
            self._market_open = False      
    
    def get_formation_days(self) -> None: 
        if self._market_open:
            self._formation_days = self._market_calendar["date"][- self._lookback - 1: - 1].tolist()
        else:
            #logging.warning(f"Market not open on {self._date}")
            pass

    def fetch_data(self) -> None:
        assert self._market_open, f"Market not open on {self._date}"
        assert self._formation_days is not None, "No lookback days to download data"
        df_market_hours = self._market_calendar.copy()
        df_market_hours["market open"] = df_market_hours["date"] + "T" + df_market_hours["open"] + ":00Z"
        df_market_hours["market close"] = df_market_hours["date"] + "T" + df_market_hours["close"] + ":00Z"
        df_market_hours = df_market_hours[["date", "market open", "market close"]]
        df_market_hours.set_index("date", inplace= True)
        for symbol in self._symbols :
            for formation_date in self._formation_days:
                df_market_hours_by_formation_date = df_market_hours[df_market_hours.index == formation_date]
                date = df_market_hours_by_formation_date.index[0]
                market_open = df_market_hours_by_formation_date["market open"][0]
                market_close = df_market_hours_by_formation_date["market close"][0]

                if isinstance(date, str):
                    date = datetime.datetime.strptime(date, '%Y-%m-%d')
                csv_filename = os.path.join(self._data_folder_name, f"{symbol}_{date.strftime('%Y%m%d')}_quote.csv")

                if os.path.exists(csv_filename):
                    logging.info(f"Data for {symbol} on {date.strftime('%Y-%m-%d')} already exists. Skipping download.")
                    continue
                
                logging.info(f"Start downloading quote data for {symbol} on {date.strftime('%Y-%m-%d')}...")
                quote_data = self._api.get_quotes(symbol=symbol, start=market_open, end=market_close, limit=3000000).df  
                logging.info(f"Finished downloading quote data for {symbol} on {date.strftime('%Y-%m-%d')}")

                quote_data.to_csv(csv_filename, index=True)
                logging.info(f"Saved data to {csv_filename}")
                print("--------------------")
        
        #logging.info("Check data coverage")
        #all_files = os.listdir(self._data_folder_name)
        #csv_files = [f for f in all_files if f.endswith('.csv')]

        self._data_coverage = True
        # for csv in csv_files:
        #     df = pd.read_csv(f"{self._data_folder_name}/"   + csv)
        #     num_rows = len(df)
        #     # Check if the number of rows is exactly 2,000,001          
        #     if num_rows == 3000000:
        #         self._data_coverage = False
        #         logging.warning(f"{csv} does not have all quotes!")
        # if self._data_coverage:
        #     logging.info("Data coverge is OK")
        # else:
        #     logging.warning("Data coverge is not OK!")

    def get_unique_pairs(self) -> None:
        self._pairs = list(combinations(self._symbols, 2))

    def load_quote_data(self, symbol: str, date: str) -> pd.DataFrame:
        csv_name = f"{symbol}_{date}_quote.csv"
        quote_data = pd.read_csv(f"{self._data_folder_name}/{csv_name}", sep=",")
        
        # Convert timestamp to datetime, handling potential errors
        try:
            quote_data['timestamp'] = pd.to_datetime(quote_data['timestamp']) 
        except ValueError as e:
            print(f"Error parsing timestamps for {symbol}: {e}")
            # Optionally use coerce to handle this
            quote_data['timestamp'] = pd.to_datetime(quote_data['timestamp'], errors='coerce')
        
        quote_data.set_index('timestamp', inplace=True)
        return quote_data

    def calculate_midprice_and_downsample(self, quote_data : pd.DataFrame , downsample : int) -> pd.Series:
        quote_data["mid_price"] = np.where(
            (quote_data["ask_price"] > 0) & (quote_data["bid_price"] > 0),
            (quote_data["ask_price"] + quote_data["bid_price"]) * 0.5,
            np.nan
        )
        downsampled_mid_price = quote_data["mid_price"].resample(f'{downsample}S').last()
        downsampled_mid_price = downsampled_mid_price.fillna(method='ffill')
        return downsampled_mid_price
    
    def calculate_half_life(self, spread : np.ndarray) -> float:
        # Ensure spread is a Pandas Series for easy manipulation
        df_spread = pd.DataFrame(spread, columns=["spread"])     
        # Calculate lagged spread and spread returns
        spread_lag = df_spread.spread.shift(1).fillna(method="bfill")  # Backfill the first NaN
        spread_ret = df_spread.spread - spread_lag
        # Add a constant to the lagged spread
        spread_lag2 = sm.add_constant(spread_lag)    
        # Fit the OLS model
        model = sm.OLS(spread_ret, spread_lag2)
        res = model.fit()
        # Calculate the half-life
        halflife = -np.log(2) / res.params[1]     
        return round(halflife, 0)

    def cointegration_check_weighted(self, price_data: pd.DataFrame, asset1: str, asset2: str) -> Tuple[bool, Optional[dict]]:
        res_mean = 0 
        res_std = 0
        rsquared_adj = 0
        const = 0
        hedge_ratio = 0 
        half_life = 0
        p_value_adf_test = 0
        adjustment_coefficient = 0
        length = len(price_data)

        price_data["date"] = price_data.index.date
        unique_date = price_data["date"].unique()

        for date in unique_date:

            price_data_by_date = price_data[price_data.date == date]
            weight = len(price_data_by_date) / length
        
            # Step 1: Perform OLS regression
            model1 = sm.OLS(price_data_by_date[asset2], sm.add_constant(price_data_by_date[asset1])).fit()
            res_by_date = model1.resid  # Note the change to model1.resid

            # Calculate the mean and std of the residual
            res_mean_by_date = np.mean(res_by_date)
            res_std_by_date= np.std(res_by_date)
            rsquared_adj_by_date = model1.rsquared_adj
            const_by_date = model1.params[0]
            hedge_ratio_by_date = model1.params[1]
            half_life_by_date = self.calculate_half_life(np.array(res_by_date))   

            # Step 2: ADF test on residuals
            adf_test_result_by_date = sm.tsa.stattools.adfuller(res_by_date)
            p_value_adf_test_by_date = adf_test_result_by_date[1]   

            # Step 3: Error Correction Model (ECM)
            # Include lagged residuals and the difference of asset1 as regressors
            asset1_diff = sm.add_constant(pd.concat([price_data_by_date[asset1].diff(), res_by_date.shift(1)], axis=1).dropna())
            asset2_diff = price_data_by_date[asset2].diff().dropna()

            asset1_diff = asset1_diff.loc[asset2_diff.index]
            model2 = sm.OLS(asset2_diff, asset1_diff).fit()

            # Step 4: Check if the adjustment coefficient is negative
            adjustment_coefficient_by_date = list(model2.params)[-1]

            res_mean += res_mean_by_date * weight 
            res_std += res_std_by_date * weight 
            rsquared_adj += rsquared_adj_by_date * weight
            const += const_by_date * weight
            hedge_ratio += hedge_ratio_by_date * weight
            half_life += half_life_by_date * weight

            p_value_adf_test = max(p_value_adf_test_by_date, p_value_adf_test)
            adjustment_coefficient += adjustment_coefficient_by_date * weight

        coint_result = {"asset 1": asset1,
                        "asset 2": asset2,
                        "res mean":res_mean ,
                        "res std":res_std,
                        "adj rsquared": rsquared_adj,
                        "constant" : const,
                        "hedge ratio": hedge_ratio,
                        "half life": half_life,
                        "p-value of adf test" : p_value_adf_test,
                        "adjustment coef":adjustment_coefficient}
        if (p_value_adf_test < 0.05 ) and (adjustment_coefficient < 0):
            return True, coint_result
        else:
            return False, None
        
    def calculate_pairsParams(self) -> None:
        logging.info("Searching co-integrated pairs...")
        df_mid_price = pd.DataFrame({})
        for formation_day in self._formation_days: 
            formation_day = datetime.datetime.strptime(formation_day, '%Y-%m-%d')
            formation_day = formation_day.strftime('%Y%m%d')
            midprice_by_formation_day = pd.DataFrame()
            for symbol in self._symbols:
                quote_data_by_symbol = self.load_quote_data(symbol= symbol, date=formation_day)
                downsampled_mid_price_by_symbol = self.calculate_midprice_and_downsample(quote_data = quote_data_by_symbol, downsample = self._downsample)
                midprice_by_formation_day[symbol] = downsampled_mid_price_by_symbol
            df_mid_price = pd.concat([df_mid_price, midprice_by_formation_day])
            df_mid_price = df_mid_price.fillna(method='ffill')
            df_mid_price = df_mid_price.fillna(method='bfill')
        coint_pair_and_params_by_day = []
        for pair in self._pairs:
            coint_result = self.cointegration_check_weighted(df_mid_price, pair[0], pair[1]) 
            if coint_result[0]: 
                coint_pair_and_params_by_day.append(coint_result[1])
        self._cointPairsParams = coint_pair_and_params_by_day
        #return {"Trading Day": self._date, "Coint Pairs and Params": coint_pair_and_params_by_day}

    def find_largest_non_repeating_pairs(self) -> None:
        logging.info("Remove pairs with repeating symbols")
        # Sort the pairs by half-life (or any other criteria) to prioritize better pairs
        sorted_pairs = sorted(self._cointPairsParams, key=lambda x: x['half life'])

        selected_pairs = []
        used_symbols = set()  # To track used symbols

        for pair in sorted_pairs:
            asset1 = pair['asset 1']
            asset2 = pair['asset 2']

            # If neither symbol has been used, select this pair
            if asset1 not in used_symbols and asset2 not in used_symbols:
                selected_pairs.append(pair)
                used_symbols.update([asset1, asset2])  # Mark symbols as used
        self._cointPairsParams_no_repeat = selected_pairs

    def save_cointPairsParams(self) -> None:
        cointPairsParams = self._cointPairsParams_no_repeat
        trading_day = self._date
        trading_day = trading_day.strftime('%Y%m%d')
        file_name = f"params_{trading_day}_ds{self._downsample}.txt"
        logging.info(f"saving parameters to {file_name}")
        with open(f"{self._params_folder_name}/{file_name}", 'w') as file:
            for entry in cointPairsParams:
                file.write(json.dumps(entry) + '\n')
        self._paramsFilename = file_name
        logging.info("Saved")

    def get_paramsFilename(self) -> Optional[str]:
        return self._paramsFilename

    async def main(self) -> None:
        #logging.info("Search for co-integrated pairs and calculate parameters")
        try:
            await self.start()
            await self.get_market_calendar()
            self.check_market_open()
            self.get_formation_days()
            self.fetch_data()
            self.get_unique_pairs()
            self.calculate_pairsParams()
            self.find_largest_non_repeating_pairs()
            self.save_cointPairsParams()
            logging.info("Parameter calculation successful!")
        except Exception as e:
            logging.warning(f"Parameter calculation failed! Error : {e}")
            




# async def main():
#     pairsparams = PairsTradeParamsCalculation(symbols=["SMH", "SOXX","NVDA", "TSM", "AMD", "ASML", "AMAT", "QCOM","INTC"], date = "2024-10-04", lookback = 2, downsample = 5)  # Create an instance of the classll the async start method
#     await pairsparams.main()
#     await Client.close_session()

# # Ensure this runs in a proper async context
# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     try:
#         loop.run_until_complete(main())
#     except RuntimeError as e:
#         print(f"Runtime error: {e}")
#     finally:
#         loop.run_until_complete(Client.close_session())
