import os
import logging
import ast
import datetime
import asyncio
from typing import Optional, List 
from core import DataClient, OrderManager, MarketClockCalendar, Client
from PairTrade import PairTrade
from find_coint_pairs_and_params import PairsTradeParamsCalculation

# Create a logger
today = datetime.datetime.today().date().strftime('%Y%m%d')
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the log level for the logger
file_handler = logging.FileHandler(f'logs/pairs_trade_log_{today}.txt')  # Log to file
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

async def market_open() -> None:
    marketclockcalendar = MarketClockCalendar()
    await marketclockcalendar.start()
    while True:
        is_open = await marketclockcalendar.is_open()
        if is_open:
            break
        else:
            logging.info("Market is not open yet. Checking again in 60 seconds.")
            await asyncio.sleep(60)

async def market_time_left() -> Optional[datetime.datetime]:
    marketclockcalendar = MarketClockCalendar()
    await marketclockcalendar.start()
    timeout = await marketclockcalendar.time_left_before_next_close()
    return timeout

async def calculate_params(symbols : List[str], lookback : int , downsample : int):
    pairsparams = PairsTradeParamsCalculation(symbols=symbols, date = None, lookback = lookback, downsample = downsample)  # Create an instance of the classll the async start method
    await pairsparams.main()
    await Client.close_session()

async def trader(cointPairsparams: Optional[List[dict]], total_capital: float, downsample: int, k: int):
    symbols = {symbol for pair in cointPairsparams for symbol in (pair["asset 1"], pair["asset 2"])} 
    capital_per_pair = round(total_capital / len(cointPairsparams))
    d = DataClient(symbols=symbols)
    o = OrderManager()
    await asyncio.sleep(5)  
    pair_trade_instances = []
    for pair in cointPairsparams:
        _pair_trade_instance = PairTrade(dataclient=d, ordermanager=o,asset1=pair['asset 1'],asset2=pair['asset 2'], capital=capital_per_pair, hedge_ratio=pair['hedge ratio'], const= pair['constant'] , downsample=downsample, k=k )
        pair_trade_instances.append(_pair_trade_instance._trader())
    asyncio.create_task(d.start())
    await o.start()
    await asyncio.sleep(2)  
    await o.cancel_all_orders()
    await o.close_all_positions()
    await asyncio.sleep(2)  
    time_left_before_close = await market_time_left()
    assert time_left_before_close is not None, "time_left_before_close is None"
    timeout = (time_left_before_close - datetime.timedelta(seconds=300)).total_seconds()
    try:
        logging.info("Start Trading")
        await asyncio.wait_for(asyncio.gather(*pair_trade_instances), timeout= timeout)
    except asyncio.TimeoutError:
        logging.info("Market closing in 5 mins. Cancel open orders and close positions")
        await o.cancel_all_orders()
        await o.close_all_positions()
        logging.info("Exit...")
        exit()
         
if __name__ == "__main__":
    Total_capital = 10000
    symbols = ["NVDA", "TSM", "AMD", "ASML", "QCOM", "INTC"]  ##TODO remove ASML, TSM
    lookback = 2
    downsample = 30
    k = 2
    RUN_PARAMS_CALCULATOR = False
    today = datetime.datetime.today().date()
    data_folder = "data/"
    params_folder = "params/"
    paramsFilename = f"params_{today.strftime('%Y%m%d')}_ds{downsample}.txt"
    #paramsFilename = f"params_20241004_ds{downsample}.txt"
    logging.info("Main script has started.")
    if os.path.exists(params_folder + paramsFilename):
        logging.info(f"The file {params_folder + paramsFilename} exists.")
    else:
        logging.info(f"The file {params_folder + paramsFilename} does not exist.")
        
        if RUN_PARAMS_CALCULATOR:
            logging.info("Search for co-integrated pairs and calculate parameters")
            loop = asyncio.get_event_loop()
            try:
               loop.run_until_complete(calculate_params(symbols=symbols, lookback=lookback, downsample=downsample))
            except KeyboardInterrupt:
                logging.info('Stopped (KeyboardInterrupt)')
            finally:
              loop.run_until_complete(Client.close_session()) 
        else:
            logging.info("Exit...")
            exit()
    # Load parameters
    cointPairsparams = []
    try:
        with open(params_folder + paramsFilename, 'r') as file:
            for line in file:
                line = line.strip()
                line = ast.literal_eval(line)
                cointPairsparams.append(line)
                logging.info(f"{line['asset 1']}-{line['asset 2']} Pair. Hedge ratio: {line['hedge ratio']}. Half Life: {line['half life']}. Const: {line['constant']}")
            if len(cointPairsparams) == 0:
                logging.info(f"No co-integrated pairs to trade today ({today}). Exit...")
                exit()
    except Exception as e:
        logging.warning(f"Cannot load {paramsFilename}! Error: {e}")
        logging.info("Exit...")
        exit()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(market_open()) 
        loop.run_until_complete(trader(cointPairsparams=cointPairsparams, total_capital=Total_capital, downsample=downsample, k=k)) 
    except KeyboardInterrupt:
        logging.info('Stopped (KeyboardInterrupt)')
    finally:
        loop.run_until_complete(Client.close_session()) 