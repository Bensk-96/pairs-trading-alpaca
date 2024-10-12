# Implementaion of pairs trading strategy on Alpaca Markets

- **core.py** : Trading infrastructure for acquiring real-time market data, order management, position monitoring and checking the market clock and calendar 

- **PairTrade.py** : Strategy logic
   - **Step 1** : Resample mid-prices of assets in a paris 
   - **Step 2** : Claculate spread of the assets
   - **Step 3** : Calculate Bollinger Bands and %b of the spread 
   - **Step 4** : 
     - Long the spread when %b < 0 or short the spread when %b > 1
     - close position when %b crosses 0.5 
     - Use IOC order for trading frequency = 5 seconds, and GTC order for lower frequencies

- **find_coint_pairs_and_params.py** : Search for co-integrated pairs and the corresponding parameters 

- **main.py** : Strategy configuration and execution
  - Confiugre total capital, trading frequency, instrucments(ETF and stocks), threshold for Bollinger Bands, lookback period for parameter calculation
  - Automatic start when market opens and stops 5 minutes beofre market close
  - Logging for monitoring strategy performance and execution 
