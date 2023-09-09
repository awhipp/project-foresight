from indicator import Indicator

import pandas as pd
import datetime

# Setup logging and log timestamp prepend
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', 
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class MovingAverageIndicator(Indicator):
    '''Moving Average Indicator
    
    Parameters:
        instrument (str): The instrument to fetch
        timescale (str): The timescale to fetch (M = minutes, H = hours, D = days)
    '''
    def __init__(self, instrument: str, timescale: str):
        super().__init__(
            component_name='moving_average',
            instrument=instrument,
            timescale=timescale
        )

    def value(self, data: pd.DataFrame) -> float:
        '''Calculate how bullish or bearish something is based on 10-ma, 20-ma, 50-ma.'''
        # Calculate the moving averages
        data = data[data['price'].notnull()]
        ma_10 = data['price'].rolling(10).mean().iloc[-1]
        ma_20 = data['price'].rolling(20).mean().iloc[-1]
        ma_30 = data['price'].rolling(30).mean().iloc[-1]
        ma_40 = data['price'].rolling(40).mean().iloc[-1]
        ma_50 = data['price'].rolling(50).mean().iloc[-1]

        # Calculate the value
        value = 0 # Neutral
        if ma_10 > ma_20 > ma_30 > ma_40 > ma_50:
            value = 1 # Bullish
        elif ma_10 < ma_20 < ma_30 < ma_40 < ma_50:
            value = -1 # Bearish
        else:
            value = 0 # Neutral
        return value
        

    def do_work(self):
        '''Calculates bullishness or bearishness based on moving averages.'''
        data = self.pull_from_queue()
        if data is None:
            return
        
        results = {
            "time": data['time'].max(),
            "latest_price": data['price'].iloc[0],
            "value": self.value(data)
        }

        logger.info(results)
        return results

if __name__ == "__main__":
    maInd = MovingAverageIndicator(instrument='EUR_USD', timescale='M')
    maInd.schedule_work()
