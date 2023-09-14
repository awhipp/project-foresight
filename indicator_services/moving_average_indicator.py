from indicator import Indicator

import pandas as pd

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

    def value(self, data: pd.DataFrame) -> dict:
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

        if ma_10 > ma_20:
            value += 0.25
        else:
            value -= 0.25
        if ma_20 > ma_30:
            value += 0.25
        else:
            value -= 0.25
        if ma_30 > ma_40:
            value += 0.25
        else:
            value -= 0.25
        if ma_40 > ma_50:
            value += 0.25
        else:
            value -= 0.25
        return {
            '10-minute': round(ma_10, 5),
            '20-minute': round(ma_20, 5),
            '30-minute': round(ma_30, 5),
            '40-minute': round(ma_40, 5),
            '50-minute': round(ma_50, 5),
            'value': value
        }

    def do_work(self) -> dict:
        '''Calculates bullishness or bearishness based on moving averages.'''
        data = self.pull_from_queue()
        if data is None:
            return
        
        results = self.value(data)

        logger.info(results)
        return results

if __name__ == "__main__":
    maInd = MovingAverageIndicator(instrument='EUR_USD', timescale='M')
    maInd.schedule_work()
