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
    def __init__(self, instrument: str, timescale: str, order_type: str):
        super().__init__(
            component_name='moving_average',
            instrument=instrument,
            timescale=timescale,
            order_type=order_type
        )

    def do_work(self) -> dict:
        '''Calculates bullishness or bearishness based on moving averages.'''
        data = pd.DataFrame(self.pricing)

        # Slow and fast moving averages on the price
        fast = 2
        slow = 5

        data['ma_fast'] = data['price'].rolling(window=fast).mean()
        data['ma_slow'] = data['price'].rolling(window=slow).mean()

        # Drop nulls
        data = data.dropna()

        return data.to_dict('records')

if __name__ == "__main__":
    maInd = MovingAverageIndicator(instrument='EUR_USD', timescale='M', order_type='mid')
    maInd.schedule_work()
