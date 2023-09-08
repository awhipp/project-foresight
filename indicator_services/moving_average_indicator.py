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
    def __init__(self, instrument: str, timescale: str):
        super().__init__(
            component_name='moving_average',
            instrument=instrument,
            timescale=timescale
        )

    def do_work(self):
        '''
        Calculates 20 and 50 period moving averages.'''
        data = self.pull_from_queue()
        if data is None:
            return
        data = data[data['ask'].notnull()]
        data = data[data['bid'].notnull()]
        if data is not None:
            # Calculate the moving averages\
            results = {
                "instrument": data['instrument'].iloc[0],
                # time UTC
                "time": datetime.datetime.now(),
                "ask": data['ask'].mean(),
                "bid": data['bid'].mean()
            }
            logger.info(results)
            return results

if __name__ == "__main__":
    maInd = MovingAverageIndicator(instrument='EUR_USD', timescale='1T')
    maInd.schedule_work()
