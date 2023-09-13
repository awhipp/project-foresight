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

class InstrumentPricingIndicator(Indicator):
    '''Instrument Pricing Indicator
    
    Parameters:
        instrument (str): The instrument to fetch
        timescale (str): The timescale to fetch (M = minutes, H = hours, D = days)
    '''
    def __init__(self, instrument: str, timescale: str):
        super().__init__(
            component_name='instrument_pricing',
            instrument=instrument,
            timescale=timescale
        )

    def value(self, data: pd.DataFrame) -> pd.DataFrame:
        ''''Calculate the all price data for the instrument as a list of json objects'''
        return data[data['price'].notnull()]

    def do_work(self) -> pd.DataFrame:
        '''Calculates all price data for the instrument'''
        data = self.pull_from_queue()
        if data is None:
            return
        
        results = self.value(data)

        logger.info(f"Added {len(results)} records to {self.component_name} indicator table.")
        return results

if __name__ == "__main__":
    maInd = InstrumentPricingIndicator(instrument='EUR_USD', timescale='M')
    maInd.schedule_work()
