# utf-8
import sys
import logging
from infraestructure.conf import getConf
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution
from usecases.ods_buyer import OdsBuyer
from usecases.ods_user_buyer import OdsUserBuyer
from usecases.ods_user_seller import OdsUserSeller


if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('incremental-buyers')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    TIME.get_time()
    # Calling main process
    OdsBuyer(CONFIG, PARAMS).generate()
    OdsUserBuyer(CONFIG, PARAMS).generate()
    OdsUserSeller(CONFIG, PARAMS).generate()
    # End process
    LOGGER.info('Process ended successfully.')
