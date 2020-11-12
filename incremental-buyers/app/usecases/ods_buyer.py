# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class OdsBuyer():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.log = logging.getLogger('Buyer')

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.insert_copy(self.data_buyers, 'ods', 'buyer')
        db.close_connection()

    # Query data from data warehouse
    @property
    def data_buyers(self):
        return self.__data_buyers

    @data_buyers.setter
    def data_buyers(self, config):
        db_source = Database(conf=config)
        data_buyers_ = db_source.select_to_dict(
            Query(config, self.params).get_ad_reply_stg())
        data_buyers_ = data_buyers_[data_buyers_.buyer_id_pk_aux == 0]
        data_buyers_.drop(
            ['buyer_id_pk_aux'],
            axis=1,
            inplace=True)
        self.__data_buyers = data_buyers_
        db_source.close_connection()

    def generate(self):
        self.log.info('Starting ods_buyer step')
        self.data_buyers = self.config.db
        self.save()
        self.log.info('Ending ods_buyer step')
