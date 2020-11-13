# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class OdsUserSeller():
    def __init__(self, config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.path_1 = None
        self.path_2 = None
        self.log = logging.getLogger('UserSeller')

    def save_new_users(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.insert_copy('ods','user',self.path_1)
        db.close_connection()

    def update_users(self) -> None:
        db = Database(conf=self.config.db)
        db.update_table(
            df=self.path_2,
            schema='ods',
            table='user',
            union_key='user_id_pk')
        db.close_connection()

    @property
    def data_sellers_ods(self):
        return self.__data_sellers_ods

    @data_sellers_ods.setter
    def data_sellers_ods(self, config):
        db_source = Database(conf=config)
        data_dwh = pd.read_sql(
            Query(config, self.params).get_sellers_ods(),
            db_source.get_sqlalchemy_conn())
        db_source.close_connection()
        self.__data_sellers_ods = data_dwh

    @property
    def data_aproval_sellers(self):
        return self.__data_aproval_sellers

    @data_aproval_sellers.setter
    def data_aproval_sellers(self, config):
        db_source = Database(conf=config)
        data_dwh = pd.read_sql(
            Query(config, self.params).get_approval_of_sellers(),
            db_source.get_sqlalchemy_conn())
        db_source.close_connection()
        self.__data_aproval_sellers = data_dwh

    def update_users_aproval_date(self):
        db = Database(conf=self.config.db)
        db.update_table(self.data_aproval_sellers, 'ods', 'user')
        db.close_connection()

    def generate(self):
        self.log.info('Starting ods_user_seller step')
        self.data_sellers_ods = self.config.db
        self.path_1 = self.data_sellers_ods[
            self.data_sellers_ods['user_id_pk'] == None
        ]
        self.path_1 = self.path_1[[
            'seller_id_nk',
            'email',
            'user_creation_date',
            'seller_creation_date',
            'first_approval_date',
            'insert_date',
            'update_date']]
        self.path_1.rename(
            columns={'seller_id_nk': 'user_id_nk'},
            inplace=True)
        self.save_new_users()

        self.path_2 = self.data_sellers_ods[
            self.data_sellers_ods['user_id_pk'] != None
        ]
        self.path_2['user_creation_date'] = self.path_2.apply(
            lambda x: x['seller_creation_date'] 
                if x['seller_creation_date'] <= x['user_creation_date_aux']
                else x['user_creation_date_aux'],
            axis=1
        )
        self.path_2['first_approval_date'] = self.\
            path_2['first_approval_date'].apply(
            lambda x: x if x is not None else '1900-01-01 00:00:00'
        )
        self.path_2.rename(
            columns={'seller_id_nk': 'user_id_nk'},
            inplace=True)

        self.path_2 = self.path_2[[
            'user_id_pk',
            'user_creation_date',
            'seller_creation_date',
            'update_date',
            'first_approval_date'
        ]]            
        self.update_users()

        self.log.info('Ending ods_user_seller step')

        self.data_aproval_sellers = self.config.db
        self.update_users_aproval_date()
