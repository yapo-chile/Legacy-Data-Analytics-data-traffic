# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class OdsUserBuyer():
    def __init__(self,
                 config,
                 params: ReadParams) -> None:
        self.config = config
        self.params = params
        self.path_1 = None
        self.path_2 = None
        self.log = logging.getLogger('UserBuyer')

    def delete_users_of_day(self):
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.excecute_command(
            query.delete_records_users()
        )
        db.close_connection()

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
    def data_buyers_ods(self):
        return self.__data_buyers_ods

    @data_buyers_ods.setter
    def data_buyers_ods(self, config):
        db_source = Database(conf=config)
        data_dwh = pd.read_sql(
            Query(config, self.params).get_buyers_ods(),
            db_source.get_sqlalchemy_conn())
        db_source.close_connection()
        self.__data_buyers_ods = data_dwh

    def generate(self):
        self.log.info('Deleting users for day {}'.format(
            self.params.get_date_from()
        ))
        self.delete_users_of_day()

        self.log.info('Starting ods_user_buyer step')
        self.data_buyers_ods = self.config.db
        self.path_1 = self.data_buyers_ods[
            self.data_buyers_ods['user_id_pk'] == None
        ]
        self.path_1 = self.path_1[[
            'buyer_id_nk',
            'email',
            'buyer_creation_date',
            'user_creation_date',
            'insert_date',			
            'update_date']]
        self.path_1.rename(
            columns={'buyer_id_nk': 'user_id_nk'},
            inplace=True)

        self.save_new_users()
        self.path_2 = self.data_buyers_ods[
            self.data_buyers_ods['user_id_pk'] != None
        ]
        self.path_2['user_creation_date'] = self.path_2.apply(
            lambda x: x['buyer_creation_date'] if 
            x['buyer_creation_date'] <= x['user_creation_date_aux']
            else x['user_creation_date_aux']
        )
        self.path_2 = self.path_2[[
            'user_id_pk',
            'user_creation_date',
            'buyer_creation_date',
            'update_date']]
        self.update_users()
        self.log.info('Ending ods_user_buyer step')
