# pylint: disable=no-member
# utf-8
import requests
from infraestructure.psql import Database
from utils.query import DwhQuery
from utils.read_params import ReadParams


class NpsSender(DwhQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def dwh_data_emails(self):
        return self.__dwh_data_emails

    @dwh_data_emails.setter
    def dwh_data_emails(self, config):
        db_source = Database(conf=config)
        dwh_data_emails = db_source.select_to_dict(self.select_distint_emails())
        db_source.close_connection()
        self.__dwh_data_emails = dwh_data_emails

    def send_emails(self, x):
        survey_id = self.config.survey.survey_id
        link = "https://my.surveypal.com/api/rest/survey/"+survey_id+"/answer/email/invite?email="+x['email']+"&subject=Nos%20gustar%C3%ADa%20saber%20tu%20experiencia%20al%20vender%20en%20Yapo.cl"
        headers = {"X-Auth-Token": self.config.survey.api_key, "Accept": "application/json"}
        requests.put(link, headers=headers)
        return x

    def generate(self):
        self.dwh_data_emails = self.config.db
        self.logger.info("First records as evidence")
        self.logger.info(self.dwh_data_emails.head())

        self.dwh_data_emails.apply(self.send_emails, axis=1)
        self.logger.info("Survey was saved successfully")
        return True
