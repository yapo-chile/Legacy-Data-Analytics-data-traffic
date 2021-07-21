# pylint: disable=no-member
# utf-8
import logging
from infraestructure.psql import Database
from utils.read_params import ReadParams
from usecases.sender import NpsSender


class Process():
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    def generate(self):
        self.nps_sender = NpsSender(self.config,
                                    self.params,
                                    self.logger).generate()
