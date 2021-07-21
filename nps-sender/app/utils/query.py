from infraestructure.conf import getConf
from utils.read_params import ReadParams


class DwhQuery:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def select_distint_emails(self) -> str:
        """
        Method return str with query
        """
        query = """select 
                distinct email 
            from ods.ad a
            left join (select distinct seller_id_fk from  ods.ad a 
                        where reason_removed_detail_id_fk=1 and deletion_date::date between now()::date - INTERVAL '1 MONTH' and now()::date - INTERVAL '2 DAY' ) sh 
                        on sh.seller_id_fk=a.seller_id_fk
            left join ods.seller se on se.seller_id_pk=a.seller_id_fk
            where reason_removed_detail_id_fk=1 and deletion_date between now()::date-interval '1 DAY' and now()
            and sh.seller_id_fk is null and email is not null"""
        return query
