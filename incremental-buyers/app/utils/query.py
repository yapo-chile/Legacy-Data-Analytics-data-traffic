from infraestructure.conf import getConf
from utils.read_params import ReadParams

class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def delete_records_users(self) -> str:
        query = """
            delete from ods.user 
            where insert_date::date = '{date_from}'::date;
        """.format(date_from= self.params.get_date_from())

    def get_ad_reply_stg(self) -> str:
        query = """
            SELECT  ad.buyer_id_nk,
                    ad.email,
                    coalesce(b.buyer_id_pk, 0) as buyer_id_pk_aux,
                    ad.buyer_creation_date ,
                    ad.insert_date
            FROM (  SELECT sender_email as buyer_id_nk,
                        sender_email as email,
                        min(added_at) as buyer_creation_date ,
                        now() as insert_date
                    FROM stg.ad_reply , ods.ad
                    WHERE list_id = ad.list_id_nk
                    GROUP BY sender_email) ad
            LEFT JOIN ods.buyer b on ad.email = b.buyer_id_nk;"""
        return query

    def get_buyers_ods(self) -> str:
        query = """
            SELECT  buyer_id_nk,
                    email,
                    buyer_creation_date,
                    buyer_creation_date as user_creation_date,
                    now() as insert_date,
                    now() as update_date,
                    user_id_pk,
                    user_creation_date_aux
            FROM ods.buyer bu
            left join (
                select user_creation_date as user_creation_date_aux,
                    user_id_pk,
                    user_id_nk
                from ods.user
                where seller_creation_date is not null
            ) us2 on us2.user_id_nk = bu.buyer_id_nk
            where 1=1
            and buyer_creation_date >= '{date_from}'
            order by buyer_creation_date
            """.format(date_from=self.params.get_date_from())
        return query

    def get_sellers_ods(self) -> str:
        query = """
            SELECT  seller_id_nk,
                    email,
                    seller_creation_date,
                    seller_creation_date as user_creation_date,
                    first_approval_date,
                    u2.user_creation_date_aux,
                    now() as insert_date,
                    now() as update_date,
                    u2.user_id_pk,
                    u2.user_creation_date_aux
            FROM ods.seller
            left join (
                select user_creation_date as user_creation_date_aux,
                    user_id_pk,
                    user_id_nk
                from ods.user
                where buyer_creation_date is not null
                ) u2 on u2.user_id_nk = seller_id_nk
            where seller_creation_date >= '{date_from}'
            order by seller_creation_date
            """.format(date_from=self.params.get_date_from())
        return query

    def get_approval_of_sellers(self):
        query = """
        select se.first_approval_date, us.user_id_pk
        from ods.seller as se ,ods.user us
        where se.seller_id_nk = us.user_id_nk
            and us.seller_creation_date is not null
            and us.first_approval_date is null
            and se.first_approval_date is not null;
        """
        return query