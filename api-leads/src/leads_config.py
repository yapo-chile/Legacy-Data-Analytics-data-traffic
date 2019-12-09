import environ


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="XITI")
    class XitiConfig:
        """
        XitiConfig class represeting the configuration to access the
        Xiti service
        """
        authorization = environ.var()
        site_msite = environ.var("535499")
        site_android = environ.var("557231")
        site_ios = environ.var("557229")
        api_url_count = environ.var(
            "https://apirest.atinternet-solutions.com/data/v2/json/getRowCount")
        api_url_data = environ.var(
            "https://apirest.atinternet-solutions.com/data/v2/json/getData")
        sort = environ.var("{-m_visitors}")
        columns_msite = environ.var("{cl_142578,cl_142563,m_visitors}")
        columns_android = environ.var("{cl_351206,cl_351209,m_visitors}")
        columns_ios = environ.var("{cl_351192,cl_351195,m_visitors}")
        filter_msite = environ.var("{cl_142563:{$empty:false}}")
        filter_android = environ.var("{cl_351209:{$empty:false}}")
        filter_ios = environ.var("{cl_351195:{$empty:false}}")

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        name = environ.var("dw_blocketdb_ch")
        password = environ.var()
        user = environ.var("bnbiuser")
        host = environ.var("postgres")
    xiti = environ.group(XitiConfig)
    db = environ.group(DBConfig)
