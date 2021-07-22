# pylint: disable=no-member
import environ

INI_BLOCKET = environ.secrets.INISecrets.from_path_in_env("APP_BLOCKET_SECRET")
SURVEY_SECRET = environ.secrets.INISecrets.from_path_in_env("APP_SURVEY_SECRET")

@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="BLOCKET")
    class BlocketConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_BLOCKET.secret(name="host", default=environ.var())
        port: int = INI_BLOCKET.secret(name="port", default=environ.var())
        name: str = INI_BLOCKET.secret(name="dbname", default=environ.var())
        user: str = INI_BLOCKET.secret(name="user", default=environ.var())
        password: str = INI_BLOCKET.secret(name="password", default=environ.var())
    
    @environ.config(prefix="SURVEY")
    class SurveypalConfig:
        """
        SurveypalConfig Class representing the configuration to access surveypal api
        """
        api_key: str = SURVEY_SECRET.secret(name="authorization", default=environ.var())
        survey_id: str = SURVEY_SECRET.secret(name="survey_id", default=environ.var())

    db = environ.group(BlocketConfig)
    survey = environ.group(SurveypalConfig)

def getConf():
    return environ.to_config(AppConfig)
