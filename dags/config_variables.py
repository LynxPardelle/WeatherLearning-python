from dotenv import load_dotenv, dotenv_values
from airflow.models import Variable
import os
load_dotenv()
config = dotenv_values(".env")
dag_path = os.getcwd()

""" 
    You can change this defaults to your own values, and then you can use the function configDB() to get the values from the .env file or the defaults.
"""
defaults: dict = {


    """ DB """


    "db_host": "localhost",
    "database": "db",
    "db_user": "user",
    "db_password": "password",
    "db_port": "5432",


    """ SMTP """


    "smtp": "smtp.gmail.com",
    "smtp_port": 587,
    "smtp_user": "cuenta_remitente@gmail.com",
    "smtp_password": "password",
    "smtp_from": "cuenta_remitente@gmail.com",
    "smtp_to": "cuenta_destinatario@gmail.com",


    """ API """


    "api_key": "api_key",
    "api_city_id": "242560",  # city_id for the weather API, by default is Mexico City

    """ Configs """


    "priorize_defaults": True,
    "thresholds": [
        {
            "property": "minimum_temperature",
            "evaluation": "<",
            "value": 10,
        },
        {
            "property": "maximum_temperature",
            "evaluation": ">",
            "value": 30,
        },
        {
            "property": "day_phrase",
            "evaluation": "equal",
            "value": "Party sunny",
        },
        {
            "property": "night_phrase",
            "evaluation": "equal",
            "value": "Party cloudy",
        }
    ]
}

""" DB """


def configDB():
    database = None
    db_host = None
    db_user = None
    db_password = None
    db_port = None
    if config.get("DATABASE") is not None:
        database = config.get("DATABASE")
    else:
        with open(dag_path+'/keys/'+"db.txt", 'r') as f:
            database = f.read()
        f.close()
    if config.get("DB_HOST") is not None:
        db_host = config.get("DB_HOST")
    else:
        with open(dag_path+'/keys/'+"host.txt", 'r') as f:
            db_host = f.read()
        f.close()
    if config.get("DB_USER") is not None:
        db_user = config.get("DB_USER")
    else:
        with open(dag_path+'/keys/'+"user.txt", 'r') as f:
            db_user = f.read()
        f.close()
    if config.get("DB_PASSWORD") is not None:
        db_password = config.get("DB_PASSWORD")
    else:
        with open(dag_path+'/keys/'+"pwd.txt", 'r') as f:
            db_password = f.read()
        f.close()
    if config.get("DB_PORT") is not None:
        db_port = config.get("DB_PORT")
    else:
        with open(dag_path+'/keys/'+"port.txt", 'r') as f:
            db_port = f.read()
        f.close()
    return {
        "db_host": db_host or defaults.get("db_host"),
        "database": database or defaults.get("database"),
        "db_user": db_user or defaults.get("db_user"),
        "db_password": db_password or defaults.get("db_password"),
        "db_port": db_port or defaults.get("db_port")
    }


""" SMTP """


def configSMTP():
    smtp = None
    smtp_password = None
    smtp_user = None
    smtp_port = None
    smtp_from = None
    smtp_to = None
    if config.get("SMTP") is not None:
        smtp = config.get("SMTP")
    if config.get("SMTP_PASSWORD") is not None:
        smtp_password = config.get("SMTP_PASSWORD")
    if config.get("SMTP_USER") is not None:
        smtp_user = config.get("SMTP_USER")
        smtp_from = smtp_user
    if config.get("SMTP_PORT") is not None:
        smtp_port = config.get("SMTP_PORT")
    if config.get("SMTP_FROM") is not None:
        smtp_from = config.get("SMTP_FROM")
    if config.get("SMTP_TO") is not None:
        smtp_to = config.get("SMTP_TO")
    return {
        "smtp": smtp if smtp else defaults.get("smtp"),
        "smtp_password": smtp_password if smtp_password else defaults.get("smtp_password"),
        "smtp_user": smtp_user or defaults.get("smtp_user"),
        "smtp_port": smtp_port or defaults.get("smtp_port"),
        "smtp_from": smtp_from or defaults.get("smtp_from"),
        "smtp_to": smtp_to or defaults.get("smtp_to")
    }


""" API """


def configAPI():
    api_key = None
    api_city_id = None
    if config.get("API_KEY") is not None:
        api_key = config.get("API_KEY")
    else:
        with open(dag_path+'/keys/'+"apiKey.txt", 'r') as f:
            api_key = f.read()
        f.close()
    if config.get("API_CITY_ID") is not None:
        api_city_id = config.get("API_CITY_ID")
    return {
        "api_key": api_key or defaults.get("api_key"),
        "api_city_id": api_city_id or defaults.get('api_city_key')
    }


""" Airflow_Variables """


def setVariables(db_host=None, database=None, db_user=None, db_password=None, db_port=None, smtp=None, smtp_password=None, smtp_user=None, smtp_port=None, smtp_from=None, smtp_to=None, api_key=None, api_city_id=None):
    Variable.set("db_host", db_host if db_host or defaults.get(
        'priorize_defaults') is True else defaults.get("db_host"))
    Variable.set("database", database if database or defaults.get(
        'priorize_defaults') is True else defaults.get("database"))
    Variable.set("db_user", db_user if db_user or defaults.get(
        'priorize_defaults') is True else defaults.get("db_user"))
    Variable.set("db_password", db_password if db_password or defaults.get(
        'priorize_defaults') is True else defaults.get("db_password"))
    Variable.set("db_port", db_port if db_port or defaults.get(
        'priorize_defaults') is True else defaults.get("db_port"))
    Variable.set("smtp", smtp if smtp or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp"))
    Variable.set("smtp_password", smtp_password if smtp_password or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp_password"))
    Variable.set("smtp_user", smtp_user if smtp_user or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp_user"))
    Variable.set("smtp_port", smtp_port if smtp_port or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp_port"))
    Variable.set("smtp_from", smtp_from if smtp_from or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp_from"))
    Variable.set("smtp_to", smtp_to if smtp_to or defaults.get(
        'priorize_defaults') is True else defaults.get("smtp_to"))
    Variable.set("api_key", api_key if api_key or defaults.get(
        'priorize_defaults') is True else defaults.get("api_key"))
    Variable.set("api_city_id", api_city_id if api_city_id or defaults.get(
        'priorize_defaults') is True else defaults.get("api_city_id"))
    Variable.set(key='thresholds', value=defaults.get(
        "thresholds"), serialize_json=True)


def doConfig():
    db_config = configDB()
    smtp_config = configSMTP()
    api_config = configAPI()
    setVariables(
        db_host=db_config.get("db_host"),
        database=db_config.get("database"),
        db_user=db_config.get("db_user"),
        db_password=db_config.get("db_password"),
        db_port=db_config.get("db_port"),
        smtp=smtp_config.get("smtp"),
        smtp_password=smtp_config.get("smtp_password"),
        smtp_user=smtp_config.get("smtp_user"),
        smtp_port=smtp_config.get("smtp_port"),
        smtp_from=smtp_config.get("smtp_from"),
        smtp_to=smtp_config.get("smtp_to"),
        api_key=api_config.get("api_key"),
        api_city_id=api_config.get("api_city_id"))

    print("doConfig()")
