from datetime import timedelta, datetime
import requests
from email import message
import json
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import smtplib
import ssl
import os
from config_variables import doConfig, defaults

doConfig()
dag_path = os.getcwd()

""" DB """
database = Variable.get("database", default_var=defaults.get("database"))
db_user = Variable.get("db_user", default_var=defaults.get("db_user"))
db_password = Variable.get(
    "db_password", default_var=defaults.get("db_password"))
db_host = Variable.get("db_host", default_var=defaults.get("db_host"))
db_port = Variable.get("db_port", default_var=defaults.get("db_port"))
""" SMTP """
smtp = Variable.get("smtp", default_var=defaults.get("smtp"))
smtp_password = Variable.get(
    "smtp_password", default_var=defaults.get("smtp_password"))
smtp_user = Variable.get("smtp_user", default_var=defaults.get("smtp_user"))
smtp_port = Variable.get("smtp_port", default_var=defaults.get("smtp_port"))
smtp_from = Variable.get("smtp_from", default_var=defaults.get("smtp_from"))
smtp_to = Variable.get("smtp_to", default_var=defaults.get("smtp_to"))
""" API """
api_key = Variable.get("api_key", default_var=defaults.get("api_key"))
api_city_id = Variable.get(
    "api_city_id", default_var=defaults.get("api_city_id"))

url = db_host
dbData = {
    'db_user': db_user,
    'db_password': db_password,
    'db_host': db_host,
    'db_port': db_port,
    'database': database
}
smtpData = {
    'smtp': smtp,
    'smtp_password': smtp_password,
    'smtp_user': smtp_user,
    'smtp_port': smtp_port,
    'smtp_from': smtp_from,
    'smtp_to': smtp_to
}
s = {
    'api_key': api_key,
    'api_city_id': api_city_id,
    'db':  dbData,
    'smtp': smtpData
}

redshift_conn = {
    'db_host': url,
    'db_user': db_user,
    'database': database,
    'db_port': db_port,
    'db_password': db_password
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'LynxPardelle',
    'email': ['lynxpardelle@lynxpardelle.com'],
    'email_on_retry': False,
    'email_on_failure': False,
    'start_date': datetime(2024, 1, 15),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='Weather_ETL',
    default_args=default_args,
    description='Add weather data daily',
    schedule_interval="@daily",
    catchup=False
)

# Extract Data Function


def extract_data(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        api = "http://dataservice.accuweather.com/locations/v1/topcities/50?apikey="
        url = f"{api}{api_key}"
        data = requests.get(url)
        if data and data.status_code and data.status_code == 200:
            print('Success!')
            data = data.json()
            if isinstance(data, list) and len(data) != 0 and data[0].get('Key'):
                data = [x for x in data if x['Key']
                        == '242560'][0]  # Mexico City
                forecastAPI = "http://dataservice.accuweather.com/forecasts/v1/daily/1day/"
                dataKey = data.get('Key')
                foreCastUrl = f"{forecastAPI}{dataKey}?apikey={api_key}"
                foreCastData = requests.get(foreCastUrl)
                if foreCastData.status_code != 200:
                    raise Exception("Error al obtener el forecast")
                forecast = foreCastData.json()
                fore_cast_and_localization = {
                    'location': data,
                    'forecast': forecast
                }
                file2Create = dag_path+'/raw_data/'+"data_" + \
                    str(date.year)+'-'+str(date.month)+'-' + \
                    str(date.day)+'-'+str(date.hour)+".json"
                print('file2Create', file2Create)
                with open(file2Create, "w") as json_file:
                    json.dump(fore_cast_and_localization, json_file)
                    print('Data saved!')
        else:
            print('An error has occurred.')
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e

# Transform Data Function


def checkForTresholds(ti, data, date):
    try:
        thresholds = Variable.get("thresholds", deserialize_json=True)
        print('thresholds', thresholds)
        print('type', type(thresholds))
        if thresholds and isinstance(thresholds, list) and len(thresholds) > 0:
            for threshold in thresholds:
                property = threshold.get('property')
                evaluation = threshold.get('evaluation')
                value = threshold.get('value')
                if property and evaluation and value:
                    if property in data.get('foreCastData'):
                        thresholdsPassed = []
                        propertyValue = data.get('foreCastData').get(property)
                        propertyIntoInt = int(
                            float(propertyValue.replace('F', '')))
                        if evaluation == '<':
                            if propertyIntoInt < value:
                                thresholdsPassed.append(
                                    f'{property} < {value}')
                        elif evaluation == '<=':
                            if propertyIntoInt <= value:
                                thresholdsPassed.append(
                                    f'{property} <= {value}')
                        elif evaluation == '>':
                            if propertyIntoInt > value:
                                thresholdsPassed.append(
                                    f'{property} > {value}')
                        elif evaluation == '>=':
                            if propertyIntoInt >= value:
                                thresholdsPassed.append(
                                    f'{property} >= {value}')
                        elif evaluation == 'equal':
                            if propertyValue == value:
                                thresholdsPassed.append(
                                    f'{property} == {value}')
                        elif evaluation == 'notequal':
                            if propertyValue != value:
                                thresholdsPassed.append(
                                    f'{property} != {value}')
                        elif evaluation == 'contains':
                            if value in propertyValue:
                                thresholdsPassed.append(
                                    f'{property} contains {value}')
                        elif evaluation == 'notcontains':
                            if value not in propertyValue:
                                thresholdsPassed.append(
                                    f'{property} not contains {value}')
                        print('thresholdsPassed', thresholdsPassed)
                        if len(thresholdsPassed) > 0:
                            file2Create = dag_path+'/treshold_data/'+"data_"+str(date.year)+'-' + \
                                str(date.month)+'-'+str(date.day) + \
                                '-'+str(date.hour)+".json"
                            print('file2Create', file2Create)
                            with open(file2Create, 'w') as json_file:
                                print('data', data)
                                json.dump(data, json_file)
                                print('Data saved!')
                                ti.xcom_push(key='thresholdsPassed',
                                             value=thresholdsPassed)
                                print('thresholds in XCOM:', thresholdsPassed)
                    else:
                        raise Exception(
                            f"La propiedad {property} no existe en el objeto foreCastData")
                else:
                    raise Exception(
                        "Faltan propiedades en el objeto threshold")
        else:
            raise Exception("No hay thresholds definidos")
    except ValueError as e:
        print("Error in value al obtener thresholds", e)
        # raise e
    except Exception as e:
        print("Error al obtener thresholds", e)
        # raise e


def transform_data(ti, **kwargs):
    exec_date = kwargs.get('exec_date')
    print(f"Transformando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data = json.load(json_file)
        location = loaded_data.get('location')
        foreCastData = loaded_data.get('forecast')
        fore_cast_and_localization = {
            'localization': {
                'key': location.get('Key'),
                'type': location.get('Type'),
                'rank': location.get('Rank'),
                'city': location.get('EnglishName'),
                'region': location.get('AdministrativeArea', {}).get('EnglishName'),
                'country': location.get('Country', {}).get('EnglishName'),
                'timezone': f"{location.get('TimeZone', {}).get('Code')} - {location.get('TimeZone', {}).get('GmtOffset')}",
                'latitude': location.get('GeoPosition', {}).get('Latitude'),
                'longitude': location.get('GeoPosition', {}).get('Longitude'),
                'elevation': f"{location.get('GeoPosition', {}).get('Elevation', {}).get('Metric', {}).get('Value')}",
            },
            'foreCastData': {
                'key': location.get('Key'),
                'date': f"{foreCastData.get('Headline', {}).get('EffectiveDate')}",
                'minimum_temperature': f"{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Minimum', {}).get('Value')}{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Minimum', {}).get('Unit')}",
                'maximum_temperature': f"{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Maximum', {}).get('Value')}{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Maximum', {}).get('Unit')}",
                'day_phrase': foreCastData.get('DailyForecasts', [{}])[0].get('Day', {}).get('IconPhrase'),
                'day_has_precipitation': foreCastData.get('DailyForecasts', [{}])[0].get('Day', {}).get('HasPrecipitation'),
                'night_phrase': foreCastData.get('DailyForecasts', [{}])[0].get('Night', {}).get('IconPhrase'),
                'night_has_precipitation': foreCastData.get('DailyForecasts', [{}])[0].get('Night', {}).get('HasPrecipitation'),
            },
        }
        with open(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
            json.dump(fore_cast_and_localization, json_file)
        checkForTresholds(ti, fore_cast_and_localization, date)

# Redshift Connection Function


def redshift_conexion(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}")
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn.get("database"),
            user=redshift_conn.get("db_user"),
            password=redshift_conn.get("db_password"),
            port=redshift_conexion.get("db_port"))
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)


# Load Data Function
tableProperties = {
    'localization': 'localization_id INT PRIMARY KEY NOT NULL UNIQUE IDENTITY(0,1), key VARCHAR(255), type VARCHAR(255), rank VARCHAR(255), city VARCHAR(255), region VARCHAR(255), country VARCHAR(255), timezone VARCHAR(255), latitude FLOAT, longitude FLOAT, elevation FLOAT',
    'foreCastData': 'fore_cast_data_id INT PRIMARY KEY NOT NULL UNIQUE IDENTITY(0,1), key VARCHAR(255), date DATETIME, minimum_temperature VARCHAR(255), maximum_temperature VARCHAR(255), day_phrase VARCHAR(255), day_has_precipitation BOOLEAN, night_phrase VARCHAR(255), night_has_precipitation BOOLEAN',
}
tableStagingProperties = {
    'localization': ['key', 'type', 'rank', 'city', 'region', 'country', 'timezone', 'latitude', 'longitude', 'elevation'],
    'foreCastData': ['key', 'date', 'minimum_temperature', 'maximum_temperature', 'day_phrase', 'day_has_precipitation', 'night_phrase', 'night_has_precipitation'],
    #    'fore_cast_and_localization': ['localization_id', 'fore_cast_data_id']
}
tableStagingPropertiesWTypes = {
    'localization': [{'key': 'key', 'type': 'str'}, {'key': 'type', 'type': 'str'}, {'key': 'rank', 'type': 'str'}, {'key': 'city', 'type': 'str'}, {'key': 'region', 'type': 'str'}, {'key': 'country', 'type': 'str'}, {'key': 'timezone', 'type': 'str'}, {'key': 'latitude', 'type': 'float'}, {'key': 'longitude', 'type': 'float'}, {'key': 'elevation', 'type': 'float'}],
    'foreCastData': [{'key': 'key', 'type': 'str'}, {'key': 'date', 'type': 'datetime'}, {'key': 'minimum_temperature', 'type': 'str'}, {'key': 'maximum_temperature', 'type': 'str'}, {'key': 'day_phrase', 'type': 'str'}, {'key': 'day_has_precipitation', 'type': 'bool'}, {'key': 'night_phrase', 'type': 'str'}, {'key': 'night_has_precipitation', 'type': 'bool'}],
}
tableStagingWhere = {
    'localization': ['key', 'city', 'region', 'country'],
    'foreCastData': ['key', 'date'],
    #    'fore_cast_and_localization': ['localization_id', 'fore_cast_data_id']
}


def createTableIfDoesntExists(cur, tableName):
    if dbData and dbData.get('db_user') and dbData.get('db_password') and dbData.get('db_host') and dbData.get('db_port') and dbData.get('database'):
        try:
            sentence = "CREATE TABLE IF NOT EXISTS "
            cur.execute(f"{sentence}{tableName} (" +
                        tableProperties.get(tableName)+");")
            return True
        except KeyError as e:
            print(f"Error: Missing property in dbData dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False


def createSchemaIfDoesntExists():
    if dbData and dbData.get('db_user') and dbData.get('db_password') and dbData.get('db_host') and dbData.get('db_port') and dbData.get('database'):
        try:
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('db_user'), password=dbData.get('db_password'), host=dbData.get('db_host'), port=dbData.get('db_port')) as conn:
                print('Connected to RedShift in create Schema')
                with conn.cursor() as cur:
                    # Check if schema exists
                    df = pd.read_sql_query(
                        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+dbData.get('db_user')+"'", conn)
                    print("Schema exists.")
                    print(df)
                    """ cur.execute(f"CREATE SCHEMA IF NOT EXISTS {
                        dbData.get('db_user')};") """
                    print("Schema created or already exists.")
                    if (createTableIfDoesntExists(cur, 'localization')):
                        print("The localization table exists.")
                    else:
                        raise Exception(
                            "The localization table doesn't exist.")
                    if (createTableIfDoesntExists(cur, 'foreCastData')):
                        print("The foreCastData table exists.")
                    else:
                        raise Exception(
                            "The foreCastData table doesn't exist.")
                    conn.commit()
                    return True
        except KeyError as e:
            print(f"Error: Missing property in dbData dictionary: {e}")
        except Exception as e:
            print(f"Error: {e}")


def createStagingTable(tableName):
    if dbData and dbData.get('db_user') and dbData.get('db_password') and dbData.get('db_host') and dbData.get('db_port') and dbData.get('database'):
        try:
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('db_user'), password=dbData.get('db_password'), host=dbData.get('db_host'), port=dbData.get('db_port')) as conn:
                with conn.cursor() as cur:
                    print(f'Connected to RedShift in create staging table, '+tableName)
                    query = f"CREATE TABLE IF NOT EXISTS "+tableName + \
                        f"_staging ({tableProperties.get(tableName)})"
                    print(query)
                    cur.execute(query)
                    print(f"Created staging table. {tableName}_staging")
                    return True
        except KeyError as e:
            print(f"Error: Missing property in dbData dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False


def bringCorrectValue(value, type):
    if type == 'str':
        return f"'{value}'"
    elif type == 'bool':
        return value
    elif type == 'datetime':
        # Eliminate the 'T' and the 'Z' from the string
        value = value.replace('T', ' ').replace('Z', '')
        return f"'{value}'"
    elif type == 'float':
        return value
    elif type == 'int':
        return value
    else:
        return value


def insertDataIntoStagging(tableName, data, cur):
    try:
        tableStagingPropertiesOfTheTable = tableStagingProperties.get(
            tableName)
        print(tableStagingPropertiesOfTheTable)
        joinedTableStagingPropertiesOfTheTable = ", ".join(
            tableStagingPropertiesOfTheTable)
        print(joinedTableStagingPropertiesOfTheTable)
        newValues = [bringCorrectValue(data.get(t.get('key')), t.get(
            'type')) for t in tableStagingPropertiesWTypes[tableName]]
        print(newValues)
        joinedValues = ", ".join(str(v) for v in newValues)
        print(joinedValues)
        tableUser = dbData.get('db_user')
        query = f"INSERT INTO {tableUser}."+tableName + \
            "_staging ("+joinedTableStagingPropertiesOfTheTable + \
            ") VALUES ("+joinedValues+")"
        print(query)
        cur.execute(query)
        return True
    except KeyError as e:
        print(
            f"Error inserting Data Into Stagging: Missing property in dbData dictionary: {e}")
        return False
    except Exception as e:
        print(f"Error inserting Data Into Stagging: {e}")
        return False


def insertDataFromStagingToFinal(tableName, newData):
    try:
        if dbData and dbData.get('db_user') and dbData.get('db_password') and dbData.get('db_host') and dbData.get('db_port') and dbData.get('database'):
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('db_user'), password=dbData.get('db_password'), host=dbData.get('db_host'), port=dbData.get('db_port')) as conn:
                with conn.cursor() as cur:
                    print(
                        'Connected to RedShift in insert data from staging to final table ' + tableName)
                    where_conditions = [f"{tableName}."+column+" = "+tableName +
                                        "_staging."+column for column in tableStagingWhere.get(tableName)]
                    where_conditions = " AND ".join(where_conditions) if len(
                        where_conditions) > 1 else where_conditions[0]
                    cur.execute(f"DELETE FROM "+tableName+" USING " +
                                tableName+f"_staging WHERE {where_conditions}")
                    print(f"Deleted from final table. {tableName}")
                    tableStagingPropertiesOfTheTable = tableStagingProperties.get(
                        tableName)
                    print(tableStagingPropertiesOfTheTable)
                    joinedTableStagingPropertiesOfTheTable = ", ".join(
                        tableStagingPropertiesOfTheTable)
                    print(joinedTableStagingPropertiesOfTheTable)
                    newValues = [bringCorrectValue(newData.get(t.get('key')), t.get(
                        'type')) for t in tableStagingPropertiesWTypes[tableName]]
                    print(newValues)
                    joinedValues = ", ".join(str(v) for v in newValues)
                    print(joinedValues)
                    dbUser = dbData.get('db_user')
                    query = f"INSERT INTO "+dbUser+"."+tableName + \
                        "("+joinedTableStagingPropertiesOfTheTable + \
                        f") VALUES ({joinedValues})"
                    print(query)
                    cur.execute(query)
                    print(f"Inserted from staging to final table. {tableName}")
                    conn.commit()
                    query = f"DROP TABLE IF EXISTS {tableName}_staging"
                    print(query)
                    cur.execute(query)
                    conn.commit()
                    conn.close()
                    print(f"Dropped staging table. {tableName}_staging")
                    return True
        else:
            raise Exception("Missing dbData dictionary.")
    except KeyError as e:
        print(f"Error: Missing property in dbData dictionary: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def load_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        data = json.load(json_file)
    if not createSchemaIfDoesntExists():
        raise Exception("The database doesn't exists.")
    try:
        if dbData and dbData.get('db_user') and dbData.get('db_password') and dbData.get('db_host') and dbData.get('db_port') and dbData.get('database'):
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('db_user'), password=dbData.get('db_password'), host=dbData.get('db_host'), port=dbData.get('db_port')) as conn:
                with conn.cursor() as cur:
                    print('Connected to RedShift in save to RedShift')
                    if not createStagingTable('localization'):
                        conn.close()
                        raise Exception(
                            "The localization staging table doesn't exist.")
                    else:
                        print("The localization staging table exists.")
                        if (not insertDataIntoStagging('localization', data.get('localization'), cur)):
                            conn.close()
                            raise Exception(
                                "Error inserting data into localization_staging table.")
                        else:
                            print("Data inserted into localization_staging table.")
                            conn.commit()
                            insertDataFromStagingToFinal(
                                'localization', data.get('localization'))
                    if not createStagingTable('foreCastData'):
                        conn.close()
                        raise Exception(
                            "The foreCastData staging table doesn't exist.")
                    else:
                        print("The foreCastData staging table exists.")
                        if (not insertDataIntoStagging('foreCastData', data.get('foreCastData'), cur)):
                            conn.close()
                            raise Exception(
                                "Error inserting data into foreCastData_staging table.")
                        else:
                            print("Data inserted into foreCastData_staging table.")
                            conn.commit()
                            insertDataFromStagingToFinal(
                                'foreCastData', data.get('foreCastData'))
                    return True
        else:
            raise Exception("Missing dbData dictionary.")
    except KeyError as e:
        print(f"Error: Missing property in dbData dictionary: {e}")
        raise Exception(f"Error: Missing property in dbData dictionary: {e}")
    except Exception as error:
        print("Error while connecting to RedShift:", error)
        raise Exception("Error while connecting to RedShift:", error)


def send_email(exec_date, subject, body_text):
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtpData.get('smtp'), int(smtpData.get('smtp_port')), context=context) as email_server:
            email_server.starttls()
            print('Waiting to login...')
            email_server.login(smtpData.get('smtp_user'),
                               smtpData.get('smtp_password'))
            print('Logged in!')
            subject = f'{subject} {exec_date}!'
            message = 'Subject: {}\n\n{}'.format(subject, body_text)
            print('Sending email...')
            email_server.sendmail(smtpData.get(
                'smtp_from'), smtpData.get('smtp_to'), message)
            print('Success sending email!')
            email_server.quit()
    except Exception as error:
        print('Error while sending email', error)
        raise Exception("Error while sending email: ", error)


def report_successful_load(exec_date):
    try:
        print(f"Enviando correo de carga exitosa para la fecha: {exec_date}")
        send_email(exec_date, 'Se ha realizado una carga exitosa en',
                   'El proceso de carga de datos ha sido exitoso!')
        print('Correo enviado!')
    except Exception as error:
        print('Error while reporting the successful load', error)
        raise Exception("Error while reporting the successful load:", error)


def send_treshold_email(ti, **kwargs):
    try:
        exec_date = kwargs.get('exec_date')
        print(f"Enviando correo de alerta para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        with open(dag_path+'/treshold_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
            data = json.load(json_file)
            thresholdsPassed = ti.xcom_pull(
                key="thresholdsPassed", task_ids="transform_data")
            subject = 'Alerta de umbrales para la fecha: '+exec_date + \
                ', en la ciudad de {data.get("localization").get("city")}'
            body_text = 'Los umbrales que se han pasado son: ' + \
                ', '.join(thresholdsPassed)+'.Más infomración sobre la data: ' + \
                [f'{key}: {value}' for key, value in data]
            send_email(exec_date, subject, body_text)
    except Exception as error:
        print('Error while reporting the successful load', error)
        raise Exception("Error while reporting the successful load:", error)


# Tareas
# 0. Start in paralell
task_start = BashOperator(
    task_id='start',
    bash_command='date'
)
# 1. Extraction
task_1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 2. Transformacion
task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'exec_date': "{{ ds }} {{ execution_date.hour }}"},
    dag=BC_dag,
    do_xcom_push=True,
)

# 3. Envio de data
# 3.1 Conexion a base de datos
task_31 = PythonOperator(
    task_id="redshift_conexion",
    python_callable=redshift_conexion,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 4 Envio de correo
task_4 = PythonOperator(
    task_id='report_successful_load',
    python_callable=report_successful_load,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)
# s1 Sensor de archivos
task_s1 = FileSensor(
    task_id="file_sensor",
    poke_interval=60,
    timeout=60 * 30,
    filepath=dag_path+'/treshold_data/data_*.json',
    dag=BC_dag
)

# t1 Enviar correo de alerta
task_t1 = PythonOperator(
    task_id="send_treshold_email",
    python_callable=send_treshold_email,
    op_kwargs={'exec_date': "{{ ds }} {{ execution_date.hour }}"},
    dag=BC_dag
)

# Definicion orden de tareas
task_start >> [
    task_1 >> task_2 >> task_31 >> task_32 >> task_4,
    task_s1 >> task_t1
]
