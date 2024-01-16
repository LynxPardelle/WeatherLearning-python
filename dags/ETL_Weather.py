from datetime import timedelta,datetime
import requests
import json
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

dag_path = os.getcwd()
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()
with open(dag_path+'/keys/'+"apiKey.txt",'r') as f:
    apiKey= f.read()
with open(dag_path+'/keys/'+"port.txt",'r') as f:
    port= f.read()
with open(dag_path+'/keys/'+"host.txt",'r') as f:
    host= f.read()
url=host
dbData = {
    'user': user,
    'password': pwd,
    'host': host,
    'port': port,
    'database': data_base
}
s = {
    'apiKey' : apiKey,
    'db':  dbData
}

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': port,
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'DavidBU',
    'start_date': datetime(2024,1,15),
    'retries':5,
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
        url = f"http://dataservice.accuweather.com/locations/v1/topcities/50?apikey={apiKey}"
        data = requests.get(url)
        if data and data.status_code and data.status_code == 200:
            print('Success!')
            data = data.json()
            if isinstance(data, list) and len(data) != 0 and data[0].get('Key'):
                data = [x for x in data if x['Key'] == '242560'][0] # Mexico City
                foreCastUrl = f"http://dataservice.accuweather.com/forecasts/v1/daily/1day/{data.get('Key')}?apikey={apiKey}"
                foreCastData = requests.get(foreCastUrl)
                if foreCastData.status_code != 200:
                    raise Exception("Error al obtener el forecast")
                forecast = foreCastData.json()
                fore_cast_and_localization = {
                  'location': data,
                  'forecast': forecast
                }
                with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                    json.dump(fore_cast_and_localization, json_file)
        else:
            print('An error has occurred.') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e  

# Transform Data Function
def transform_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
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
    
# Redshift Connection Function
def redshift_conexion(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port=redshift_conexion["port"])
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
    if dbData and dbData.get('user') and dbData.get('password') and dbData.get('host') and dbData.get('port') and dbData.get('database'):
        try:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {tableName} ({tableProperties.get(tableName)});")
            return True
        except KeyError as e:
            print(f"Error: Missing property in dbData dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False

def createSchemaIfDoesntExists():
    if dbData and dbData.get('user') and dbData.get('password') and dbData.get('host') and dbData.get('port') and dbData.get('database'):
        try:
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('user'), password=dbData.get('password'), host=dbData.get('host'), port=dbData.get('port')) as conn:
                print('Connected to RedShift in create Schema')
                with conn.cursor() as cur:
                    # Check if schema exists
                    df = pd.read_sql_query(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dbData.get('user')}'", conn)
                    print("Schema exists.")
                    print(df)
                    """ cur.execute(f"CREATE SCHEMA IF NOT EXISTS {
                        dbData.get('user')};") """
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
    if dbData and dbData.get('user') and dbData.get('password') and dbData.get('host') and dbData.get('port') and dbData.get('database'):
        try:
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('user'), password=dbData.get('password'), host=dbData.get('host'), port=dbData.get('port')) as conn:
                with conn.cursor() as cur:
                    print(f'Connected to RedShift in create staging table, {tableName}')
                    query = f"CREATE TABLE IF NOT EXISTS {tableName}_staging ({tableProperties.get(tableName)})"
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
        tableStagingPropertiesOfTheTable = tableStagingProperties.get(tableName)
        print(tableStagingPropertiesOfTheTable)
        joinedTableStagingPropertiesOfTheTable = ", ".join(tableStagingPropertiesOfTheTable)
        print(joinedTableStagingPropertiesOfTheTable)
        newValues = [bringCorrectValue(data.get(t.get('key')),t.get('type')) for t in tableStagingPropertiesWTypes[tableName]]
        print(newValues)
        joinedValues = ", ".join(str(v) for v in newValues)
        print(joinedValues)
        tableUser = dbData.get('user')
        query = f"INSERT INTO {tableUser}.{tableName}_staging ({joinedTableStagingPropertiesOfTheTable}) VALUES ({joinedValues})"
        print(query)
        cur.execute(query)
        return True
    except KeyError as e:
        print(f"Error inserting Data Into Stagging: Missing property in dbData dictionary: {e}")
        return False
    except Exception as e:
        print(f"Error inserting Data Into Stagging: {e}")
        return False

def insertDataFromStagingToFinal(tableName, newData):
    try:
        if dbData and dbData.get('user') and dbData.get('password') and dbData.get('host') and dbData.get('port') and dbData.get('database'):
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('user'), password=dbData.get('password'), host=dbData.get('host'), port=dbData.get('port')) as conn:
                with conn.cursor() as cur:
                    print(
                        'Connected to RedShift in insert data from staging to final table ' + tableName)
                    where_conditions = [f"{tableName}.{column} = {tableName}_staging.{column}" for column in tableStagingWhere.get(tableName)]
                    where_conditions = " AND ".join(where_conditions) if len(where_conditions) > 1 else where_conditions[0]
                    cur.execute(f"DELETE FROM {tableName} USING {tableName}_staging WHERE {where_conditions}")
                    print(f"Deleted from final table. {tableName}")
                    tableStagingPropertiesOfTheTable = tableStagingProperties.get(tableName)
                    print(tableStagingPropertiesOfTheTable)
                    joinedTableStagingPropertiesOfTheTable = ", ".join(tableStagingPropertiesOfTheTable)
                    print(joinedTableStagingPropertiesOfTheTable)
                    newValues = [bringCorrectValue(newData.get(t.get('key')),t.get('type')) for t in tableStagingPropertiesWTypes[tableName]]
                    print(newValues)
                    joinedValues = ", ".join(str(v) for v in newValues)
                    print(joinedValues)
                    query = f"INSERT INTO {dbData.get('user')}.{tableName}({joinedTableStagingPropertiesOfTheTable}) VALUES ({joinedValues})"
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
        data=json.load(json_file)
    if not createSchemaIfDoesntExists():
        raise Exception("The database doesn't exists.")
    try:
        if dbData and dbData.get('user') and dbData.get('password') and dbData.get('host') and dbData.get('port') and dbData.get('database'):
            with psycopg2.connect(dbname=dbData.get('database'), user=dbData.get('user'), password=dbData.get('password'), host=dbData.get('host'), port=dbData.get('port')) as conn:
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

# Tareas
##1. Extraction
task_1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
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

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32