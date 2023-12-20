import datetime
import requests
import my_secrets as s
import json
import psycopg
import pandas as pd
psycopg._encodings._py_codecs["UNICODE"] = "utf-8"
psycopg._encodings.py_codecs.update(
    (k.encode(), v) for k, v in psycopg._encodings._py_codecs.items()
)
apiKey = s.apiKey

tableProperties = {
    'localization': 'localization_id INT PRIMARY KEY NOT NULL UNIQUE IDENTITY(0,1), key VARCHAR(255), type VARCHAR(255), rank VARCHAR(255), city VARCHAR(255), region VARCHAR(255), country VARCHAR(255), timezone VARCHAR(255), latitude FLOAT, longitude FLOAT, elevation FLOAT',
    'foreCastData': 'fore_cast_data_id INT PRIMARY KEY NOT NULL UNIQUE IDENTITY(0,1), key VARCHAR(255), date DATETIME, minimum_temperature VARCHAR(255), maximum_temperature VARCHAR(255), day_phrase VARCHAR(255), day_has_precipitation BOOLEAN, night_phrase VARCHAR(255), night_has_precipitation BOOLEAN',
    #    'fore_cast_and_localization': 'fore_cast_and_localization_id INT PRIMARY KEY NOT NULL UNIQUE IDENTITY(0,1), localization_id INT, fore_cast_data_id INT, FOREIGN KEY (localization_id) REFERENCES localization(localization_id), FOREIGN KEY (fore_cast_data_id) REFERENCES foreCastData(fore_cast_data_id)'
}

tableStagingProperties = {
    'localization': ['key', 'type', 'rank', 'city', 'region', 'country', 'timezone', 'latitude', 'longitude', 'elevation'],
    'foreCastData': ['key', 'date', 'minimum_temperature', 'maximum_temperature', 'day_phrase', 'day_has_precipitation', 'night_phrase', 'night_has_precipitation'],
    #    'fore_cast_and_localization': ['localization_id', 'fore_cast_data_id']
}
tableStagingWhere = {
    'localization': ['key', 'city', 'region', 'country'],
    'foreCastData': ['key', 'date'],
    #    'fore_cast_and_localization': ['localization_id', 'fore_cast_data_id']
}


def getForeCastData(element):
    foreCastUrl = f"http://dataservice.accuweather.com/forecasts/v1/daily/1day/{
        element.get('Key')}?apikey={apiKey}"
    foreCastData = requests.get(foreCastUrl)
    if foreCastData.status_code == 200:
        foreCastData = foreCastData.json()
        return foreCastData
    else:
        raise Exception("Error getting the forecast data.")


def saveDataToFile(element, foreCastData):
    elementAndForeCast = {
        'element': element,
        'foreCastData': foreCastData
    }
    fileName = f"{element.get('EnglishName')}.json"
    with open(fileName, 'w') as file:
        json.dump(elementAndForeCast, file)


def createForeCastAndLocalizationData(element, foreCastData):
    fore_cast_and_localization = {
        'localization': {
            'key': element.get('Key'),
            'type': element.get('Type'),
            'rank': element.get('Rank'),
            'city': element.get('EnglishName'),
            'region': element.get('AdministrativeArea', {}).get('EnglishName'),
            'country': element.get('Country', {}).get('EnglishName'),
            'timezone': f"{element.get('TimeZone', {}).get('Code')} - {element.get('TimeZone', {}).get('GmtOffset')}",
            'latitude': element.get('GeoPosition', {}).get('Latitude'),
            'longitude': element.get('GeoPosition', {}).get('Longitude'),
            'elevation': f"{element.get('GeoPosition', {}).get('Elevation', {}).get('Metric', {}).get('Value')}",
        },
        'foreCastData': {
            'key': element.get('Key'),
            'date': f"{foreCastData.get('Headline', {}).get('EffectiveDate')}",
            'minimum_temperature': f"{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Minimum', {}).get('Value')}{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Minimum', {}).get('Unit')}",
            'maximum_temperature': f"{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Maximum', {}).get('Value')}{foreCastData.get('DailyForecasts', [{}])[0].get('Temperature', {}).get('Maximum', {}).get('Unit')}",
            'day_phrase': foreCastData.get('DailyForecasts', [{}])[0].get('Day', {}).get('IconPhrase'),
            'day_has_precipitation': foreCastData.get('DailyForecasts', [{}])[0].get('Day', {}).get('HasPrecipitation'),
            'night_phrase': foreCastData.get('DailyForecasts', [{}])[0].get('Night', {}).get('IconPhrase'),
            'night_has_precipitation': foreCastData.get('DailyForecasts', [{}])[0].get('Night', {}).get('HasPrecipitation'),
        },
    }
    return fore_cast_and_localization


def createSchemaIfDoesntExists():
    if s.db and s.db.get('user') and s.db.get('password') and s.db.get('host') and s.db.get('port') and s.db.get('database'):
        try:
            with psycopg.connect(dbname=s.db.get('database'), user=s.db.get('user'), password=s.db.get('password'), host=s.db.get('host'), port=s.db.get('port')) as conn:
                print('Connected to RedShift in create Schema')
                with conn.cursor() as cur:
                    # Check if schema exists
                    df = pd.read_sql_query(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{
                        s.db.get('user')}'", conn)
                    print("Schema exists.")
                    print(df)
                    """ cur.execute(f"CREATE SCHEMA IF NOT EXISTS {
                        s.db.get('user')};") """
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
                    """ if (createTableIfDoesntExists(cur, 'fore_cast_and_localization')):
                        print("The fore_cast_and_localization table exists.")
                    else:
                        raise Exception(
                            "The fore_cast_and_localization table doesn't exist.") """
                    conn.commit()
                    return True
        except KeyError as e:
            print(f"Error: Missing property in s.db dictionary: {e}")
        except Exception as e:
            print(f"Error: {e}")


def createTableIfDoesntExists(cur, tableName):
    if s.db and s.db.get('user') and s.db.get('password') and s.db.get('host') and s.db.get('port') and s.db.get('database'):
        try:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {
                tableName} ({tableProperties.get(tableName)});")
            return True
        except KeyError as e:
            print(f"Error: Missing property in s.db dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False


def createStagingTable(tableName):
    if s.db and s.db.get('user') and s.db.get('password') and s.db.get('host') and s.db.get('port') and s.db.get('database'):
        try:
            with psycopg.connect(dbname=s.db.get('database'), user=s.db.get('user'), password=s.db.get('password'), host=s.db.get('host'), port=s.db.get('port')) as conn:
                with conn.cursor() as cur:
                    print(f'Connected to RedShift in create staging table, {
                          tableName}')
                    query = f"CREATE TABLE IF NOT EXISTS {
                        tableName}_staging ({tableProperties.get(tableName)})"
                    print(query)
                    cur.execute(query)
                    print(f"Created staging table. {tableName}_staging")
                    return True
        except KeyError as e:
            print(f"Error: Missing property in s.db dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False


def insertDataFromStagingToFinal(tableName, newData):
    if s.db and s.db.get('user') and s.db.get('password') and s.db.get('host') and s.db.get('port') and s.db.get('database'):
        try:
            with psycopg.connect(dbname=s.db.get('database'), user=s.db.get('user'), password=s.db.get('password'), host=s.db.get('host'), port=s.db.get('port')) as conn:
                with conn.cursor() as cur:
                    print(
                        'Connected to RedShift in insert data from staging to final table ' + tableName)
                    where_conditions = [f"{tableName}.{column} = {tableName}_staging.{
                        column}" for column in tableStagingWhere.get(tableName)]
                    where_conditions = " AND ".join(where_conditions) if len(
                        where_conditions) > 1 else where_conditions[0]
                    cur.execute(f"DELETE FROM {tableName} USING {
                        tableName}_staging WHERE {where_conditions}")
                    print(f"Deleted from final table. {tableName}")
                    query = f"INSERT INTO {s.db.get('user')}.{tableName}({' , '.join(tableStagingProperties[tableName])}) VALUES ({
                        ", ".join([f"{newData.get(t) if isinstance(newData.get(t), (int, float, complex)) else "'" + newData.get(t) + "'"}" for t in tableStagingProperties[tableName]])})"
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
        except KeyError as e:
            print(f"Error: Missing property in s.db dictionary: {e}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False
    return False


def insertDataIntoStagging(tableName, data, cur):
    try:
        query = f"INSERT INTO {s.db.get('user')}.{tableName}_staging ({', '.join(tableStagingProperties[tableName])}) VALUES ({
            ", ".join([f"{data.get(t) if isinstance(data.get(t), (int, float, complex)) else "'" + data.get(t) + "'"}" for t in tableStagingProperties[tableName]])})"
        print(query)
        cur.execute(query)
        return True
    except KeyError as e:
        print(f"Error: Missing property in s.db dictionary: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def saveToRedShift(data):
    if s.db and s.db.get('user') and s.db.get('password') and s.db.get('host') and s.db.get('port') and s.db.get('database'):
        try:
            with psycopg.connect(dbname=s.db.get('database'), user=s.db.get('user'), password=s.db.get('password'), host=s.db.get('host'), port=s.db.get('port')) as conn:
                with conn.cursor() as cur:
                    print('Connected to RedShift in save to RedShift')
                    # Save data.get('localization') to localization table
                    if not createStagingTable('localization'):

                        conn.close()
                        raise Exception(
                            "The localization staging table doesn't exist.")
                    else:
                        print("The localization staging table exists.")
                        # Insert data from data.get('localization') to localization_staging table
                        if (not insertDataIntoStagging('localization', data.get('localization'), cur)):
                            conn.close()
                            raise Exception(
                                "Error inserting data into localization_staging table.")
                        else:
                            print("Data inserted into localization_staging table.")
                            conn.commit()
                            # Insert data from localization_staging table to localization table
                            insertDataFromStagingToFinal(
                                'localization', data.get('localization'))
                            """ query = f"SELECT localization_id FROM localization WHERE key = {
                                data.get('localization').get('key')}"
                            print(query)
                            df = pd.read_sql_query(query, conn)
                            print(df)
                            # Get localization_id from localization table where key = data.get('localization').get('key')
                            localization_id = df.iloc[0].to_dict().get(
                                'localization_id') 
                            print(localization_id) """
                    # Save data.get('foreCastData') to foreCastData table
                    if not createStagingTable('foreCastData'):
                        conn.close()
                        raise Exception(
                            "The foreCastData staging table doesn't exist.")
                    else:
                        print("The foreCastData staging table exists.")
                        # Insert data from data.get('foreCastData') to foreCastData_staging table
                        if (not insertDataIntoStagging('foreCastData', data.get('foreCastData'), cur)):
                            conn.close()
                            raise Exception(
                                "Error inserting data into foreCastData_staging table.")
                        else:
                            print("Data inserted into foreCastData_staging table.")
                            conn.commit()
                            # Insert data from foreCastData_staging table to foreCastData table
                            insertDataFromStagingToFinal(
                                'foreCastData', data.get('foreCastData'))
                            """ query = f"SELECT fore_cast_data_id FROM foreCastData WHERE key = '{data.get(
                                'foreCastData').get('key')}' AND date = '{data.get('foreCastData').get('date')}'"
                            print(query)
                            df = pd.read_sql_query(query, conn)
                            print(df)
                            # Get fore_cast_data_id from foreCastData table where key = data.get('foreCastData').get('key') AND date = data.get('foreCastData').get('date')
                            fore_cast_data_id = df.iloc[0].to_dict().get(
                                'fore_cast_data_id')
                            print(fore_cast_data_id) """
                    # Save data.get('fore_cast_and_localization') to fore_cast_and_localization table
                    """ if not createStagingTable('fore_cast_and_localization'):
                        conn.close()
                        raise Exception(
                            "The fore_cast_and_localization staging table doesn't exist.")
                    else:
                        print("The fore_cast_and_localization staging table exists.")
                        # Insert data from data.get('fore_cast_and_localization') to fore_cast_and_localization_staging table
                        if (not insertDataIntoStagging('fore_cast_and_localization', {'localization_id': localization_id, 'fore_cast_data_id': fore_cast_data_id}, cur)):
                            conn.close()
                            raise Exception(
                                "Error inserting data into fore_cast_and_localization_staging table.")
                        else:
                            print(
                                "Data inserted into fore_cast_and_localization_staging table.")
                            # Insert data from fore_cast_and_localization_staging table to fore_cast_and_localization table
                            insertDataFromStagingToFinal(
                                'fore_cast_and_localization', {'localization_id': localization_id, 'fore_cast_data_id': fore_cast_data_id})
                    conn.commit() """
                    conn.close()
                    return True
        except KeyError as e:
            print(f"Error: Missing property in s.db dictionary: {e}")
            return False
        except Exception as error:
            print("Error while connecting to RedShift:", error)
            return False
    return False


try:
    groupCode = "50"
    url = f"http://dataservice.accuweather.com/locations/v1/topcities/{
        groupCode}?apikey={apiKey}"
    data = requests.get(url)
    if data.status_code == 200:
        data = data.json()
        if isinstance(data, list) and len(data) != 0 and data[0].get('Key'):
            data = data[0:1]
            for element in data:
                foreCastData = getForeCastData(element)
                saveDataToFile(element, foreCastData)
                fore_cast_and_localization = createForeCastAndLocalizationData(
                    element, foreCastData)
                if not createSchemaIfDoesntExists():
                    raise Exception("The database doesn't exists.")
                else:
                    saveToRedShift(fore_cast_and_localization)
        else:
            raise Exception(
                "Error getting the group data; there is no iterable data.")
    else:
        # Get information from the test jsons to test the RedShift connection
        print("Error getting the group data; no response from the API. We will try to test the RedShift connection.")
        for element in ['Beijing', 'Bogota', 'Dhaka', 'Kinshasa', 'Santiago']:
            with open(f"./weather-learning-python/{element}.json", "r") as file:
                data = json.load(file)
                fore_cast_and_localization = createForeCastAndLocalizationData(
                    data.get('element'), data.get('foreCastData'))
                if not createSchemaIfDoesntExists():
                    raise Exception("The database doesn't exists.")
                else:
                    saveToRedShift(fore_cast_and_localization)
        # raise Exception(
        #    "Error getting the group data; no response from the API.")
except Exception as e:
    print(e)
    now = datetime.datetime.now()
    with open("error.log", "a") as file:
        file.write(f"{now} {e}\n")


def dontUse(conn):
    """ df = pd.read_sql('SELECT * FROM weather', con=coon)
    print(df) """
