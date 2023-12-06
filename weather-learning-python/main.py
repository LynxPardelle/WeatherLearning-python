import requests
import secrets as s
import json
try:
    apiKey = s.apiKey
    # Conseguir el top 50, 100 or 150 ciudades, alrededor del mundo.
    groupCode = input("Enter group length(50, 100 or 150): ")
    # https://developer.accuweather.com/accuweather-locations-api/apis/get/locations/v1/topcities/%7Bgroup%7D
    url = "http://dataservice.accuweather.com/locations/v1/topcities/" + \
        groupCode + '?apikey=' + apiKey
    data = requests.get(url)
    print(data.status_code)
    if (data.status_code == 200):
        # Convertir a JSON
        data = data.json()
        # Revisar si la respuesta es una lista y si no está vacía
        if isinstance(data, list) and len(data) != 0 and data[0]['Key']:
            # Obtener los primeros 5 elementos de la lista
            # Esto para no tener mucho uso de la API por ahora, mientras esto está en desarrollo
            data = data[0:5]
            # Iterar en cada elemento de la lista para obtener el pronóstico del clima
            for element in data:
                # https://developer.accuweather.com/accuweather-forecast-api/apis/get/forecasts/v1/daily/1day/%7BlocationKey%7D
                foreCastUrl = "http://dataservice.accuweather.com/forecasts/v1/daily/1day/" + \
                    element['Key'] + '?apikey=' + apiKey
                foreCastData = requests.get(foreCastUrl)
                if foreCastData.status_code == 200:
                    # Convertir a JSON
                    foreCastData = foreCastData.json()
                    # Imprimir el pronóstico del clima
                    print(foreCastData)
                    # Guardar el pronóstico del clima y el elemento en un diccionario
                    # https://www.w3schools.com/python/python_dictionaries.asp
                    # https://www.w3schools.com/python/python_json.asp
                    elementAndForeCast = {
                        'element': element,
                        'foreCastData': foreCastData
                    }
                    fileName = element['EnglishName'] + '.json'
                    # Guardar el diccionario en un archivo JSON
                    # https://www.w3schools.com/python/python_file_write.asp
                    with open(fileName, 'w') as file:
                        json.dump(elementAndForeCast, file)
                    # TODO: Crear un diccionario con los datos que se necesitan según la tabla de RedShift
                    # TODO: Guardar el diccionario en una base de datos RedShift
                else:
                    raise Exception("Error Getting the forecast data.")
        else:
            raise Exception(
                "Error Getting the group data; there is no iterable data.")
    else:
        raise Exception(
            "Error Getting the group data; no response from the API.")
except Exception as e:
    print(e)
    # Insertar en un archivo de texto el error con el tiempo en que ocurrió
    # https://www.w3schools.com/python/python_file_write.asp
    # https://www.w3schools.com/python/python_datetime.asp
    import datetime
    now = datetime.datetime.now()
    with open("error.log", "a") as file:
        file.write(str(now) + " " + str(e) + "\n")
