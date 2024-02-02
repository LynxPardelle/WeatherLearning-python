# WeatherLearning-python

## Entrega 1 & 2

- Se ha creado un repositorio en GitHub para almacenar el proyecto.

- Se ha creado un archivo README.md para documentar el proyecto.

- Se ha creado un archivo LICENSE para definir la licencia del proyecto.

- Se ha creado una cuenta en AccuWeather para obtener la API Key.[https://developer.accuweather.com/](AccuWeather) [https://developer.accuweather.com/accuweather-locations-api/apis/get/locations/v1/topcities/%7Bgroup%7D](AccuWeather Locations API) [https://developer.accuweather.com/accuweather-forecast-api/apis/get/forecasts/v1/daily/1day/%7BlocationKey%7D](AccuWeather Forecast API)

- Se ha creado un archivo main.py para ejecutar el código.

- Se ha creado un archivo requirements.txt para instalar las dependencias.
  - pip freeze > requirements.txt 
  - pip install -r "./requirements.txt"

- Se ha creado un archivo my_secrets.py para almacenar la API Key y la información para acceder a la BDD.

- Se ha creado un archivo .gitignore para ignorar el archivo my_secrets.py

## Entrega 3

- Se ha creado un archivo docker-compose.yml para crear el contenedor de Docker. [New docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml)
- Se ha creado ETCL_Weather.py en dags para crear el DAG.
- Se ha pasado el código de main.py a ETCL_Weather.py y se ha modificado para que funcione con Airflow.
- Se ha agregado keys al gitignore.
- Se requiere ejecutar el siguiente comando para activar el docker-compose.yml:
  - docker-compose up
- Ahora se puede ejecutar el DAG desde Airflow en localhost:8080
