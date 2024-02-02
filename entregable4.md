# Entregable 4

Script de 3ra entrega se deberán añadir alertas y mecanismos de fail-safe.

## Objetivos generales

✅ Partiendo del último entregable, el script ya debería funcionar correctamente dentro de Airflow en un contenedor Docker. En este entregable, añadiremos alertas en base a thresholds de los valores que estemos analizando.

## Objetivos específicos

✅ Incorporar código que envíe alertas mediante SMTP.
✅ Incorporar una forma fácil de configurar thresholds que van a ser analizados para el envío de alertas.

## Formato

✅ Dockerfile y código con todo lo necesario para correr (si es necesario incluir un manual de instrucciones o pasos para correrlo), subido en repositorio de Github o en Google Drive.
✅ Proporcionar screenshot de un ejemplo de un correo envíado habiendo utilizado el código.

## Sugerencias✅

✅ La base de datos donde estará esta tabla no hace falta que viva en el container, sino que se tiene en cuenta que es un Redshift en la nube.
✅ Investigar sobre Docker Compose para facilitar la tarea.
✅ NO añadan ningún tipo de credencial al código. Usen variables de entorno.
✅ Revisar la [rúbrica](https://drive.google.com/file/d/17-A9o-liPw-Fde1lTg4cQD-DOBnQD4OZ/view?usp=sharing)
