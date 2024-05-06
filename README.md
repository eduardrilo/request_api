Configuración Inicial

Importación de librerías: 
Se importan las librerías necesarias como requests para hacer peticiones HTTP, pandas para el manejo de datos, 
pyodbc para la conexión con bases de datos SQL, math para cálculos matemáticos y datetime y pytz para manejo de fechas y zonas horarias.

Consulta a la API

Configuración de la URL y autenticación: Se establece la URL de la API y las credenciales para la autenticación básica.
Parámetros de la solicitud: Se define un data dictionary que contiene los IDs de estado que se desean consultar.
Encabezados HTTP: Se especifican los encabezados necesarios para la solicitud, en este caso indicando que el contenido es JSON.
Realización de la solicitud: Se usa requests.get para hacer una solicitud GET a la API con la URL, las credenciales, los encabezados y los datos en formato JSON.
Manejo de la respuesta: Si la respuesta tiene un código de estado 200 (éxito), se extraen los datos en formato JSON y se crea un DataFrame df con los requestIds obtenidos.

Procesamiento de los Datos

División de datos en lotes: Para manejar grandes volúmenes de datos, se divide el DataFrame en lotes de hasta 5000 filas para su procesamiento por separado.
Iteración sobre lotes de datos: Para cada lote de datos, se crea una lista de IDs, se formatea adecuadamente para la consulta siguiente, y se realiza una nueva solicitud GET a la API para obtener detalles de los incidents.

Transformación y Almacenamiento de Datos

Preparación de los datos para inserción en SQL: Se realizan varias transformaciones en el DataFrame para adecuar los datos a los tipos y formatos esperados por la base de datos. Se manejan valores nulos, se ajustan formatos de fecha y se limpian las cadenas de texto.
Conexión a la base de datos SQL: Se establece una conexión con la base de datos usando pyodbc.
Inserción de datos en la base de datos: Se construye una consulta SQL para insertar los datos del DataFrame en la base de datos y se ejecuta esta consulta.
Manejo de la conexión: Se confirma la transacción con commit, se cierran el cursor y la conexión.
