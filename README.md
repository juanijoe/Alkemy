# Alkemy - Challenge Data Analytics - Python

Con el objetivo de cumplir los requisitos planteados para el desafío, el programa resultante ha sido estructurado de la siguiente forma, conteniendo los archivos:

  * archivos_fuentes.py : archivo de soporte, aquí encontramos las clases y los métodos necesarios para descargar la información de los archivos fuente de registros
  * conexion_db.py : archivo de soporte que gestiona la conexión y validaciones a la base de datos de PostgreSQL
  * logger_base.py : archivo rutinario de manejo del modo debug para el tratamiento de errores en tiempo de ejecución
  * main.py : archivo principal ejecutable que contiene el menú para acceder a todas las funcionalidades del desarrollo. Concentra el acceso tanto a la configuración de la base de datos, como a las funciones de actualización y despliegue de la información. Ejecutar este archivo únicamente.
  *  normalizar_db : archivo de soporte encargado de normalizar y transferir la información contenida en los archivos fuente hacia las tablas contenedoras de la base de datos
  *  transaccion_db: archivo de soporte que administra el traspaso de información dentro de la base de datos y la ejecución de las consultas requeridas

Además de estos archivos principales de programa, se incluye el archivo requierement.txt. El mismo contiene las librerías instaladas para el entorno virtual creado, necesarias para el funcionamiento del programa. Ejecutar comando pip desde consola de Python haciendo uso de este archivo señalador.

Requisitos: 
El desarrollo fue llevado a cabo en Python v3.10 y PostgreSQL v6.4, programas necesarios a ser instalados para el funcionamiento de la aplicación. Adicionalmente, es necesario instalar las librerías de Python listadas en el archivo requierement.

Funcionalidad:

Archivos fuente

El programa se desarrolla en torno a los lineamientos principales planteados. Primeramente se descargan los archivos solicitados. Se guardan el formato .Csv pedido y con la rotulación en forma automática del nombre del archivo y directorio, según la fecha actual. No existe posibilidad de duplicar archivos o directorios, ya que son actualizados cada vez que se requieran. Los directorios se crearán en la raíz de la carpeta que contenga al programa.

Procesamiento de datos

Siguiendo las pautas, la información se procesa y se almacena de acuerdo a los requisitos solicitados. La base de datos se crea integramente desde cero sin necesidad de crearla previamente desde el gestor pgAdmin de PostgreSQL. 

Creación de tablas en la Base de Datos

Existen clases dedicadas para el cumplimiento de esta tarea. Contienen los scripts SQL que ejecutan las acciones requeridas, desde scripts.py. Esto es posible por el uso tanto de la librería SQLAlchemy como de Pandas. Los datos de conexión pueden accederse desde el menú del programa principal para su configuración.

Actualización de la base de datos

Se encuentran implementadas clases específicas que se encargan de actualizar debidamente la información de los registros. Dicha información puede actualizarse en cualquier momento. Se agrega para su control la información de la fecha de carga de la información por cada registro actualizado.
