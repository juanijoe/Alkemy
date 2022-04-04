import pandas as pd
import os
import sys
from logger_base import log
from archivos_fuente import Obtener
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
from decouple import config
from sqlalchemy import Column, Integer, String, Float

Base = declarative_base()                                                   #iniciamos función declarative_base(), herencia de las clase tabla de SQLAlchemy

class Conexion:                                                             #clase conexión controla la configuración de la conexión a la base de datos

    @classmethod
    def config_db(cls,user,password,host,port,db):                          #escribe el archivo .env para almacenar la credenciales de conexión
        with open('.env','w',newline='') as f:                              #este archivo es .gitignore es propio para cada entorno de ejecución
            f.write(f"{'_PG_USER=':<9}{user}"                               #se crea desde cero en caso de no existir
                    f"\n{'_PG_PASSWD=':<11}{password}"
                    f"\n{'_PG_HOST=':<9}{host}"
                    f"\n{'_PG_PORT=':<9}{port}"
                    f"\n{'_PG_DB=':<7}{db}")
        print('Configuración actualizada')

    @classmethod
    def get_config_db(cls):                                                 #método que extrae los credenciales para crear el objeto de conexión
        _PG_USER = config('_PG_USER')                                       #a la base de datos de postgres
        _PG_PASSWD = config('_PG_PASSWD')                                   #utilizamos la función config de la librería decouple para recuperar atributos
        _PG_HOST = config('_PG_HOST')
        _PG_PORT = config('_PG_PORT')
        _PG_DB = config('_PG_DB')
        return f'postgresql://{_PG_USER}:{_PG_PASSWD}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}'


    @classmethod                                                             #se crea el método para inicializar la base de datos desde Python
    def crear_db(cls, usuario, password, host, port, db):
        base_datos = f'postgresql://{usuario}:{password}@{host}:{port}/{db}'
        if not database_exists(base_datos):                                  #consulta si la base ya existe
            create_database(base_datos)                                      #si no es el caso, la crea con los atributos especificados
            input(print(f'\nBase de Datos se creó correctamente'))
        else:
            input(print(f'\nla Base de Datos ya existe'))
        Conexion.config_db(usuario,password,host,port,db)                    #escribimos el archivo .env con la configuración seleccionado
        return base_datos

    @classmethod
    def verificar_db(cls):                                                  #verifica que exista una base de datos a la cual conectar,
        url = Conexion.get_config_db()                                      #antes de ejecutar cualquier acción
        ini = url.index('/',14)
        fin = len(url)
        engine = create_engine(url, pool_size=5, echo=False, pool_pre_ping = True)  #función de SQLAlchemy que crea el pool de conexiones para la db
        Base.metadata.create_all(engine)
        print(f'\nBase de Datos encontrada: {url[ini+1:fin]} ok!')          #en caso de existir, devuelve el nombre y no sobreescribe la base ya existente

if __name__ == '__main__':

        print('archivo soporte de conexión a la base de datos, ejecutar main.py')




