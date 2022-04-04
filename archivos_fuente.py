import requests
import os
import sys
from datetime import datetime
from logger_base import log
import shutil

class Obtener:

    @classmethod                                    #clase para testear la conexión al sitio, con uno de los archivos fuente
    def test_connection(cls):
        response = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv')
        if response.status_code == 200:
            return True
        else:
            return False

    @classmethod
    def obtener_mes(cls):  # se debe definir manualmente la conversión de cada mes a Español, para poderse incluir como nombre de directorio
        mes = datetime.today().strftime('%B')
        if mes == 'January':
            mes = 'Enero'
        elif mes == 'February':
            mes = 'Febrero'
        elif mes == 'March':
            mes = 'Marzo'
        elif mes == 'April':
            mes = 'Abril'
        elif mes == 'May':
            mes = 'Mayo'
        elif mes == 'June':
            mes = 'Junio'
        elif mes == 'July':
            mes = 'Julio'
        elif mes == 'August':
            mes = 'Agosto'
        elif mes == 'September':
            mes = 'Septiembre'
        elif mes == 'October':
            mes = 'Octubre'
        elif mes == 'November':
            mes = 'Noviembre'
        elif mes == 'December':
            mes = 'Diciembre'
        return mes

    @classmethod
    def fecha(cls):                                                                 #obtenemos la fecha actual en formato año numérico, mes alfabético
        fecha = datetime.today().strftime('%Y') +'-'+Obtener.obtener_mes()
        return fecha

    @classmethod
    def fecha_dia(cls):                                                             #obtenemos la fecha del día en el formato requerido dd-mm-aa
        fecha_dia = datetime.today().strftime('%d-%m-%Y')
        return fecha_dia

    @classmethod
    def get_url(cls,url,categoria):                                                 #clase para obtener el archivo fuente, con argumentos url y categoría
        try:
            datos = requests.get(url)                                               #descargamos el contenido del archivo con la función requests
            registros_tabla = datos.content                                         #extraemos el contenido a una variable contenedora
        except Exception as e:
            log.error(f'Ocurrió un error en la descarga: {e}')
            sys.exit()
        try:
            if os.path.exists(f'{categoria}'):                                      #para mantener la info siempre actualizada, eliminamos cualquier
                shutil.rmtree(f'{categoria}')                                       #directorio y archivo existente, volviendolo a crear nuevamente
            os.makedirs(f'{categoria}/{Obtener.fecha()}', exist_ok=True)
            archivo = open(f'{categoria}/{Obtener.fecha()}/{categoria}-{Obtener.fecha_dia()}.csv', 'wb')    #iniciamos el archivo .csv
            archivo.write(registros_tabla)                                                                  #volcamos los registros obtenidos
            archivo.close()                                                                                 #guardamos la información tal cual la fuente
        except Exception as e:
            log.error(f'Ocurrió un error al crear el archivo de la categoría')
            sys.exit()
        print(f'Archivo de {categoria} descargado con éxito en la base de datos')

if __name__ == '__main__':

    print(f'archivo de soporte')

