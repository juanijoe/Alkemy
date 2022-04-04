from datetime import datetime
import pandas
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from conexion_db import Conexion


Base = declarative_base()

class principal():                  #clase que crea la tabla principal solicitada como requisito del ejercicio

    @classmethod
    def from_table(cls,categoria):
        selects = 'SELECT * FROM ' + categoria          #requerimos la información de cada categoria comunicada en el argumento
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        sessionmaker(engine)                                        #iniciamos la sesión para la transferencia mendiante esta función de SQLAlchemy
        lectura = engine.execute(selects).fetchall()                #el puntero que ejecuta el SELECT devuelve su contenido
        df = pd.DataFrame(columns = ['cod_localidad', 'id_provincia', 'id_departamento',
                           'categoria', 'provincia', 'localidad', 'nombre',
                           'domicilio', 'codigo_postal', 'cod_area',                    #utilizando Pandas creamos el Dataframe para contener los
                           'telefono', 'mail', 'web','fecha_actualizacion'])            #registros fetchados
        df.to_sql('principal', con=engine, if_exists='append', index=False)             #función que inicializa la tabla con el Dataframe especificado
        if categoria == 'museos':
            engine.execute("DELETE FROM principal WHERE categoria = 'Museos'")          #en caso de existir previamente, cada tabla es reseteada
        elif categoria == 'salas_de_cine':                                              #para evitar añadir registros de información duplicada
            engine.execute("DELETE FROM principal WHERE categoria = 'Salas de cine'")
        elif categoria == 'bibliotecas_populares':
            engine.execute("DELETE FROM principal WHERE categoria = 'Bibliotecas Populares'")
        for row in lectura:
            serie = row
            fecha = datetime.today().strftime('%d-%m-%Y %H:%M:%S')                      #generamos la fecha de última actualización de registros
            df = pd.DataFrame({'cod_localidad': serie[0], 'id_provincia': serie[1], 'id_departamento': serie[2],
                               'categoria': serie[4], 'provincia': serie[6], 'localidad': serie[7], 'nombre': serie[8],
                               'domicilio': serie[9], 'codigo_postal': serie[11], 'cod_area': serie[12],
                               'telefono': serie[13], 'mail': serie[14], 'web': serie[15],'fecha_actualizacion':fecha}, index=[0])
            df.to_sql('principal', con=engine, if_exists='append', index=False)
        if categoria == 'museos':
            engine.execute("UPDATE principal SET categoria = 'Museos' WHERE categoria = 'Espacios de Exhibición Patrimonial' OR categoria = ''")

class registros():              #clase que crea la tabla registros solicitada en el ejercicio

    @classmethod
    def from_table(cls, categoria):
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        sessionmaker(engine)                                  #consulta SELECT que nos devuelve las cantidades solicitadas por clasificación
        selects = 'select distinct categoria, count(categoria) over (partition by categoria) as total_'+categoria+'\
                          ,fuente, count (fuente) over (partition by fuente) as total_por_fuente\
                          ,provincia, count(provincia) over(partition by provincia) as total_por_provincia from ' + categoria + ' order by provincia,fuente desc'
        df = pd.DataFrame(columns=['categoria','total_categoria','fuente','total_por_fuente','provincia','total_por_provincia'])
        df.to_sql('registros', con=engine, if_exists='append', index=False)
        if categoria == 'museos':
            engine.execute("DELETE FROM registros WHERE categoria = 'Museos'")          #reseteamos la tabla en caso de volver a actualizar la información
        elif categoria == 'salas_de_cine':
            engine.execute("DELETE FROM registros WHERE categoria = 'Salas de cine'")
        elif categoria == 'bibliotecas_populares':
            engine.execute("DELETE FROM registros WHERE categoria = 'Bibliotecas Populares'")
        lectura = engine.execute(selects).fetchall()
        for row in lectura:
            serie = row
            df = pd.DataFrame({'categoria':serie[0],'total_categoria':serie[1],'fuente':serie[2],'total_por_fuente':serie[3],'provincia':serie[4],'total_por_provincia':serie[5]},index=[0])
            df.to_sql('registros',con=engine,if_exists='append',index=False)


class info_cines():             #clase que crea la tabla para la información de salas de cine solicitada en el ejercicio

    @classmethod
    def from_table(cls):
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        sessionmaker(engine)                                    #SELECT de estructura WITH compleja que devuelve los registros solicitados
        selects = 'with total_cines as(' \          
                                    'select distinct provincia,count(provincia) as Total_provincia,' \
                                    'sum(pantallas) as Total_pantallas,' \
                                    'sum(butacas) as Total_butacas ' \
                                    'from salas_de_cine ' \
                                    'group by provincia ' \
                                    'order by provincia' \
                                    '), ' \
                       'total_espacios as(' \
                                    'select distinct provincia,count(espacio_incaa) as Todos ' \
                                    'from salas_de_cine ' \
                                    'group by provincia ' \
                                    'order by provincia ' \
                                    "), " \
                       'total_incaa as (' \
                                    'select distinct provincia, count(espacio_incaa) as Solo_Incaa ' \
                                    'from salas_de_cine ' \
                                    "where espacio_incaa = '' " \
                                    'group by provincia ' \
                                    'order by provincia) '\
                  "select tc.provincia,tc.total_provincia,tc.total_pantallas,tc.total_butacas, " \
                  "(te.Todos - ti.Solo_Incaa) as Espacios_Incaa " \
                  "from total_cines tc " \
                  "inner join total_espacios te " \
                  "on te.provincia = tc.provincia " \
                  "inner join total_incaa ti " \
                  "on ti.provincia = tc.provincia "
        df = pd.DataFrame(columns=['provincia','total_provincia','total_pantallas','total_butacas','Total_Espacios_INCAA'])
        df.to_sql('info_cines', con=engine, if_exists='append', index=False)
        engine.execute('TRUNCATE info_cines')                                   #cabe destacar que no todas las provincias cuentan con Espacios_INCAA
        lectura = engine.execute(selects).fetchall()                            #pero deben mostrar su información de todas maneras
        for row in lectura:
            serie = row
            df = pd.DataFrame({'provincia':serie[0],'total_provincia':serie[1],'total_pantallas':serie[2],'total_butacas':serie[3],'Total_Espacios_INCAA':serie[4]},index=[0])
            df.to_sql('info_cines',con=engine,if_exists='append',index=False)


class mostrar_datos:                #clase que muestra en salida de pantalla la información recopilada de cada categoria

    @classmethod
    def show_info(cls,categoria):               #este método devuelve la información completa de todos los registros existentes por categoría
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        datos = pandas.read_sql(f"select * from principal where categoria='{categoria}' order by provincia,localidad,nombre",con=engine)
        print(datos)

    @classmethod
    def show_short(cls,categoria):              #este método devuelve la información sumaria agrupada en totales clasificados en la tabla registros
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        if categoria == 'Salas de cine':
            datos = pandas.read_sql(f"select * from info_cines", con=engine)    #para el caso de salas de cine muestra la propia información sumaria
            print(datos)                                                        #registrada en la tabla pedida en el ejercicio
        else:
            datos = pandas.read_sql(f"select * from registros where categoria='{categoria}'", con=engine)
            print(datos)

if __name__ == '__main__':

    print('archivo de soporte para la tablas de información requeridas')




