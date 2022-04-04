import csv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from archivos_fuente import Obtener
from sqlalchemy import Column, Integer, String, create_engine

from conexion_db import Conexion

Base = declarative_base()

class CSV_to_Table:           #clase que se encarga de transferir los valores de los registros csv a las tablas de la base de datos

    INSERT_museo = 'INSERT INTO museos (cod_localidad,id_provincia,id_departamento,observaciones,subcategoria,categoria,provincia,localidad,nombre,domicilio,piso,codigo_postal,cod_area,telefono,mail,web,latitud,longitud,tipolatitudlongitud,info_adicional,fuente,jurisdiccion,ano_inauguracion,actualizacion,id_museo)'\
                   'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    INSERT_cine = 'INSERT INTO salas_de_cine (cod_localidad,id_provincia,id_departamento,observaciones,categoria,provincia,departamento,localidad,nombre,domicilio,piso,codigo_postal,cod_area,telefono,mail,web,informacion_adicional,latitud,longitud,tipolatitudlongitud,fuente,tipo_gestion,pantallas,butacas,espacio_incaa,ano_actualizacion,id_cine) ' \
                  'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    INSERT_biblioteca = 'INSERT INTO bibliotecas_populares (cod_localidad,id_provincia,id_departamento,observacion,categoria,subcategoria,provincia,departamento,localidad,nombre,domicilio,piso,codigo_postal,cod_tel,telefono,mail,web,informacion_adicional,latitud,longitud,tipolatitudlongitud,fuente,tipo_gestion,ano_inicio,ano_actualizacion,id_biblioteca) ' \
                        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    insert = ''
    TRUNCATE_museos = 'TRUNCATE museos'
    TRUNCATE_cines = 'TRUNCATE salas_de_cine'
    TRUNCATE_bibliotecas = 'TRUNCATE bibliotecas_populares'

    @classmethod                                    #método que popula o actualiza la información de las tablas con el contenido de los archivos csv creados
    def obtener_info(cls, categoria):
        info = open(f'{categoria}/{Obtener.fecha()}/{categoria}-{Obtener.fecha_dia()}.csv',encoding='utf8')
        lectura = csv.reader(info)
        next(lectura)                                           #obtenemos la información de los registros, y mediante el objeto de conexión engine
        url = Conexion.get_config_db()
        engine = create_engine(url, pool_size=5, echo=False)
        sessionmaker(engine)
        if categoria == 'museos':                               #ejecutamos Truncate según la tabla para poder rellenar con la última información actual
            cls.insert = cls.INSERT_museo
            engine.execute(cls.TRUNCATE_museos)
        elif categoria == 'salas-de-cine':
            cls.insert = cls.INSERT_cine
            engine.execute(cls.TRUNCATE_cines)
        elif categoria == 'bibliotecas-populares':
            cls.insert = cls.INSERT_biblioteca
            engine.execute(cls.TRUNCATE_bibliotecas)
        rows = []
        i = 0
        for row in lectura:
            rows.append(row)
            tupla = rows[i]
            tupla.append(str(i+1))                  #agregamos un campo de variable secuenciado para función de índice
            engine.execute(cls.insert, tupla)       #insertamos los registros indexados con id único
            if categoria == 'museos':
                engine.execute("UPDATE museos SET categoria = 'Museos' WHERE categoria = 'Espacios de Exhibición Patrimonial' OR categoria = ''")
            i += 1                                  #colocamos una categoría homogenea a museos
        info.close()

class museos(Base):                            #creación de la clase tabla museo, se conserva la relación de todos los campos contenidos en el registro
    __tablename__ = 'museos'
    cod_localidad = Column(Integer,nullable=False)
    id_provincia = Column(Integer,nullable=False)
    id_departamento = Column(Integer,nullable=False)
    observaciones = Column(String(500))
    subcategoria = Column(String(60))
    categoria = Column(String(60))
    provincia = Column(String(60))
    localidad = Column(String(70))
    nombre = Column(String(120))
    domicilio = Column(String(150))
    piso = Column(String(50))
    codigo_postal = Column(String(30))
    cod_area = Column(String(10))
    telefono = Column(String(40))
    mail = Column(String(150))
    web = Column(String(300))
    latitud = Column(String(25))
    longitud = Column(String(25))
    tipolatitudlongitud = Column(String(50))
    info_adicional = Column(String(250))
    fuente = Column(String(200))
    jurisdiccion = Column(String(400))
    ano_inauguracion = Column(String(8))
    actualizacion = Column(String(8))
    id_museo = Column(Integer,primary_key=True,nullable=False)    #NOTA IMPORTANTE: SQLAlchemy no admite crear funciones de tabla sin definir campo 'Primary Key'

    def __init__(self,cod_localidad,id_provincia,id_departamento,observaciones,subcategoria,categoria,provincia,localidad,nombre,domicilio,piso,codigo_postal,cod_area,telefono,mail,web,latitud,longitud,tipolatitudlongitud,info_adicional,fuente,jurisdiccion,ano_inauguracion,actualizacion,id_museo):
        self.cod_localidad = cod_localidad
        self.id_provincia = id_provincia
        self.id_departamento = id_departamento
        self.observaciones = observaciones
        self.subcategoria = subcategoria
        self.categoria = categoria
        self.provincia = provincia
        self.localidad = localidad
        self.nombre = nombre
        self.domicilio = domicilio
        self.piso = piso
        self.codigo_postal = codigo_postal
        self.cod_area = cod_area
        self.telefono = telefono
        self.mail = mail
        self.web = web
        self.latitud = latitud
        self.longitud = longitud
        self.tipolatitudlongitud = tipolatitudlongitud
        self.info_adicional = info_adicional
        self.fuente = fuente
        self.jurisdiccion = jurisdiccion
        self.ano_inauguracion = ano_inauguracion
        self.actualizacion = actualizacion
        self.id_museo = id_museo

class cines(Base):                      #creación de la clase tabla salas_de_cine, se conserva la relación de todos los campos contenidos en el registro
    __tablename__ = 'salas_de_cine'
    cod_localidad = Column(Integer, nullable=False)
    id_provincia = Column(Integer, nullable=False)
    id_departamento = Column(Integer, nullable=False)
    observaciones = Column(String(500))
    categoria = Column(String(100))
    provincia = Column(String(60))
    departamento = Column(String(150))
    localidad = Column(String(150))
    nombre = Column(String(200))
    domicilio = Column(String(150))
    piso = Column(String(25))
    codigo_postal = Column(String(25))
    cod_area = Column(String(10))
    telefono = Column(String(15))
    mail = Column(String(150))
    web = Column(String(200))
    latitud = Column(String(30))
    longitud = Column(String(30))
    tipolatitudlongitud = Column(String(80))
    informacion_adicional = Column(String(250))
    fuente = Column(String(200))
    tipo_gestion = Column(String(200))
    pantallas = Column(Integer)
    butacas = Column(Integer)
    espacio_incaa = Column(String(200))
    ano_actualizacion = Column(String(4))
    id_cine = Column(Integer,primary_key=True,nullable=False)  #Misma observación que en clase museos

    def __init__(self,cod_localidad,id_provincia,id_departamento,observaciones,categoria,provincia,departamento,localidad,nombre,domicilio,piso,codigo_postal,cod_area,telefono,mail,web,informacion_adicional,latitud,longitud,tipolatitudlongitud,fuente,tipo_gestion,pantallas,butacas,espacio_incaa,ano_actualizacion,id_cine):
        self.cod_localidad = cod_localidad
        self.id_provincia = id_provincia
        self.id_departamento = id_departamento
        self.observaciones = observaciones
        self.categoria = categoria
        self.provincia = provincia
        self.departamento = departamento
        self.localidad = localidad
        self.nombre = nombre
        self.domicilio = domicilio
        self.piso = piso
        self.codigo_postal = codigo_postal
        self.cod_area = cod_area
        self.telefono = telefono
        self.mail = mail
        self.web = web
        self.latitud = latitud
        self.longitud = longitud
        self.tipolatitudlongitud = tipolatitudlongitud
        self.informacion_adicional = informacion_adicional
        self.fuente = fuente
        self.tipo_gestion = tipo_gestion
        self.pantallas = pantallas
        self.butacas = butacas
        self.espacio_incaa = espacio_incaa
        self.ano_actualizacion = ano_actualizacion
        self.id_cine = id_cine

class bibliotecas(Base):            #creación de la clase tabla bibliotecas_populares, se conserva la relación de todos los campos contenidos en el registro
    __tablename__ = 'bibliotecas_populares'
    cod_localidad = Column(Integer, nullable=False)
    id_provincia = Column(Integer, nullable=False)
    id_departamento = Column(Integer, nullable=False)
    observacion = Column(String(400))
    categoria = Column(String(150))
    subcategoria = Column(String(150))
    provincia = Column(String(80))
    departamento = Column(String(200))
    localidad = Column(String(200))
    nombre = Column(String(200))
    domicilio = Column(String(200))
    piso = Column(String(30))
    codigo_postal = Column(String(25))
    cod_tel = Column(String(10))
    telefono = Column(String(15))
    mail = Column(String(150))
    web = Column(String(150))
    informacion_adicional = Column(String(500))
    latitud = Column(String(30))
    longitud = Column(String(30))
    tipolatitudlongitud = Column(String(100))
    fuente = Column(String(150))
    tipo_gestion = Column(String(150))
    ano_inicio = Column(String(4))
    ano_actualizacion = Column(String(4))
    id_biblioteca = Column(Integer,primary_key=True,nullable=False) #misma observación que en clase museos

    def __init__(self,cod_localidad,id_provincia,id_departamento,observacion,categoria,subcategoria,provincia,departamento,localidad,nombre,domicilio,piso,codigo_postal,cod_tel,telefono,mail,web,informacion_adicional,latitud,longitud,tipolatitudlongitud,fuente,tipo_gestion,ano_inicio,ano_actualizacion,id_biblioteca):
        self.cod_localidad = cod_localidad
        self.id_provincia = id_provincia
        self.id_departamento = id_departamento
        self.observacion = observacion
        self.categoria = categoria
        self.subcategoria = subcategoria
        self.provincia = provincia
        self.departamento = departamento
        self.localidad = localidad
        self.nombre = nombre
        self.domicilio = domicilio
        self.piso = piso
        self.codigo_postal = codigo_postal
        self.cod_tel = cod_tel
        self.telefono = telefono
        self.mail = mail
        self.web = web
        self.informacion_adicional = informacion_adicional
        self.latitud = latitud
        self.longitud = longitud
        self.tipolatitudlongitud = tipolatitudlongitud
        self.fuente = fuente
        self.tipo_gestion = tipo_gestion
        self.ano_inicio = ano_inicio
        self.ano_actualizacion = ano_actualizacion
        self.id_biblioteca = id_biblioteca

def crear_tablas():                                     #método de clase que llama a crear todas las tablas al iniciarse
    url = Conexion.get_config_db()
    engine = create_engine(url, pool_size=5, echo=False)
   # Base.metadata.drop_all(engine)  # limpia todos los registros de tablas que pudieran existir en la base creada
    Base.metadata.create_all(engine)  # inicializa las tablas declaradas(clases)


if __name__ == '__main__':

    print('archivo de soporte para la carga de registros en la base de datos, ejecutar main.py')

    #Nota: se elige como método cargar la información contenido en los archivos fuente en sus respectivas tablas equivalentes en la base de datos, a fin
    #de poder trabajar con la información desde la base de datos, para evitar la apertura y cierre de archivos cada vez que se quiere acceder a los datos
