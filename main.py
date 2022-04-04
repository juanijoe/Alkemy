import os
from archivos_fuente import Obtener
from conexion_db import Conexion
from normalizar_db import CSV_to_Table, crear_tablas
from transaccion_db import principal, registros, info_cines, mostrar_datos

clearConsole = lambda: os.system('cls')
opcion=0
while(True):
    clearConsole()
                                                            #menú principal del programa
    print('''                                                   
        BIENVENIDO AL MENU DE SELECCION DE INFORMACION
    
        1)Obtener información actualizada de Museos
        2)Obtener información actualizada de Salas de Cine
        3)Obtener información actualizada de Bibliotecas Populares
        4)Configurar la base de datos
        5)Salir
        
        ''')
    try:
        opcion=int(input(f'Elige una opción: '))
        if opcion == 5:
            print('Salida del Programa...')
            break
        elif opcion<0 or opcion>5:
            input('Opción Incorrecta, elige otra opción')
        else:
            while(opcion==1 or opcion==2 or opcion==3 or opcion==4):
                if opcion==1:
                    try:
                        Conexion.verificar_db()
                    except Exception as e:
                        input(print('Por favor verifica la configuración de conexión a la Base de Datos antes de continuar...'))
                        clearConsole()
                        break
                    clearConsole()
                    print('MUSEOS:'.center(60, '-'))
                    print(f"\n{'1)Descargar y actualizar la información más reciente':<52}"
                          f"\n{'2)Mostrar la información sobre Museos':<37}"
                          f"\n{'3)Volver al menú principal':<26}")
                    opcion1 = int(input(f'\nElige una opción: '))
                    if 0 > opcion1 or opcion1 > 3:
                        print(f'Valor ingresado incorrecto, elige una opción...')
                    elif opcion1 == 1:
                        if Obtener.test_connection():
                                Obtener.get_url('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv','museos')
                                crear_tablas()
                                CSV_to_Table.obtener_info('museos')
                                print(f'Actualizando datos...')
                                principal.from_table('museos')
                                registros.from_table('museos')
                                input(f'La información de museos se actualizó correctamente, presione cualquier tecla para continuar...')
                    elif opcion1 == 2:
                        print('MUSEOS:'.center(60, '-'))
                        mostrar_datos.show_info('Museos')
                        input(print('\nPulse cualquier tecla para continuar...'))
                        mostrar_datos.show_short('Museos')
                        input(print('\nPulse cualquier tecla para regresar...'))
                    elif opcion1 == 3:
                        break
                elif opcion==2:
                    try:
                        Conexion.verificar_db()
                    except Exception as e:
                        input(print('Por favor verifica la configuración de conexión a la Base de Datos antes de continuar...'))
                        clearConsole()
                        break
                    clearConsole()
                    print('SALAS DE CINE:'.center(60, '-'))
                    print(f"\n{'1)Descargar y actualizar la información más reciente':<52}"
                          f"\n{'2)Mostrar la información sobre Salas de Cine':<44}"
                          f"\n{'3)Volver al menú principal':<26}")
                    opcion2 = int(input(f'\nElige una opción: '))
                    if 0 > opcion2 or opcion2 > 3:
                        print(f'Valor ingresado incorrecto, elige una opción...')
                    elif opcion2 == 1:
                        if Obtener.test_connection():
                                Obtener.get_url('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv','salas-de-cine')
                                crear_tablas()
                                CSV_to_Table.obtener_info('salas-de-cine')
                                print(f'Actualizando datos...')
                                principal.from_table('salas_de_cine')
                                registros.from_table('salas_de_cine')
                                info_cines.from_table()
                                input(f'La información de salas de cine se actualizó correctamente, presione cualquier tecla para continuar...')
                    elif opcion2 == 2:
                        print('SALAS DE CINE:'.center(60, '-'))
                        mostrar_datos.show_info('Salas de cine')
                        input(print('\nPulse cualquier tecla para continuar...'))
                        mostrar_datos.show_short('Salas de cine')
                        input(print('\nPulse cualquier tecla para regresar...'))
                    elif opcion2 == 3:
                        break
                elif opcion==3:
                    try:
                        Conexion.verificar_db()
                    except Exception as e:
                        input(print('Por favor verifica la configuración de conexión a la Base de Datos antes de continuar...'))
                        clearConsole()
                        break
                    clearConsole()
                    print('BIBLIOTECAS POPULARES:'.center(60, '-'))
                    print(f"\n{'1)Descargar y actualizar la información más reciente':<52}"
                          f"\n{'2)Mostrar la información sobre Bibliotecas Populares':<52}"
                          f"\n{'3)Volver al menú principal':<26}")
                    opcion3 = int(input(f'\nElige una opción: '))
                    if 0 > opcion3 or opcion3 > 3:
                        print(f'Valor ingresado incorrecto, elige una opción...')
                    elif opcion3 == 1:
                        if Obtener.test_connection():
                                Obtener.get_url('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv','bibliotecas-populares')
                                crear_tablas()
                                CSV_to_Table.obtener_info('bibliotecas-populares')
                                print(f'Actualizando datos...')
                                principal.from_table('bibliotecas_populares')
                                registros.from_table('bibliotecas_populares')
                                input(f'La información de bibliotecas populares se actualizó correctamente, presione cualquier tecla para continuar...')
                    elif opcion3 == 2:
                        print('BIBLIOTECAS POPULARES:'.center(60, '-'))
                        mostrar_datos.show_info('Bibliotecas Populares')
                        input(print('\nPulse cualquier tecla para continuar...'))
                        mostrar_datos.show_short('Bibliotecas Populares')
                        input(print('\nPulse cualquier tecla para regresar...'))
                    elif opcion3 == 3:
                        break
                elif opcion==4:
                    clearConsole()
                    print('CONFIGURAR LA BASE DE DATOS:'.center(60, '-'))
                    print('''
                            1)Configurar una nueva conexión a base de datos(postgreSQL)
                            2)Mostrar Información de la conexión actual 
                            3)Volver al menú principal
                            
                            ''')
                    opcion4 = int(input(f'Elige una opción: '))
                    if 0 > opcion4 or opcion4 > 4:
                        print(f'Valor ingresado incorrecto, elige una opción...')
                    elif opcion4  == 1:
                        print(f'\nIngrese los atributos para la Conexión a la Base de Datos de PostgreSQL')
                        url = Conexion.crear_db(input(f'\nUser: '), input(f'Password: '), input(f'Host: '),
                                                input(f'Port: '), input(f'Database: '))
                    elif opcion4 == 2:
                        try:
                            Conexion.verificar_db()
                            input(print('\nConexión exitosa'))
                        except Exception as e:
                            input(print('\nConexión a la Base de Datos no encontrada o configuración erronea'))
                    if opcion4 == 3:
                        break
    except Exception as e:
        print(input('\nOcurrió un Error, Intente otro valor'))
        clearConsole = lambda: os.system('cls')
        clearConsole()