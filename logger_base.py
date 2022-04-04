import logging as log

log.basicConfig(level=log.DEBUG,    #acá seleccionamos el nivel
                format='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)s] %(message)s',
                datefmt='%I:%M:%S %p',   #estos parámetros para que nos indique que archivo está comunicando
                handlers = [               #y el nivel de logging, número de linea y mensaje
                   log.FileHandler('datos_log.log'),   #agrega la información a un archivo creado
                   log.StreamHandler()
                ])

if __name__ == '__main__':
    #log.debug('mensaje a nivel debug')      #nivel debug es el de mayor nivel de detalle e informaciòn
    #log.info('mensaje a nivel info')
    #log.warning('mensaje a nivel warning')
    #log.error('mensaje a nivel error')
    #log.critical('mensaje a nivel critical')

    print('archivo log para manejo de errores, ejecutar main.py')
