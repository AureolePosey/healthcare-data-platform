import logging
import os


def setup_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')

    #creation du formater
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    #handle pour les fichiers de logs
    file_handler = logging.FileHandler('logs/app.log')
    file_handler.setFormatter(formatter)
    #handle pour la console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    #configuration du logger
    logger = logging.getLogger('app_logger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger