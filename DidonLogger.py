#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
#Nom :  : DidonLogger.py
#Description : Classe de définition d'un logger 
#Copyright : 2018, Air Breizh
#Auteur :  Manuel
#Version: 1.0

"""
    Classe définissant le logger Didon :
    Paramétrage général: 
        Type d'environnement d'éxécution du traitement ("LOCAL" ou "PROD")
    Le handler INFO implique la rotation du fichier de log quotidiennement alors 
    que le handler ERROR implique la création d'un nouveau fichier à chaque execution
    du logger.
"""

import datetime
import logging
import logging.handlers
import os

class DidonLogger:

## ######################################
## constructeur
## ######################################

    def __init__(self, programme, logDir):

        """
            Fonction d'initialisation du logger
            Fonction permettant d'initialiser le logger :
                - deux 'handlers' définis pour INFO et ERROR
                - un 'formatter' de gestion des format de traces
                - une méthodes de trace specifique
                - un répertoire de stockage des logs

        """
        print('DidonLogger.__init__ : initialisation pour '+str(programme))

        dateTraitement = datetime.datetime.now()
        timestamp = str(dateTraitement.year) + str(dateTraitement.month) + str(dateTraitement.day) 
        timestamp +="_" + str(dateTraitement.hour) + str(dateTraitement.minute) + str(dateTraitement.second)

        didonformatter = None
        didonformatter = logging.Formatter("%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s")

        if os.path.exists(logDir) == False:
            print('Probleme de choix du repertoire de log : ' + logDir + ' inexistant')
            logDir=""

        ##didonHandlerInfo = logging.FileHandler(logDir+programme+"_info_"+timestamp+".log", mode="a", encoding="utf-8")
        didonHandlerInfo = None
        #didonHandlerInfo = logging.FileHandler(logDir+programme+"_info_.log", mode="w", encoding="utf-8")
        didonHandlerInfo = logging.handlers.TimedRotatingFileHandler(logDir+programme+"_info_.log", when='D',interval=1, backupCount=10)
        didonHandlerInfo.suffix = "%Y-%m-%d" # or anything else that strftime will allow
        didonHandlerError = None
        didonHandlerError = logging.FileHandler(logDir+programme+"_error_"+timestamp+".log", mode="w", encoding="utf-8")

        didonHandlerError.setFormatter(didonformatter)
        didonHandlerInfo.setFormatter(didonformatter)

        didonHandlerInfo.setLevel(logging.INFO)
        didonHandlerError.setLevel(logging.ERROR)
        didonHandlerError.setLevel(logging.CRITICAL)

        self.logger = logging.getLogger(programme)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(didonHandlerError)
        self.logger.addHandler(didonHandlerInfo)

## ######################################
## fonctions utils
## ######################################

    def ecrireLog(self, origine, fonction, message, level='INFO'):
        """
            Fonction d'ecriture de log prenant en parametre :
            - origine : classe ou script appelant
            - fonction : fonction appelante
            - message : message à tracer
            - level : niveau de trace optionnel ('INFO' par défaut)
        """
        if origine == '':
            origine = 'Undefined'
        
        if fonction == '':
            origine = 'Undefined'
                
        if message == '':
            message = '######################################'
        
        msgFinal = str(origine)+'.'+str(fonction)+' : '+str(message)

        if level == 'ERROR' or level == 'CRITICAL':
            self.logger.error(msgFinal)
        else :
            self.logger.info(msgFinal)
