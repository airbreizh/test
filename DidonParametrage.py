#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
#Nom :  : DidonParametrage.py
#Description : Classe de stockage et récupération des parametres nécessaires pour les traitements Didon
#Copyright : 2018, Air Breizh
#Auteur :  Manuel
#Version: 1.0

"""
    Classe définissant le paramétrage pour la récupération des mesures :
    Paramétrage général: 
        Type d'environnement d'éxécution du traitement ("LOCAL" ou "PROD")
        Répertoire de stockage des logs du traitements (selon l'environnement)
        Booleen de desactivation de l'insertion en base (debug)

    Paramétrage de connexion XR et DIDON
        Connexion au serveur XR (utilisateur, mot de passe, serveur et base selon l'environnement)
        Connexion à la base DIDON (utilisateur , mot de passe, serveur, base et schema selon l'environnement) 

    Paramétrage métier
        Fréquences de traitements autorisés ('A', 'M', 'D' et 'H')
        Dictionnaire de correspondance entre fréquences de rééchantillonnage et fréquences autorisées
        Dictionnaire de correspondance entre Taux de représentativité de la donnée et fréquences autorisées
        Dictionnaire de correspondance entre Arrondis utilisés et fréquences autorisées
        Dictionnaire de correspondance entre Tables de données de la base DIDON et fréquences autorisées
        Liste des noms courts de mesures à prendre en considération pour le traitement
        Dernière année à traiter (pour le calcul des moyennes annuelles)
        Nombre d'années à prendre en considération à compter de la dernière années à traiter (pour le calcul des moyennes annuelles)
        Nombre de jours à traiter (pour les calculs sur période glissante : moyennes mensuelles, journalières et horaires)
        Dictionnaire des valeurs de mesures à remplacer et valeur de remplacement
        Dictionnaire de correspondance entre les codes de validité XR et DIDON : 0 ou 1

    """

from  DidonLogger import DidonLogger
import numpy as np

class DidonParametrage: # Définition de la classe DidonParametrage

## ######################################
## var globales
## ENV_LOCAL : permet de definir que le traitement est réalisé en environnement LOCAL
## ENV_PROD : permet de definir que le traitement est réalisé en environnement PROD
## ######################################

    global ENV_LOCAL
    ENV_LOCAL = "LOCAL"
    global ENV_PROD
    ENV_PROD = "PROD"

    global nomClasse
    nomClasse='DidonParametrage'

## ######################################
## constructeur
## ######################################

    def __init__(self, envToParam): # Notre méthode constructeur
        """
            Constructeur de parametrage avec un parametre 'envToParam' :
                - vide : par defaut creation d'un parametrage LOCAL
                - 'LOCAL' : creation d'un parametrage LOCAL
                - 'PROD' : creation d'un parametrage PROD
        """
        nomFonction='__init__'

        self.env = ENV_LOCAL

        self.LABEL_FREQ_A = 'A'
        self.LABEL_FREQ_M = 'M'
        self.LABEL_FREQ_D = 'D'
        self.LABEL_FREQ_H = 'H'

        
        if envToParam == ENV_PROD:
            print('Initialisation du parametrage de PROD')
        elif envToParam == ENV_LOCAL:
            print('Initialisation du parametrage de LOCAL')
        else :
            print('Pas d''environnement précisé, initialisation du parametrage LOCAL')
            
        if envToParam == ENV_PROD:
            ## parametrage de PROD
            self.XRuser = "MC"
            self.XRpwd = "MC"
            self.XRhost = "172.16.29.33"
            self.XRbase = "N"

            self.DIDONuser = "bzh"
            self.DIDONpwd = "@irBREIZH3522"
            self.DIDONhost = "90.88.68.27"

            self.DIDONlogDir = "/home/bzh/didon_scripts/logs/"
        else :
            ## parametrage LOCAL et DEFAUT
            self.XRuser = "MC"
            self.XRpwd = "MC"
            self.XRhost = "172.16.29.33"
            self.XRbase = "N"
        
            self.DIDONuser = "postgres"
            self.DIDONpwd = "postgres"
            self.DIDONhost = "localhost"
            self.DIDONlogDir = "C:/temp/"

        self.DIDONbase = "didon"
        self.DIDONschemaMesure = "mesure"
        
        self.frequencesMesure = { self.LABEL_FREQ_A , \
                                    self.LABEL_FREQ_M , \
                                    self.LABEL_FREQ_D , \
                                    self.LABEL_FREQ_H }

        self.frequencesResampling = { self.LABEL_FREQ_A:'YS' , \
                                        self.LABEL_FREQ_M:'MS' , \
                                        self.LABEL_FREQ_D:'D' , \
                                        self.LABEL_FREQ_H:'H' }

        self.frequencesRepresentativite = { self.LABEL_FREQ_A:85 , \
                                            self.LABEL_FREQ_M:85 , \
                                            self.LABEL_FREQ_D:75 , \
                                            self.LABEL_FREQ_H:75 }

        self.frequenceArrondis = { self.LABEL_FREQ_A:0 , \
                                    self.LABEL_FREQ_M:0 , \
                                    self.LABEL_FREQ_D:1 , \
                                    self.LABEL_FREQ_H:1 }

        self.tablesMesure = { self.LABEL_FREQ_A:'mesure_annuelle' , 
                            self.LABEL_FREQ_M:'mesure_mensuelle' , \
                            self.LABEL_FREQ_D:'mesure_quotidienne' , \
                            self.LABEL_FREQ_H:'mesure_horaire'}

        ## Pour tests
        #self.nomsCourtsMesures  = {'NO2BAL'}
        self.nomsCourtsMesures  = {'NO2BIS', 'NO2BAL', 'NO2CTM', 'NO2CVL', 'NO2DES', 'NO2HAL', 'NO2LAE', 'NO2MAC', 'NO2YVE' , 'NO2_ZOLA', 'NO2UTA', 'NO2_STG','NO2_RBY', \
                                    'NOX_STG',\
                                    'O3_BAL','O3_BIS','O3_CVL','O3_UTA','O3_PAS','O3_YVE','O3_STG','O3_ZOLA','O3_RBY',\
                                    'P10E_STG', 'P10E_LAE','P10E_BIS','P10E_DES','P10E_UTA','P10E_BAL','P10E_MAC','P10E_POM','P10E_PBA','P10E_TRI',\
                                    'P25E_STG','P25E_BIS','P25E_MAC','P25E_LAE','P25E_PBA','P25E_UTA','P10E_RBY',\
                                    'CO_HAL'}

        self.derniereAnneeATraiter = 2017
        #self.derniereAnneeATraiter = 2013
        self.nbAnneesATraiter = 5
        #self.nbAnneesATraiter = 1
        self.nbJoursATraiter = 365

        ## Gestion des valeurs et codes de mesures à ne pas remplacer tel quel dans la base DIDON
        ## mauvaises valeur : 100001.0 -> nan
        ## mauvais code : aucun code à remplacer
        self.remplacementValeurs = {100001.0:np.nan,}
        self.remplacementCodes = {'Z':'0', 'C':'0', 'D':'0', 'M':'0', 'I':'0', 'N':'0',
                                  'A' : '1', 'O' : '1', 'R' : '1', 'P' : '1', 'W' : '1'}

        ## desactivation de l'insertion en base (debug)
        self.isDBinsertionActivated=True
        #self.isDBinsertionActivated=False

        ## Initialisation du logger
        self.didonLogger = None
        self.didonLogger = DidonLogger('DidonGetMesures',self.DIDONlogDir)
    
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Initialisation du parametrage terminée')

        
## ######################################
## accesseurs aux parametres globaux
## ######################################

    def getENV_LOCAL(self):
        """
            Permet de récupérer la valeur du paramètre ENV_LOCAL
        """
        return self.ENV_LOCAL

    def getENV_PROD(self):
        """
            Permet de récupérer la valeur du paramètre ENV_PROD
        """
        return self.ENV_PROD

    def getLABEL_FREQ_A(self):
        """
            Permet de récupérer la valeur du paramètre LABEL_FREQ_A
        """
        return self.LABEL_FREQ_A

    def getLABEL_FREQ_M(self):
        """
            Permet de récupérer la valeur du paramètre LABEL_FREQ_M
        """
        return self.LABEL_FREQ_M

    def getLABEL_FREQ_D(self):
        """
            Permet de récupérer la valeur du paramètre LABEL_FREQ_D
        """
        return self.LABEL_FREQ_D

    def getLABEL_FREQ_H(self):
        """
            Permet de récupérer la valeur du paramètre LABEL_FREQ_H
        """
        return self.LABEL_FREQ_H

    def getXRuser(self):
        """
            Permet de récupérer la valeur du paramètre utilisateur pour XR
        """
        return self.XRuser

    def getXRpwd(self):
        """
            Permet de récupérer la valeur du paramètre mot de passe pour XR
        """
        return self.XRpwd

    def getXRhost(self):
        """
            Permet de récupérer la valeur du paramètre serveur pour XR
        """
        return self.XRhost

    def getXRbase(self):
        """
            Permet de récupérer la valeur du paramètre base pour XR
        """
        return self.XRbase

    def getDIDONuser(self):
        """
            Permet de récupérer la valeur du paramètre utilisateur pour DIDON
        """
        return self.DIDONuser

    def getDIDONpwd(self):
        """
            Permet de récupérer la valeur du paramètre mot de passe pour DIDON
        """
        return self.DIDONpwd

    def getDIDONhost(self):
        """
            Permet de récupérer la valeur du paramètre serveur pour DIDON
        """
        return self.DIDONhost

    def getDIDONbase(self):
        """
            Permet de récupérer la valeur du paramètre base pour DIDON
        """
        return self.DIDONbase

    def getDIDONschemaMesure(self):
        """
            Permet de récupérer la valeur du paramètre schema de mesure pour DIDON
        """
        return self.DIDONschemaMesure
        
    def getFrequencesMesure(self):
        """
            Permet de récupérer la valeur du paramètre liste des fréquences autorisées
        """
        return self.frequencesMesure

    def getTablesMesure(self):
        """
            Permet de récupérer la valeur du paramètre liste des tables autorisées
        """
        return self.tablesMesure

    def getTable(self, frequence):
        """
            Permet de récupérer la valeur de la table en fonction de la fréquence
        """
        return self.tablesMesure[frequence]

    def getDictRepresentativites(self):
        """
            Permet de récupérer la valeur de la table en fonction de la fréquence
        """
        return self.frequencesRepresentativite

    def getRepresentativite(self, frequence):
        """
            Permet de récupérer la valeur du paramètre liste des taux de représentativité autorisés
        """
        return self.frequencesRepresentativite[frequence]

    def getDictFreqResampling(self):
        """
            Permet de récupérer la valeur du paramètre liste des fréquences de rééchantillonnage autorisées
        """
        return self.frequencesResampling

    def getFreqResampling(self, frequence):
        """
            Permet de récupérer la valeur de la fréquence de rééchantillonnage en fonction de la fréquence
        """
        return self.frequencesResampling[frequence]


    def getDictArrondis(self):
        """
            Permet de récupérer la valeur du paramètre liste des arrondis autorisés
        """
        return self.frequenceArrondis

    def getArrondi(self, frequence):
        """
            Permet de récupérer la valeur de l'arrondi en fonction de la fréquence
        """
        return self.frequenceArrondis[frequence]

    def getNomsCourtsMesures(self):
        """
            Permet de récupérer la valeur du paramètre liste des noms courts de mesures
        """
        return self.nomsCourtsMesures
        
    def getDerniereAnneeATraiter(self):
        """
            Permet de récupérer la valeur du paramètre dernières années à traiter
        """
        return self.derniereAnneeATraiter

    def getNbAnneesATraiter(self):
        """
            Permet de récupérer la valeur du paramètre nombre d'années à traiter
        """
        return self.nbAnneesATraiter

    def getNbJoursATraiter(self):
        """
            Permet de récupérer la valeur du paramètre nombre de jour à traiter
        """
        return self.nbJoursATraiter

    def getDidonLogger(self):
        """
            Permet de récupérer la valeur du logger
        """
        return self.didonLogger
        
    def getDictRemplacementValeurs(self):
        """
            Permet de récupérer le dictionnaire de valeurs de remplacements de mesures
        """
        return self.remplacementValeurs

    def remplacerValeur(self, mesure):
        """
            Permet de récupérer la valeur de remplacements d'une mesure
        """
        return self.remplacementValeurs[mesure]

    def getDictRemplacementCodes(self):
        """
            Permet de récupérer le dictionnaire de valeurs de remplacements de codes
        """
        return self.remplacementCodes

    def remplacerCode(self, code):
        """
            Permet de récupérer la valeur de remplacements d'un code
        """
        return self.remplacementCodes[code]

    def getIsDBinsertionActivatedBool(self):
        """
            Permet de retourner la configuration de l'activation de l'insertion en base de données
        """
        return self.isDBinsertionActivated

    def afficheParametrages(self):
        """
            Permet d'afficher le paramétrage
        """
        nomFonction='afficheParametrages'
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Parametres de connexion XR')
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRuser='+str(self.getXRuser()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRpwd='+str(self.getXRpwd()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRhost='+str(self.getXRhost()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRbase='+str(self.getXRbase()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Parametres de connexion DIDON') 
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRuser='+str(self.getDIDONuser()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRpwd='+str(self.getDIDONpwd()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRhost='+str(self.getDIDONhost()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'XRbase='+str(self.getDIDONbase()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Noms courts des mesures pris en compte : '+str(self.getNomsCourtsMesures()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Fréquences de mesure autorisées : '+str(self.getFrequencesMesure()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Parametres de traitement')
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Nb annees à traiter (cas freq=A): '+str(self.getNbAnneesATraiter()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Dernière annee à traiter (cas freq=A): '+str(self.getDerniereAnneeATraiter()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Nb jours à traiter (cas freq=H,J,M) : '+str(self.getNbJoursATraiter()))

        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Arrondis configurés : '+str(self.getDictArrondis()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Taux de representativité configurés : '+str(self.getDictRepresentativites()))
        self.getDidonLogger().ecrireLog( nomClasse, nomFonction,'Fréquences de rééchantillonnage : '+str(self.getDictFreqResampling()))

## ######################################
## Fin de classe
## ######################################

