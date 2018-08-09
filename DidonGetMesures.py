#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
#Nom : DidonGetMesures.py
#Description : Recupere les moyennes annuelles XR necessaires pour didon
#Copyright : 2018, Air Breizh
#Auteur :  Manuel
#Version: 1.0

"""
    Classe définissant le traitement de recuperation et stockage dans DIDON des moyennes annuelles
    Usage :
    python DidonGetMesures <frequence> <environnement>
    <frequence> :
        Obligatoire de valeur A (annuel) ou M (mensuel) ou D (quotidien) ou H (horaire)
        Ce paramètre permet de gérer le type de report de mesures souhaité
    <environnement> :
        Facultatif de valeur 'LOCAL' ou 'PROD' (valeur par défaut)
        Ce paramètre permet de gérer le type d'execution (prod ou locale) et donc le parametrage qui en découle
"""

import pyair
from pyair import stats
from pyair import reg
import pandas as pd
import numpy as np
import os, sys, json, datetime, time, calendar
from sys import argv
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
from DidonParametrage import DidonParametrage

class DidonGetMesures:

## ######################################
## Declaration de variable globales
## ######################################

    global LABEL_INITIALISATION
    LABEL_INITIALISATION = 'Initialisation'
    global LABEL_FINALISATION
    LABEL_FINALISATION = 'Finalisation'

    global nomClasse
    nomClasse='DidonGetMesures'


## ######################################
## constructeur
## ######################################

    def __init__(self, envToParam, freqMesure): # Notre méthode constructeur
        """
            Initialisation de la classe DidonGetMesures :
            - initialisation du parametrage (et utilsXr)
        """

        nomFonction='__init__'
        ##Initialisation du paramtrage
        self.parametrage = None
        self.parametrage = DidonParametrage(envToParam)

        ## Declenchement du compteur de temps de traitement
        self.start_time = time.time()
        self.dateTraitement = datetime.datetime.now()
        self.parametrage.getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                    'Execution de l\'exportation des mesures du ' + str(self.getDateTraitement()))
        self.crFinal = '\nTraitement du '+str(self.getDateTraitement())+'\n'

        ## Controle de la valeur du parametre "freqMesuree" par rapport aux valeurs autorisées dans le paramétrage
        if self.isFrequenceAutorisee(freqMesure):
            self.frequenceMesure = freqMesure
            self.tableAUtiliser=self.parametrage.getTable(self.frequenceMesure)
            self.parametrage.getDidonLogger().ecrireLog( nomClasse,nomFonction, \
                                                        'frequence '+freqMesure+' autorisee => table correspondante : '+self.getTableAUtiliser())
        else:
            self.sortieFreqKO(freqMesure)

        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Tentative de connexion à XR')
        try:
            ## Initialisation de la connexion XR
            self.connectXR()
        except Exception as error:
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse,nomFonction,\
                                                            'Erreur de connexion XR : %s'%error,'ERROR')
            self.isXRConnected=False
            sys.exit(0)

        self.getParametrage().getDidonLogger().ecrireLog( nomClasse,nomFonction,\
                                                        'Tentative de connexion à DIDON')
        try:
            ## Initialisation de la connexion DIDON
            self.connectDidon()
        except Exception as error:
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse,nomFonction,\
                                                            'Erreur de connexion DIDON : %s'%error,'ERROR')
            self.isDIDONConnected=False
            sys.exit(0)

        ## Affichage du statut de démarrage du programme
        self.afficheStatut(LABEL_INITIALISATION)

## ######################################
## accesseurs
## ######################################

    def getParametrage(self):
        """
            Permet de récupérer la classe de paramétrage du programme
        """
        return self.parametrage

    def getDateTraitement(self):
        """
            Permet de récupérer la date du traitement calculé à l'initialisation du programme
        """
        return self.dateTraitement

    def getCrFinal(self):
        """
            Permet de récupérer le compte rendu final (chaine de caractères) dans les logs
        """
        return self.crFinal

    def getConnXR(self):
        """
            Permet de récupérer la connexion XR initialisée au début du programme
        """
        return self.connXR

    def getConnDidon(self):
        """
            Permet de récupérer la connexion DIDON initialisée au début du programme
        """
        return self.connDidon

    def getTableAUtiliser(self):
        """
            Permet de récupérer le nom de la table DIDON mise à jour par le programme
        """
        return self.tableAUtiliser

    def getXRConnected(self):
        """
            Permet de récupérer le booleen indiquant si la connexion XR est effective
        """
        return self.isXRConnected

    def getDIDONConnected(self):
        """
            Permet de récupérer le booleen indiquant si la connexion DIDON est effective
        """
        return self.isDIDONConnected
        
## ######################################
## fonctions de traitement
## ######################################

    def isFrequenceAutorisee(self,freq):
        """
            Fonction de controle de la frequence passee en parametre par rapport aux valeurs autorisées dans le paramétrage
            Si 
                la fréquence passée en parametre est présente dans le paramétrage DidonParametrage.frequencesMesure 
            Alors retourne True
            Sinon retourn False
        """
        if freq in self.getParametrage().getFrequencesMesure():
            return True
        else:
            return False
        
    def connectXR(self):
        """
            Fonction de onnexion XR
            Permet d'établir la connexion XR via la classe pyair.xair.XAIR avec le paramétrage de DidonParametrage
            pour un environnement PROD ou LOCAL
        """
        nomFonction='connectXR'

        try:
            ## Initialisation de la connexion XR et de l'indicateur de connexion etablie
            self.connXR = pyair.xair.XAIR(user=self.getParametrage().getXRuser(),\
                                            pwd=self.getParametrage().getXRpwd(),\
                                            adr=self.getParametrage().getXRhost(),\
                                            base=self.getParametrage().getXRbase())
            self.isXRConnected=True
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Connexion XR établie ...')
        except Exception as error:
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Erreur de connexion XR : %s'%error,'ERROR')
            self.isXRConnected=False


    def connectDidon(self):
        """
            Focntion de onnexion à la base DIDON
            Permet d'établir la connexion à la base de données DIDON avec le paramétrage de DidonParametrage
            pour un environnement PROD ou LOCAL
        """

        nomFonction='connectDidon'

        try:
            ## Initialisation de la connexion DIDON et de l'indicateur de connexion etablie
            self.urlDidon = "postgres://{0}:{1}@{2}/{3}".format(self.getParametrage().getDIDONuser(),\
                                                                self.getParametrage().getDIDONpwd(),\
                                                                self.getParametrage().getDIDONhost(),\
                                                                self.getParametrage().getDIDONbase())
            self.schemaMesureDidon = self.getParametrage().getDIDONschemaMesure()
            self.engineDidon   = create_engine(self.urlDidon)
            self.metadataDidon = MetaData(self.engineDidon,schema=self.schemaMesureDidon)
            self.BaseDidon     = declarative_base(metadata=self.metadataDidon)
            self.sessionDidon  = sessionmaker(bind=self.engineDidon)()
            self.isDIDONConnected=True
            class MesureDB(self.BaseDidon):
                __tablename__ = self.getTableAUtiliser()
                __table_args__ = {'autoload':True}
        
            self.mesureDBDidon = MesureDB
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Connexion Didon établie ...')
        except Exception as error:
            self.isDIDONConnected=False
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,
                                                            'Erreur de connexion DIDON : %s'%error,'ERROR')

    def disconnect(self):
        """
            Fonction de déconnexion à la base Didon et au serveur XR
            Permet de couper les deux connexions établies pour les traitements
        """

        nomFonction='disconnect'

        try:
            ## deconnexion DIDON
            self.engineDidon.dispose()
            isDIDONConnected=False
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Deconnexion DIDON effectuee ')

            ## deconnexion XR
            self.connXR.disconnect()
            isXRConnected=False
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Deconnexion XR effectuee ')
        except Exception as error:
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            'Erreur lors de la deconnexion DIDON et/ou XR : %s'%error,'ERROR')

    def afficheStatut(self, etape):
        """
            fonction d'affichage d'un statut permettant de tracer les donnees importantes en début et fin de traitement
            Paramètre : 
                <etape> Chaine de caractères précisant l'étape pour laquelle le statut est souhaité :
                - LABEL_INITIALISATION ('Initialisation') ou 
                - LABEL_FINALISATION = 'Finalisation'
        """

        nomFonction='afficheStatut'

        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'STATUT ('+etape+')')
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        if etape == LABEL_INITIALISATION:
            self.getParametrage().afficheParametrages()
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Fréquence à traiter : '+str(self.frequenceMesure))
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Table à utiliser : '+str(self.getTableAUtiliser()))
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'Etat des connexions')
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Connexion XR : '+str(self.getXRConnected()))
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Connexion DIDON : '+str(self.getDIDONConnected()))
        if etape == LABEL_FINALISATION:
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,self.getCrFinal())
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'')
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'')

    def getMesuresFromXR(self,nomCourt,debut,fin,frequence, arrondi, representativite):
        """
            Fonction de récuperation des données dans XR pour une mesure (nomCourt), une période (debut à fin), une fréquence (H, D, M ou A) 
            et l'arrondi et le taux de représentativité associés à cette fréquence
            Cette fonction permet d'enchainer les differentes étapes :
                - Deux cas de figures principaux : 
                    - 1/ Si la fréquence en entrée est H ou D : récuperation et utilisation telles qu'elles 
                        des données (mesures et codes) horaires ou quotidiennes et traitements suivants :
                        1.1/ récupération de la données avec les parametres <nomcourt>, <debut>, <fin>, et <frequence> d'appel
                            mesures et codes de validation sont récupérés
                        1.2/ contrôle de la structure de données récupérées
                            1.2.1/ elle contient des données
                                - remplacement des valeurs à probleme dans la mesure en fonction du parametrage
                                - remplacement des codes récupérés d'XR par 0 (invalide) ou 1 (valide) en fonction du paramétrage
                                - renommage des colonnes pour préparer la structure de données de sortie
                                - fusion des dataframes de mesures et de codes pour finaliser la structure de données de sortie
                            1.2.2/ elle ne contient pas de donnée
                    - 2/ Sinon (la fréquence en entrée est M ou A) : récupération des données (uniquement mesures) 
                        2.1/ récupération de la données avec les parametres <nomcourt>, <debut>, <fin>, et la fréquence 'HORAIRE'
                            et traitements suivants :
                            la fréquence HORAIRE est utilisée car les mesures M et A sont recalculées et recontrolées par rapport
                            au taux de représentativité (ce qui est automatique dans le cas 1/
                            Seules les mesures sont récupérées (les codes sont recalculés) 
                        2.2/ contrôle de la structure de données récupérées
                            2.2.1/ elle contient des données
                                - remplacement des valeurs à probleme dans la mesure en fonction du parametrage
                                - traitement des arrondis et reechantillonage de la donnée selon la fréquence demandée
                                    Ce traitement implique une nouvelle structure de données rééchantillonnées
                                - calcul des moyennes à la frequence demandée sur la nouvelle structure de données
                                - calcul du tx de representativité :
                                    - ajout d'une colonne avec 1 pour pouvoir réaliser les comptages nécessaires au calcul du taux
                                    - reechantillonnage pour le comptage du nombre de total de mesure et du nombre de mesures valides par fréquence 
                                    - creation d'une nouvelle structure de stockage des ces informations statistiques
                                    - fusion des structures de stockage du rééchantillonage de mesures et de statistiques
                                    - calcul du taux
                                - reaffectation des données rééchantillongées et recalculées pour préparer la structure de données de sortie
                                - renommage des colonnes dans la structure de données de sortie
                                - Ajout d'une colonne 'code_validation' contenant le code de validation avec la valeur 1
                                    (validation non récupérée donc ajoutée explicitement)
                                - Pas de fusion necessaire mais copie pour finalisation de la structure de données de sortie
                                - sélection des données pour lesquelles le comptage est supérieur ou égal au taux de representativité
                            2.2.2/ elle contient pas de donnée
                    - 3/ Controle de la structure de sortie 
                        3.1/ Traitements si la structure de sortie contient des données : 
                            - Ajout d'une colonne 'nom_mes_court' contenant nomCourt
                            - Ajout d'une colonne 'date_mesure' contenant la date de la mesure
                        3.2/ Traitement si la structure de sortie ne contient aucune donnée
                    - 4/ Fin : retour de la structure de sortie
        """

        nomFonction='getMesuresFromXR'
        etapeTravail='debut avec nomCourt='+str(nomCourt)+', debut='+str(debut)+', fin='+str(fin)+\
                        ', frequence='+str(frequence)+', arrondi='+str(arrondi)+' et representativite='+str(representativite)
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

        dataVal, codesVal, dfmerge = None, None, None

        ## Traitement de l'arrondi et remplacement des valeurs de mesures selon la représentativité
        ## TODO cas du C6H6 qu'il faut arrondir à la décimale même si moyenne annuelle

        ## 1/ Si cas HORAIRE ou JOURNALIER : 
        if frequence == self.getParametrage().getLABEL_FREQ_H() or frequence == self.getParametrage().getLABEL_FREQ_D():
            ## 1.1/ Récupération de la données avec les parametres <nomcourt>, <debut>, <fin>, et <frequence> d'appel
            ## mesures et codes de validation sont récupérés
            dataVal, codesVal = self.getConnXR().get_mesures(mes=nomCourt,debut=debut,fin=fin,freq=frequence,brut=True)
            etapeTravail='avant_remplacement_valeurs (cas D ou H)'
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

            ## 1.2/ Contrôle de la structure de données récupérées
            if dataVal.size > 0:
                ## 1.2.1/ Elle contient des données
                ## Remplacement des valeurs à probleme dans la mesure en fonction du parametrage
                for cleVal in self.getParametrage().getDictRemplacementValeurs():
                    dataVal.replace(to_replace=cleVal,value=self.getParametrage().remplacerValeur(cleVal), inplace=True)

                ## Remplacement des codes récupérés d'XR par 0 (invalide) ou 1 (valide) en fonction du paramétrage
                for cleCode in self.getParametrage().getDictRemplacementCodes():
                    codesVal.replace(to_replace=cleCode,value=self.getParametrage().remplacerCode(cleCode), inplace=True)

                etapeTravail='avant_traitement_arrondis_et_representativite(cas D ou H)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

                ## Renommage des colonnes pour préparer la structure de données de sortie
                etapeTravail='avant_renommage colonnes (cas D ou H)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,str(dataVal))
                dataVal.columns = ['valeur_mesure']
                codesVal.columns = ['code_validation']

                ## Fusion des dataframes de mesures et de codes pour finaliser la structure de données de sortie
                etapeTravail='avant_fusion_dataframe (cas D ou H)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'dataVal avant traitement : \n'+str(dataVal))
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'codesVal avant traitement : \n'+str(codesVal))
                dfmerge=dataVal.join(codesVal)
            else:
                ## 1.2.2/ Elle contient des données
                etapeTravail='cas de dataframe vide (cas D ou H)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

        ## 2/ Sinon cas MENSUEL ou ANNUEL : récupération des données (uniquement mesures) horaires et traitement
        else:
            ## 2.1/ Récupération de la données avec les parametres <nomcourt>, <debut>, <fin>, et la fréquence 'HORAIRE'
            ## et traitements suivants :
            ## la fréquence HORAIRE est utilisée car les mesures M et A sont recalculées et recontrolées par rapport
            ## au taux de représentativité (ce qui est automatique dans le cas 1/
            ## Seules les mesures sont récupérées (les codes sont recalculés) 
            dataVal = self.getConnXR().get_mesures(mes=nomCourt,debut=debut,fin=fin,freq=str(self.getParametrage().getLABEL_FREQ_H()))

            ## 2.2/ Contrôle de la structure de données récupérées
            if dataVal.size > 0:
                ## 2.2.1/ Elle contient des données
                etapeTravail='avant_remplacement_valeurs (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

                ## Remplacement des valeurs à probleme dans la mesure en fonction du parametrage
                for cleVal in self.getParametrage().getDictRemplacementValeurs():
                    dataVal.replace(to_replace=cleVal,value=self.getParametrage().remplacerValeur(cleVal), inplace=True)

                ## Traitement des arrondis et reechantillonage de la donnée selon la fréaquence demandée
                ## Ce traitement implique une nouvelle structure de données rééchantillonnées
                etapeTravail='avant_traitement_arrondis_et_representativite(cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

                ## Calcul des moyennes à la frequence demandée sur la nouvelle structure de données
                resampleDF=stats.getRound(dataVal.resample(self.getParametrage().getFreqResampling(frequence)).mean(),arrondi)
                etapeTravail='arrondi et reechantillonage (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

                ## Calcul du tx de representativité :
                ## Ajout d'une colonne avec 1 pour pouvoir réaliser les comptages nécessaires au calcul du taux
                dataVal['compteur']=int(1)
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,str(dataVal))
                resampleDF=resampleDF[resampleDF[nomCourt].notna()]

                ## Reechantillonnage pour le comptage du nombre de total de mesure et du nombre de mesures valides par fréquence 
                ## Creation d'une nouvelle structure de stockage des ces informations statistiques
                comptagesDF=dataVal.resample(self.getParametrage().getFreqResampling(frequence)).count()
                comptagesDF.columns = ['nbMes','nbTot']
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'!!!!!!!!!!!!!!!!!!!'+str(comptagesDF))
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'!!!!!!!!!!!!!!!!!!!'+str(resampleDF))

                ## Fusion des structures de stockage du rééchantillonage de mesures et de statistiques
                resampleDF=resampleDF.join(comptagesDF)
                ## calcul du taux 
                resampleDF['taux']=stats.getRound((resampleDF['nbMes'].astype(pd.np.float)/resampleDF['nbTot'].astype(pd.np.float)*100),0)
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'????????'+representativite+'?????????'+str(resampleDF))

                ## Sélection des données pour lesquelles le comptage est supérieur ou égal au taux de representativité
                resampleDF[nomCourt]=np.select( [resampleDF['taux'] >= float(representativite)], 
                                    [resampleDF[nomCourt]] , 
                                    np.nan)
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'%%%%%%%%%%%%%%%%%%%%%%%'+str(resampleDF))
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,str(dataVal))

                ## Reaffectation des données rééchantillongées et recalculées pour préparer la structure de données de sortie
                dataVal=pd.DataFrame(resampleDF,columns=[nomCourt],index=resampleDF.index)
                ## Renommage des colonnes dans la structure de données de sortie
                etapeTravail='avant_renommage colonnes (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                dataVal.columns = ['valeur_mesure']

                ## Ajout d'une colonne 'code_validation' contenant le code de validation avec la valeur 1
                ## (validation non récupérée donc ajoutée explicitement)
                etapeTravail='avant_ajout_colonnes_code_validation (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                dataVal['code_validation']=np.select( [dataVal['valeur_mesure'].isna()], [0] , 1)

                ## Pas de fusion necessaire mais copie pour finalisation de la structure de données de sortie
                etapeTravail='recopie_dataframe (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'dataVal avant traitement : \n'+str(dataVal))
                dfmerge=dataVal.copy()
            else:
                ## 2.2.2/ Elle contient pas de donnée
                SetapeTravail='cas de dataframe vide (cas M ou A)'
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

        ## 3/ Controle de la structure de sortie
        if (dfmerge is not None and dfmerge.size > 0):
            ## 3.1/ Traitements si la structure de sortie contient des données : 
            ## Ajout d'une colonne 'nom_mes_court' contenant nomCourt
            dfmerge['nom_mes_court']=nomCourt
            ## Ajout d'une colonne 'date_mesure' contenant la date de la mesure
            dfmerge['date_mesure']=dfmerge.index.values
        else:
            ## 3.2/ Traitements si la structure de sortie ne contenant aucune donnée
            etapeTravail='cas de dataframe merge vide'
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

        ## 4/ Fin : retour de la structure de sortie
        etapeTravail='fin'
        # self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail + '\n'+str(dfmerge))
        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
        return dfmerge



    def traiteMesures(self):
        """
            Fonction de traitement et insertion en base de données DIDON 
            Cette fonction permet d'enchainer les differentes étapes :
                - Controle de connexion : si l'une des 2 connexion XR ou Didon n'est pas établie alors arrêt du programme
                    - Connexions Etablies 
                        - Traitements des parametres debut, fin representativite et arrondi (pour appel de getMesuresFromXR)
                        - Boucle sur le nombre de mesures
                            Appel de la fonction getMesuresFromXR permettant de récupérer la structure de données
                                contenant le nom court, la date, la valeur et le code de validité de la mesure
                                pour les paramêtres nomCourt, debut, fin, frequence, arrondi et representativite
                            Controle de la taille de la structure de données recuperee
                            Traitements si la structure contient bien des données :
                                Vérification de la présence de mesures en table DIDON pour la date_mesure. Si oui, suppression avant l'import
                                Si il existe au moins un enregistrement alors suppression de tous les enregistrements
                                Filtrage des mesures sans valeurs et sans code 
                                    (cas des mesures invalidées ou non remontées ie. station KO ou pas de station)
                                    Reconstruire une structure de données ne conservant que les lignes pour lesquelles 
                                    le code de validation est non null
                                Insertion en base de données
                                    l'insertion peut être désactivée par parametrage (pour tests)
                            Traitement si la structure ne contient aucune données
                        - Construction du cr final pour affichage dans les logs
                    - L'une des deux connexions n'est pas établie
                - Deconnexion XR et DIDON
                - Affichage du statut de démarrage du programme

        Le traitement n'est réalisé que si les connexions XR et DIDON sont bien établies
        """

        nomFonction='traiteMesures'

        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                        'Debut du traitement avec connexion XR : '+str(self.getXRConnected())+\
                                                        ' et connexion DIDON : '+str(self.getDIDONConnected()))

        ## Controle de connexion : si l'une des 2 connexion XR ou Didon n'est pas établie alors arrêt du programme
        if (self.getDIDONConnected() and self.getXRConnected()):
            ## Connexions établies
            ## Traitements des parametres debut, fin representativite et arrondi (pour appel de getMesuresFromXR)
            arrondi=self.getParametrage().getArrondi(self.frequenceMesure)
            representativite=str(self.getParametrage().getRepresentativite(str(self.frequenceMesure)))
            ## Cas A :
            ##      date début = 01/01 de la derniereAnneeATraiter  (paramétrage) - NbAnneeATraiter (paramétrage)
            ##      date fin = 31/12 de la derniereAnneeATraiter
            ## Cas M :
            ##      date début = date du traitement - le nbJrAtraiter (paramétrage)
            ##      date fin = date de fin du mois de la date de traitement
            ## Cas D :
            ##      date début = date du traitement - le nbJrAtraiter (paramétrage)
            ##      date fin = date du traitement
            ## Cas H :
            ##      date début = date du traitement - le nbJrAtraiter (paramétrage)
            ##      date fin = date du traitement
            debut=''
            fin=''
            if str(self.frequenceMesure) == self.getParametrage().getLABEL_FREQ_A():
                debut = '01/01/' + str(self.getParametrage().getDerniereAnneeATraiter() - \
                                        self.getParametrage().getNbAnneesATraiter() + 1)
                fin = '31/12/' + str(self.getParametrage().getDerniereAnneeATraiter())
            elif str(self.frequenceMesure) == self.getParametrage().getLABEL_FREQ_M():
                debut = str(self.getDateTraitement() - datetime.timedelta(self.getParametrage().getNbJoursATraiter()))
                firstdayOfMonth, lastdayOfMonth=calendar.monthrange(self.getDateTraitement().year, self.getDateTraitement().month)
                fin = str(lastdayOfMonth)+'/'+str(self.getDateTraitement().month)+'/'+str(self.getDateTraitement().year)
            elif str(self.frequenceMesure) == self.getParametrage().getLABEL_FREQ_D():
                debut = str(self.getDateTraitement() - datetime.timedelta(self.getParametrage().getNbJoursATraiter()))
                fin = fin = str(self.getDateTraitement())
            elif str(self.frequenceMesure) == self.getParametrage().getLABEL_FREQ_H():
                debut = str(self.getDateTraitement() - datetime.timedelta(self.getParametrage().getNbJoursATraiter()))
                fin = fin = str(self.getDateTraitement())
            else :
                self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                                'Traitement impossible - choix de la frequence erroné')
                self.crFinal += 'Erreur sur le choix de la frequence : '+ str(self.frequenceMesure)
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                            '[debut,fin]=['+debut+','+fin+'] ; freq='+str(self.frequenceMesure)+\
                                                            ' et arrondi='+str(arrondi))

            ## Boucle sur le nombre de mesures
            mesuresNonTraitees = []
            mesuresTraitees = []
            for nomCourt in self.getParametrage().getNomsCourtsMesures():

                try:
                    ##creation d'une variable utilisee en cas d'exception
                    etapeTravail='etape_avant_get_mesure'
                    self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail+' nomCourt='+str(nomCourt))

                    ## Appel de la fonction getMesuresFromXR permettant de récupérer la structure de données
                    ## contenant le nom court, la date, la valeur et le code de validité de la mesure
                    ## pour les paramêtres nomCourt, debut, fin, frequence, arrondi et representativite
                    valEtCodes = self.getMesuresFromXR(nomCourt,debut,fin,str(self.frequenceMesure), arrondi, representativite)
                    etapeTravail='etape_apres_get_mesure'
                    self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)

                    ## Controle de la taille de la structure de données recuperee
                    if (valEtCodes  is not None and valEtCodes.size > 0):
                        ## Traitements si la structure contient bien des données :
                        ## Vérification de la présence de mesures en table DIDON pour la date_mesure. Si oui, suppression avant l'import
                        etapeTravail='avant_controle_donnes_existantes'
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                        records = self.sessionDidon.query(self.mesureDBDidon).\
                                filter(self.mesureDBDidon.date_mesure.in_(valEtCodes.date_mesure),
                                    self.mesureDBDidon.nom_mes_court.in_(valEtCodes.nom_mes_court)).first()

                        if records is not None:
                            ## Si il existe au moins un enregistrement alors suppression de tous les enregistrements
                            etapeTravail='avant_recuperation_nb_donnees_existantes'
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                            nb = self.sessionDidon.query(self.mesureDBDidon).\
                                filter(self.mesureDBDidon.date_mesure.in_(valEtCodes.date_mesure),
                                    self.mesureDBDidon.nom_mes_court.in_(valEtCodes.nom_mes_court)).count()
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,str(nb)+' mesures déja existantes dans la table ' + \
                                                                            str(self.getTableAUtiliser()) + ' pour ' + str(nomCourt))

                            etapeTravail='avant_supression_donnees_existantes'
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                            self.sessionDidon.query(self.mesureDBDidon).\
                                filter(and_(self.mesureDBDidon.date_mesure.in_(valEtCodes.date_mesure),
                                        self.mesureDBDidon.nom_mes_court.in_(valEtCodes.nom_mes_court))).delete(synchronize_session=False)
                            self.sessionDidon.commit()
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'Suppression des mesures '+ str(nomCourt) +' dans ' + \
                                                                            str(self.getTableAUtiliser()) + ' avant ré-injection terminée')
                        else:
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'Pas de suppression à realiser')

                        ## Filtrage des mesures sans valeurs et sans code 
                        ## (cas des mesures invalidées ou non remontées ie. station KO ou pas de station)
                        ## Reconstruire une structure de données ne conservant que les lignes pour lesquelles 
                        ## le code de validation est non null
                        etapeTravail='avant_retrait_donnes_sans_mesure'
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                        valEtCodes=valEtCodes[valEtCodes['code_validation'].notna()]
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'apres nettoyage : \n'+str(valEtCodes))

                        ## Insertion en base de données
                        ## l'insertion peut être désactivée par parametrage (pour tests)
                        etapeTravail='avant_insertion_nouvelles_donnees'
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,etapeTravail)
                        if self.getParametrage().getIsDBinsertionActivatedBool() == True:
                            valEtCodes.to_sql(self.getTableAUtiliser(),self.engineDidon, schema='mesure', if_exists='append', index=False, index_label='date_mesure')
                        else:
                            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'!! DEBUG !! !! DEBUG !! !! DEBUG !!insertion en base desactivee !! DEBUG !! !! DEBUG !! !! DEBUG !!')
                        mesuresTraitees.append(nomCourt)
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'dataframe insérées en base DIDON ')
                    else:
                        ## Traitement si la structure ne contient aucune donnée
                        mesuresNonTraitees.append(nomCourt)
                        self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                                        'nomCourt='+str(nomCourt)+' : aucune donnee recuperee depuis XR')
                except Exception as error:
                    self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                                    'Erreur lors des traitements en base DIDON du dataframe en base DIDON : '+etapeTravail,'ERROR')
                    self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,\
                                                                    'Exception levée : %s'%error,'ERROR')
                    print(error)

            ## Construction du cr final pour affichage dans les logs
            self.crFinal += 'Sur frequence = "' + self.frequenceMesure + '" (table mise a jour : '+self.getTableAUtiliser()+') \nDate de debut : '+\
                            debut+'\nDate de fin : '+fin+ '\nDurée de traitement : %s seconds ' % round((time.time() - self.start_time))+'\n' + \
                            str(len(mesuresTraitees))+' mesures traitées sur '+str(len(self.getParametrage().getNomsCourtsMesures()))+'\n'

            self.crFinal +=  'Mesures traitees : '+str(mesuresTraitees)+'\n'
            if len(mesuresNonTraitees) > 0:
                self.crFinal +=  'Mesures non traitees : '+str(mesuresNonTraitees)
        else :
            ## L'une des deux connexions n'est pas établie
            self.getParametrage().getDidonLogger().ecrireLog( nomClasse, nomFonction,'Traitement impossible connexion XR et/ou Didon non établie')
            self.crFinal += 'Erreur sur les connexions XR et/ou DIDON\nConnexion XR : ' + str(self.getXRConnected()) + \
                            '\nConnexion DIDON : ' + str(self.getDIDONConnected())

        ## Deconnexion XR et DIDON
        self.disconnect()
        ## Affichage du statut de démarrage du programme
        self.afficheStatut(LABEL_FINALISATION)


    def sortieFreqKO(self,freqMesure):
        self.parametrage.getDidonLogger().ecrireLog( nomClasse,'sortieFreqKO','frequence '+freqMesure+' interdite => fin de programme')
        sys.exit(0)

## ######################################
## fonction principale
## ######################################

'''
    Usage explanation
'''
def Usage():
    print ("Argument manquant.") 
    print ("Usage:")
    print (argv[0] + ' <frequenceDemandée> [A|M|J|H] <environnement> [default PROD|LOCAL]')    
    exit()

'''
    main
'''

if __name__ == "__main__":

    print('Execution du script : ' + str(__file__) + ' ... début')

    freqDemandee = None
    if len(argv) < 2: 
        Usage()
    ## Recuperation de la frequence (obligatoire)
    freqDemandee = argv[1]

    ## Recuperation de l'environnement (facultatif)
    env='PROD'
    if len(argv)==3 :
        if str(argv[2]) == 'LOCAL':
            env = 'LOCAL'

    ## Instanciation de la classe de traitement
    dgm = DidonGetMesures(env, freqDemandee)

    ## Appel du traitement
    dgm.traiteMesures()

    print('Execution du script : ' + str(__file__) + ' ... fin')
