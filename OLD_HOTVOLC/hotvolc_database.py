#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

import psycopg2

annee = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
         2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035,
         2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049,
         2050]

zones = ['Cameroon', 'CapVert', 'Antilles', 'Islande', 'Rift', 'France', 'Acores', 'East_Africa', 'Italie', 'Canaries',
         'Karthala', 'Reunion']


def DB_connection():
    """Fonction pour se connecter a la BDD Hotvolc"""
    con = None

    try:
        con = psycopg2.connect("host='localhost' dbname='db_hotvolc' user='jdecriem' password='ploufplouf'")
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print
        'Error %s' % e
        sys.exit(1)

    return con


def DB_close(con):
    """Fonction pour couper la connection a la BDD"""

    try:
        if con:
            con.close()
    except psycopg2.DatabaseError, e:
        print
        'Error %s' % e
        sys.exit(1)


def create_tables():
    """Fonction qui permet de creer dans la BDD les tables utiles au SO Hotvolc"""

    con = DB_connection()

    try:

        # Creation de la table chemin
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_path ("
                            "id SERIAL PRIMARY KEY,"
                            "path_xrit VARCHAR(200) NOT NULL,"
                            "path_raw VARCHAR(200) NOT NULL,"
                            "path_products VARCHAR(200) NOT NULL,"
                            "path_colorbars VARCHAR(200) NOT NULL);")

                cur.execute("COMMENT ON TABLE hotvolc_path IS"
                            "'TABLE INPUT: Cette table contient les chemins d acces aux donnees';")
                cur.execute("COMMENT ON COLUMN hotvolc_path.path_xrit IS"
                            "'Chemin d acces aux donnees au format xrit';")
                cur.execute("COMMENT ON COLUMN hotvolc_path.path_raw IS"
                            "'Chemin d acces aux donnees au format raw';")
                cur.execute("COMMENT ON COLUMN hotvolc_path.path_products IS"
                            "'Chemin d acces aux produits hotvolc';")
                cur.execute("COMMENT ON COLUMN hotvolc_path.path_colorbars IS"
                            "'Chemin d acces aux colorbars';")

        # Creation de la Table liste des zones
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_zones ("
                            "id SERIAL,"
                            "zone VARCHAR(50) UNIQUE PRIMARY KEY NOT NULL,"
                            "latmin_sat REAL DEFAULT 0.0,"
                            "latmax_sat REAL DEFAULT 0.0,"
                            "lonmin_sat REAL DEFAULT 0.0,"
                            "lonmax_sat REAL DEFAULT 0.0,"
                            "latmin_merc REAL DEFAULT 0.0,"
                            "latmax_merc REAL DEFAULT 0.0,"
                            "lonmin_merc REAL DEFAULT 0.0,"
                            "lonmax_merc REAL DEFAULT 0.0,"
                            "x_min INT DEFAULT 0,"
                            "x_max INT DEFAULT 0,"
                            "y_min INT DEFAULT 0,"
                            "y_max INT DEFAULT 0,"
                            "ash_cutoff1 REAL DEFAULT 0.5,"
                            "ash_cutoff2 REAL DEFAULT -1.0,"
                            "nti_cutoff_day REAL DEFAULT 1.2,"
                            "nti_cutoff_night REAL DEFAULT 1.2,"
                            "so2_la_cutoff REAL DEFAULT 1.3,"
                            "so2_ha_cutoff REAL DEFAULT -999.0,"
                            "pcs_def_sat VARCHAR(125)"
                            " DEFAULT 'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0',"
                            "xsize_sat INT DEFAULT 0,"
                            "ysize_sat INT DEFAULT 0,"
                            "area_extent_sat VARCHAR(300) DEFAULT '(0.0, 0.0, 0.0, 0.0)',"
                            "pcs_def_merc VARCHAR(125) DEFAULT 'proj=merc, ellps=WGS84, lon_0=0',"
                            "xsize_merc INT DEFAULT 0,"
                            "ysize_merc INT DEFAULT 0,"
                            "area_extent_merc VARCHAR(300) DEFAULT '(0.0, 0.0, 0.0, 0.0)');")

                cur.execute("COMMENT ON TABLE hotvolc_zones IS"
                            "'TABLE INPUT: Cette table contient toutes les informations qui definissent les zones "
                            "d extraction';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.zone IS"
                            "'nom de la zone';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.latmin_sat IS"
                            " 'latitude minimale de la zone en projection satellite';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.latmax_sat IS"
                            " 'latitude maximale de la zone en projection satellite';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.lonmin_sat IS"
                            " 'longitude minimale de la zone en projection satellite';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.lonmax_sat IS"
                            " 'longitude maximale de la zone en projection satellite';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.latmin_merc IS"
                            " 'latitude minimale de la zone en projection mercator';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.latmax_merc IS"
                            " 'latitude maximale de la zone en projection mercator';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.lonmin_merc IS"
                            " 'longitude minimale de la zone en projection mercator';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.lonmax_merc IS"
                            " 'longitude maximale de la zone en projection mercator';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.x_min IS"
                            " 'coordonnee x du coin inf gauche de la zone dans une image MSG de 3712*3712 pixels';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.x_max IS"
                            " 'coordonnee x du coin sup droit de la zone dans une image MSG de 3712*3712 pixels';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.y_min IS"
                            " 'coordonnee y du coin inf gauche de la zone dans une image MSG de 3712*3712 pixels';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.y_max IS"
                            " 'coordonnee y du coin sup droit de la zone dans une image MSG de 3712*3712 pixels';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.ash_cutoff1 IS"
                            " 'Seuil de la BTD[11-12] pour la detection des cendres';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.ash_cutoff2 IS"
                            " 'Seuil de la BTD[8.7-11] pour la detection des cendres';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.nti_cutoff_day IS"
                            " 'Facteur multiplicateur pour calculer le seuil de detection du NTI pendant la journee';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.nti_cutoff_night IS"
                            " 'Facteur multiplicateur pour calculer le seuil de detection du NTI pendant la nuit';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.so2_la_cutoff IS"
                            " 'Seuil pour la detection du SO2 a basse altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.so2_ha_cutoff IS"
                            " 'Seuil pour la detection du SO2 a haute altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.pcs_def_sat IS"
                            " 'Parametres de projection satellite pour le fichier de configuration area.def de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.xsize_sat IS"
                            " 'taille en x de la zone en projection sat pour le fichier de configuration area.def "
                            "de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.ysize_sat IS"
                            " 'taille en y de la zone en projection sat pour le fichier de configuration area.def "
                            "de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.area_extent_sat IS"
                            " 'area extent de la zone en projection sat pour le fichier de configuration area.def"
                            " de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.pcs_def_merc IS"
                            " 'Parametres de projection mercator pour le fichier de configuration area.def de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.xsize_merc IS"
                            " 'taille en x de la zone en projection mercator pour le fichier de configuration area.def"
                            " de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.ysize_merc IS"
                            " 'taille en y de la zone en projection mercator pour le fichier de configuration area.def"
                            " de mpop';")
                cur.execute("COMMENT ON COLUMN hotvolc_zones.area_extent_merc IS"
                            " 'area extent de la zone en projection mercator pour le fichier de configuration area.def"
                            " de mpop';")

        # Creation de la Table liste de tous les volcans
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_volcanoes("
                            "id SERIAL,"
                            "name VARCHAR(50) NOT NULL,"
                            "position GEOGRAPHY(POINT, 4326) NOT NULL,"
                            "elevation INT DEFAULT 0,"
                            "gnvid VARCHAR(50) NOT NULL,"
                            "type VARCHAR(50) NOT NULL,"
                            "status VARCHAR(50) NOT NULL,"
                            "display BOOLEAN DEFAULT FALSE,"
                            "CONSTRAINT volcan_unique PRIMARY KEY (name, gnvid));")

                cur.execute("COMMENT ON TABLE hotvolc_volcanoes IS"
                            " 'TABLE INPUT: Cette table sert à l affichage de tous les volcans sur l interface web';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.name IS"
                            " 'Nom du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.position IS"
                            " 'Position du volcan sous forme de point (lat, lon)';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.elevation IS"
                            " 'Altitude du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.gnvid IS"
                            " 'Id du volcan selon la classification du GVP';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.type IS"
                            " 'Type de volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.status IS"
                            " 'Status de l activite du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes.display IS"
                            " 'Faut il afficher le volcan';")

        # Creation de la table des volcans surveilles
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_volcanoes_monitoring ("
                            "id SERIAL,"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) UNIQUE NOT NULL,"
                            "lat REAL DEFAULT 0.0,"
                            "lon REAL DEFAULT 0.0,"
                            "offsetN_thermal INT DEFAULT 1,"
                            "offsetS_thermal INT DEFAULT 1,"
                            "offsetE_thermal INT DEFAULT 1,"
                            "offsetW_thermal INT DEFAULT 1,"
                            "offsetN_non_volc INT DEFAULT 4,"
                            "offsetS_non_volc INT DEFAULT 4,"
                            "offsetE_non_volc INT DEFAULT 4,"
                            "offsetW_non_volc INT DEFAULT 4,"
                            "offsetN_atmos INT DEFAULT 20,"
                            "offsetS_atmos INT DEFAULT 20,"
                            "offsetE_atmos INT DEFAULT 20,"
                            "offsetW_atmos INT DEFAULT 20,"
                            "volume_calculation BOOLEAN DEFAULT FALSE,"
                            "pix_area REAL DEFAULT 9.0,"
                            "Tamb REAL DEFAULT 300.0,"
                            "Tsurf REAL DEFAULT 973.15,"
                            "h REAL DEFAULT 0.75,"
                            "CONSTRAINT volcan_zone_unique PRIMARY KEY (zone, volcan));")

                cur.execute("COMMENT ON TABLE hotvolc_volcanoes_monitoring IS"
                            "'TABLE INPUT: Cette table contient la liste de tous les volcans observes par HotVolc et "
                            "les parametres necessaires aux calculs des differents produits';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.zone IS "
                            "'Nom de la zone en reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.volcan IS "
                            "'Nom du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.lat IS "
                            "'Latitude du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.lon IS "
                            "'Longitude du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetN_thermal IS "
                            "'Zone volcanique NTI: nombre de pixels vers le nord depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetS_thermal IS "
                            "'Zone volcanique NTI: nombre de pixels vers le sud depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetE_thermal IS "
                            "'Zone volcanique NTI: nombre de pixels vers l est depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetW_thermal IS "
                            "'Zone volcanique NTI: nombre de pixels vers l ouest depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetN_non_volc IS "
                            "'Zone Non Volcanique NTI: nombre de pixels vers le nord depuis la limite nord "
                            "de la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetS_non_volc IS "
                            "'Zone Non Volcanique NTI: nombre de pixels vers le sud depuis la limite sud de "
                            "la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetE_non_volc IS "
                            "'Zone Non Volcanique NTI: nombre de pixels vers l est depuis la limite est "
                            "de la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetW_non_volc IS "
                            "'Zone Non Volcanique NTI: nombre de pixels vers l ouest depuis la limite ouest de "
                            "la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetN_atmos IS "
                            "'Zone atmospherique: nombre de pixels vers le nord depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetS_atmos IS "
                            "'Zone atmospherique: nombre de pixels vers le sud depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetE_atmos IS "
                            "'Zone atmospherique: nombre de pixels vers l est depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.offsetW_atmos IS "
                            "'Zone atmospherique: nombre de pixels vers l ouest depuis le centre du volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.volume_calculation IS "
                            "'Faut-il calculer automatiquement le volume de lave lors d une eruption';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.pix_area IS "
                            "'Aire moyenne des pixels sur la zone';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.Tamb IS "
                            "'Parametre temperature ambiante pour le calcul du volume de lave';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.Tsurf IS "
                            "'Parametre temperature de surface de coulee pour le calcul du volume de lave';")
                cur.execute("COMMENT ON COLUMN hotvolc_volcanoes_monitoring.h IS "
                            "'Parametre epaisseur de coulee pour le calcul du volume de lave';")

        # Creation de la table des enregistrements atmospheriques cette table va rester vide, c'est la table mère
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_atmospheric_monitoring("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "nb_ASH2 INT DEFAULT 0,"
                            "minTb108_ASH2 REAL DEFAULT 0.0,"
                            "minBTD_ASH2 REAL DEFAULT 0.0,"
                            "nb_ASH3 INT DEFAULT 0,"
                            "minTb108_ASH3 REAL DEFAULT 0.0,"
                            "minBTD_ASH3 REAL DEFAULT 0.0,"
                            "nbSO2_LA INT DEFAULT 0,"
                            "maxSRD_SO2_LA REAL DEFAULT 0.0,"
                            "nbSO2_HA INT DEFAULT 0,"
                            "maxSRD_SO2_HA REAL DEFAULT 0.0,"
                            "CONSTRAINT zone_volcan_date_unique PRIMARY KEY (zone, volcan, date));")

                # On commente les différentes colonnes
                cur.execute("COMMENT ON TABLE hotvolc_atmospheric_monitoring IS"
                            "'TABLE OUTPUT: C est dans cette table que sont stockes toutes les 15 minutes les "
                            "enregistrements en relation avec les emissions atmospheriques';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.date IS"
                            " 'Une date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.zone IS"
                            " 'la zone de l enregistrement en reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.volcan IS"
                            " 'le volcan de l enregistrement en reference a la table hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.nb_ASH2 IS"
                            " 'le nombre de pixels contenant de la cendre selon la method bi-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.minTb108_ASH2 IS"
                            " 'Temperature de brillance à 10.8 microns minimale observee avec la methode bi-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.minBTD_ASH2 IS"
                            " 'Difference de temperature de brillance a 10.8 micron minimale observee avec la methode "
                            "bi-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.nb_ASH3 IS"
                            " 'le nombre de pixels contenant de la cendre selon la method tri-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.minTb108_ASH3 IS"
                            " 'Temperature de brillance à 10.8 microns minimale observee avec la methode tri-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.minBTD_ASH3 IS"
                            " 'Difference de temperature de brillance a 10.8 micron"
                            "- 12 micron minimale observee avec la methode "
                            "tri-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.nbSO2_LA IS"
                            " 'Nombre de pixels contenant du SO2 selon la methode de detection basse altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.maxSRD_SO2_LA IS"
                            " 'Difference de radiance spectrale a 10.8 micron"
                            "- 8.7 micron maximale observee pour la detection du SO2 low altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.nbSO2_HA IS"
                            " 'Nombre de pixels contenant du SO2 selon la methode de detection haute altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_atmospheric_monitoring.maxSRD_SO2_HA IS"
                            " 'Difference de radiance spectrale a 10.8 micron"
                            "- 7.3 micron maximale observee pour la detection du SO2 high altitude';")

        # Creation de la table mère des enregistremnts thermiques
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_thermal_monitoring("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "sizeROI INT DEFAULT 9,"
                            "nbanomalie INT DEFAULT 0,"
                            "TSR REAL DEFAULT 0.0,"
                            "r39mean REAL DEFAULT 0.0,"
                            "r39max REAL DEFAULT 0.0,"
                            "r39min REAL DEFAULT 0.0,"
                            "r12mean REAL DEFAULT 0.0,"
                            "r12max REAL DEFAULT 0.0,"
                            "r12min REAL DEFAULT 0.0,"
                            "CONSTRAINT zone_volcan_thermal_unique PRIMARY KEY (zone, volcan, date));")
                cur.execute("COMMENT ON TABLE hotvolc_thermal_monitoring IS"
                            "'TABLE OUTPUT: Cette table permet un suivi toutes les 15 minutes de l etat thermique de "
                            "la zone autour de chaque volcan';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.volcan IS "
                            "'Le nom du volcan de l enregistrement, cette colonne fait reference a la table "
                            "hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.sizeROI IS "
                            "'La taille en pixel de la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.nbanomalie IS "
                            "'Le nombre de pixels anomaliques dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.TSR IS "
                            "'La radiance spectrale totale sur la zone volcanique en cas d anomalie';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r39mean IS "
                            "'La radiance spectrale moyenne a 3.9 microns observee dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r39max IS "
                            "'La radiance spectrale maximale a 3.9 microns observee dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r39min IS "
                            "'La radiance spectrale minimale a 3.9 microns observee dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r12mean IS "
                            "'La radiance spectrale moyenne a 12 microns observee dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r12max IS "
                            "'La radiance spectrale maximale a 12 microns observee dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_monitoring.r12min IS "
                            "'La radiance spectrale minimale a 12 microns observee dans la zone volcanique';")

        # Creation de la table mère anomalies thermique. Les tables hotvolc_thermal_anomalies servent a stocker toutes
        # les informations relatives aux pixels anomaliques détéctés. On va y stocker la date, la zone d'extraction,
        # le nom du volcan associé la position en lat/lon de l'anomalie, la radiance à 3.9µm et 12µm et le nti
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_thermal_anomalies("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "lat REAL DEFAULT 0.0,"
                            "lon REAL DEFAULT 0.0,"
                            "r39 REAL DEFAULT 0.0,"
                            "r12 REAL DEFAULT 0.0,"
                            "nti REAL DEFAULT 0.0,"
                            "CONSTRAINT anomalie_unique PRIMARY KEY (date, lat, lon));")
                cur.execute("COMMENT ON TABLE hotvolc_thermal_anomalies IS"
                            "'TABLE OUTPUT: Cette table sert a stocker toutes les informations relatives aux anomalies"
                            " thermiques detectees';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.volcan IS "
                            "'Le nom du volcan proche de l anomalie, cette colonne fait reference a la table "
                            "hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.lat IS "
                            "'La latitude du pixel anomalique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.lon IS "
                            "'La longitude du pixel anomalique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.r39 IS "
                            "'La radiance spectrale a 3.9 microns du pixel anomalique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.r12 IS "
                            "'La radiance spectrale a 12 microns du pixel anomalique';")
                cur.execute("COMMENT ON COLUMN hotvolc_thermal_anomalies.nti IS "
                            "'Le Normalized Thermal Index du pixel anomalique';")

        # Creation de la table mere hotvolc_legend_label c'est dans ces tables que seront stockees les informations
        # permettant de legender la colorbar des png sur l'affichage web
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_legend_label("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "minASH2 REAL DEFAULT 0.0,"
                            "maxASH2 REAL DEFAULT 0.0,"
                            "minASH3 REAL DEFAULT 0.0,"
                            "maxASH3 REAL DEFAULT 0.0,"
                            "minRAD REAL DEFAULT 0.0,"
                            "maxRAD REAL DEFAULT 0.0,"
                            "minCLOUD REAL DEFAULT 0.0,"
                            "maxCLOUD REAL DEFAULT 0.0,"
                            "coverCLOUD REAL DEFAULT 0.0,"
                            "minSO2_LA REAL DEFAULT 0.0,"
                            "maxSO2_LA REAL DEFAULT 0.0,"
                            "minSO2_HA REAL DEFAULT 0.0,"
                            "maxSO2_HA REAL DEFAULT 0.0,"
                            "CONSTRAINT legend_unique PRIMARY KEY (date, zone));")
                cur.execute("COMMENT ON TABLE hotvolc_legend_label IS "
                            "'TABLE OUTPUT: Cette table sert à stocker les valeurs min et max de chaque produit pour"
                            " les utiliser comme etiquette des colorbars sur l affichage web';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minASH2 IS "
                            "'La valeur min de la BTD[11-12] pour la detection des cendres bi-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxASH2 IS "
                            "'La valeur max de la BTD[11-12] pour la detection des cendres bi-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minASH3 IS "
                            "'La valeur min de la BTD[11-12] pour la detection des cendres tri-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxASH3 IS "
                            "'La valeur max de la BTD[11-12] pour la detection des cendres tri-bandes';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minRAD IS "
                            "'La valeur min de la radiance spectrale a 3.9 micron';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxRAD IS "
                            "'La valeur max de la radiance spectrale a 3.9 micron';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minCLOUD IS "
                            "'La valeur min de la Tb[11] pour la detection des nuages';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxCLOUD IS "
                            "'La valeur max de la Tb[11] pour la detection des nuages';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.coverCLOUD IS "
                            "'La couverture nuageuse sur la zone exprimee en %';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minSO2_LA IS "
                            "'La valeur min de la BTD[11-8.7] pour la detection du SO2 a basse altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxSO2_LA IS "
                            "'La valeur max de la BTD[11-8.7] pour la detection du SO2 a basse altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.minSO2_HA IS "
                            "'La valeur min de la BTD[11-7.3] pour la detection du SO2 a haute altitude';")
                cur.execute("COMMENT ON COLUMN hotvolc_legend_label.maxSO2_HA IS "
                            "'La valeur max de la BTD[11-7.3] pour la detection du SO2 a haute altitude';")

        # Creation de la table mere hotvolc_nti_monitoring qui enregistre la valeur de NTI de chacun des pixels de la
        # zone volcanique
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_nti_monitoring("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "pixID INT DEFAULT 0,"
                            "nti REAL DEFAULT 0.0,"
                            "anomaly BOOLEAN DEFAULT FALSE,"
                            "CONSTRAINT nti_unique PRIMARY KEY (date, zone, volcan, pixID));")
                cur.execute("COMMENT ON TABLE hotvolc_nti_monitoring IS "
                            "'TABLE OUTPUT: Cette table enregistre la valeur de nti de chacun des pixels des "
                            "differentes zones volcaniques';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.volcan IS "
                            "'Le nom du volcan proche de l anomalie, cette colonne fait reference a la table "
                            "hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.pixID IS "
                            "'id du pixel dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.nti IS "
                            "'La valeur de nti du pixel';")
                cur.execute("COMMENT ON COLUMN hotvolc_nti_monitoring.anomaly IS "
                            "'Le pixel est-il anomalique ?';")

        # Creation de la table Quality Flag qui enregistre le cloud cover, la temp moyenne à 10.8µm et le quality flag
        # pour chaque zone volcanique
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_quality_flag("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "Tb108_mean REAL DEFAULT 0,"
                            "Cloud_Cover REAL DEFAULT 0 CONSTRAINT cc_range CHECK (Cloud_Cover >= 0.0 AND "
                            "Cloud_Cover <= 100.0),"
                            "Quality_Flag INT DEFAULT 5 CONSTRAINT qf_range CHECK (Quality_Flag >= 0 "
                            "AND Quality_Flag <= 5), "
                            "CONSTRAINT quality_unique PRIMARY KEY (date, zone, volcan));")
                cur.execute("COMMENT ON TABLE hotvolc_quality_flag IS "
                            "'TABLE OUTPUT: Cette table enregistre des indicateurs de la qualite des donnees';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.volcan IS "
                            "'Le nom du volcan proche de l anomalie, cette colonne fait reference a la table "
                            "hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.Tb108_mean IS "
                            "'La temperature de brillance moyenne a 10.8 µm dans la zone volcanique';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.Cloud_Cover IS "
                            "'La couverture nuageuse de la zone volcanique en %';")
                cur.execute("COMMENT ON COLUMN hotvolc_quality_flag.Quality_Flag IS "
                            "'Le Quality Flag les valeurs vont de 0 à 5 : 0 donnees de mauvaise qualite, 5 tres bonnes"
                            " donnees';")

        for zone in zones:
            # Creation des tables filles une pour chaque zone heritent des tables meres
            sql_create_table_nti = "CREATE TABLE z_hotvolc_nti_%s (" \
                                   "CHECK (zone = '%s')) INHERITS (hotvolc_nti_monitoring);" % (zone, zone)
            sql_alter_table_nti = "ALTER TABLE z_hotvolc_nti_%s " \
                                  "ADD CONSTRAINT nti_%s_zone FOREIGN KEY (zone) " \
                                  "REFERENCES hotvolc_zones(zone), " \
                                  "ADD CONSTRAINT nti_%s_volcan FOREIGN KEY (volcan) " \
                                  "REFERENCES hotvolc_volcanoes_monitoring(volcan), " \
                                  "ADD CONSTRAINT nti_%s_unique PRIMARY KEY (date, zone, volcan, pixID);" % \
                                  (zone, zone, zone, zone)
            sql_create_table_legend = "CREATE TABLE z_hotvolc_legend_%s (" \
                                      "CHECK (zone = '%s')) INHERITS (hotvolc_legend_label);" % (zone, zone)
            sql_alter_table_legend = "ALTER TABLE z_hotvolc_legend_%s " \
                                     "ADD CONSTRAINT legend_%s_zone FOREIGN KEY (zone) " \
                                     "REFERENCES hotvolc_zones(zone), " \
                                     "ADD CONSTRAINT legend_%s_unique PRIMARY KEY (date, zone);" % (zone, zone, zone)
            sql_create_table_anomalies = "CREATE TABLE z_hotvolc_anomalies_%s (" \
                                         "CHECK (zone = '%s')) INHERITS (hotvolc_thermal_anomalies);" % (zone, zone)
            sql_alter_table_anomalies = "ALTER TABLE z_hotvolc_anomalies_%s " \
                                        "ADD CONSTRAINT anomalies_%s_zone FOREIGN KEY (zone) " \
                                        "REFERENCES hotvolc_zones(zone), " \
                                        "ADD CONSTRAINT anomalies_%s_volcan FOREIGN KEY (volcan) " \
                                        "REFERENCES hotvolc_volcanoes_monitoring(volcan), " \
                                        "ADD CONSTRAINT anomalies_%s_unique PRIMARY KEY (date, lat, lon);" % \
                                        (zone, zone, zone, zone)
            sql_create_table_thermal = "CREATE TABLE z_hotvolc_thermal_%s (" \
                                       "CHECK (zone = '%s')) INHERITS (hotvolc_thermal_monitoring);" % (zone, zone)
            sql_alter_table_thermal = "ALTER TABLE z_hotvolc_thermal_%s " \
                                      "ADD CONSTRAINT thermal_%s_zone FOREIGN KEY (zone) " \
                                      "REFERENCES hotvolc_zones(zone), " \
                                      "ADD CONSTRAINT thermal_%s_volcan FOREIGN KEY (volcan) " \
                                      "REFERENCES hotvolc_volcanoes_monitoring(volcan), " \
                                      "ADD CONSTRAINT thermal_%s_unique PRIMARY KEY (zone, volcan, date);" % \
                                      (zone, zone, zone, zone)
            sql_create_table_atmos = "CREATE TABLE z_hotvolc_atmos_%s (" \
                                     "CHECK (zone = '%s')) INHERITS (hotvolc_atmospheric_monitoring);" % (zone, zone)
            sql_alter_table_atmos = "ALTER TABLE z_hotvolc_atmos_%s " \
                                    "ADD CONSTRAINT atmos_%s_zone FOREIGN KEY (zone) REFERENCES hotvolc_zones(zone)," \
                                    "ADD CONSTRAINT atmos_%s_volcan FOREIGN KEY (volcan) " \
                                    "REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                    "ADD CONSTRAINT atmos_%s_unique PRIMARY KEY (zone, volcan, date);" % (zone, zone,
                                                                                                          zone, zone)
            sql_create_table_quality = "CREATE TABLE z_hotvolc_quality_%s (" \
                                       "CHECK (zone = '%s')) INHERITS (hotvolc_quality_flag);" % (zone, zone)
            sql_alter_table_quality = "ALTER TABLE z_hotvolc_quality_%s " \
                                      "ADD CONSTRAINT quality_%s_zone FOREIGN KEY (zone) " \
                                      "REFERENCES hotvolc_zones(zone)," \
                                      "ADD CONSTRAINT quality_%s_volcan FOREIGN KEY (volcan) " \
                                      "REFERENCES hotvolc_volcanoes_monitoring(volcan), " \
                                      "ADD CONSTRAINT quality_%s_unique PRIMARY KEY (zone, volcan, date);" \
                                      % (zone, zone, zone, zone)
            # Creation des tables petites-filles une par annee heritent des tables filles
            sql_create_table_nti_annee = "CREATE TABLE zz_hotvolc_nti_%s_%s (" \
                                         "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00'))" \
                                         "INHERITS (z_hotvolc_nti_%s);" % (zone, '%s', '%s', ' %s', zone)
            sql_create_table_legend_annee = "CREATE TABLE zz_hotvolc_legend_%s_%s (" \
                                            "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00'))" \
                                            " INHERITS (z_hotvolc_legend_%s);" % (zone, '%s', '%s', ' %s', zone)
            sql_create_table_anomalies_annee = "CREATE TABLE zz_hotvolc_anomalies_%s_%s (" \
                                               "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00'))" \
                                               " INHERITS (z_hotvolc_anomalies_%s);" % (zone, '%s', '%s', ' %s', zone)
            sql_create_table_thermal_annee = "CREATE TABLE zz_hotvolc_thermal_%s_%s (" \
                                             "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00')) " \
                                             "INHERITS (z_hotvolc_thermal_%s);" % (zone, '%s', '%s', ' %s', zone)
            sql_create_table_atmos_annee = "CREATE TABLE zz_hotvolc_atmos_%s_%s (" \
                                           "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00'))" \
                                           "INHERITS (z_hotvolc_atmos_%s);" % (zone, '%s', '%s', ' %s', zone)
            sql_create_table_quality_annee = "CREATE TABLE zz_hotvolc_quality_%s_%s (" \
                                             "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00')) " \
                                             "INHERITS (z_hotvolc_quality_%s);" % (zone, '%s', '%s', '%s', zone)

            sql = []
            i = 0

            while i < len(annee):
                if annee[i] == 2050:
                    sql.append((annee[i], annee[i], 2051))
                else:
                    sql.append((annee[i], annee[i], annee[i + 1]))
                i += 1

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_create_table_nti)
                    cur.execute(sql_alter_table_nti)
                    cur.execute(sql_create_table_legend)
                    cur.execute(sql_alter_table_legend)
                    cur.execute(sql_create_table_anomalies)
                    cur.execute(sql_alter_table_anomalies)
                    cur.execute(sql_create_table_thermal)
                    cur.execute(sql_alter_table_thermal)
                    cur.execute(sql_create_table_atmos)
                    cur.execute(sql_alter_table_atmos)
                    cur.execute(sql_create_table_quality)
                    cur.execute(sql_alter_table_quality)

                    cur.executemany(sql_create_table_nti_annee, sql)
                    cur.executemany(sql_create_table_legend_annee, sql)
                    cur.executemany(sql_create_table_anomalies_annee, sql)
                    cur.executemany(sql_create_table_thermal_annee, sql)
                    cur.executemany(sql_create_table_atmos_annee, sql)
                    cur.executemany(sql_create_table_quality_annee, sql)
            del sql

            sql_alter_table_nti_annee = "ALTER TABLE zz_hotvolc_nti_%s_%s " \
                                        "ADD CONSTRAINT nti_%s_zone_%s FOREIGN KEY (zone) " \
                                        "REFERENCES hotvolc_zones(zone)," \
                                        "ADD CONSTRAINT nti_%s_volcan_%s FOREIGN KEY (volcan)" \
                                        " REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                        "ADD CONSTRAINT nti_%s_unique_%s PRIMARY KEY (date, zone, volcan, pixID);" % \
                                        (zone, '%s', zone, '%s', zone, '%s', zone, '%s')
            sql_alter_table_legend_annee = "ALTER TABLE zz_hotvolc_legend_%s_%s " \
                                           "ADD CONSTRAINT legend_%s_zone_%s FOREIGN KEY (zone)" \
                                           " REFERENCES hotvolc_zones(zone)," \
                                           "ADD CONSTRAINT legend_%s_unique_%s PRIMARY KEY (date, zone);" % \
                                           (zone, '%s', zone, '%s', zone, '%s')
            sql_alter_table_anomalies_annee = "ALTER TABLE zz_hotvolc_anomalies_%s_%s " \
                                              "ADD CONSTRAINT anomalies_%s_zone_%s FOREIGN KEY (zone) " \
                                              "REFERENCES hotvolc_zones(zone)," \
                                              "ADD CONSTRAINT anomalies_%s_volcan_%s FOREIGN KEY (volcan) " \
                                              "REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                              "ADD CONSTRAINT anomalies_%s_unique_%s PRIMARY KEY (date, lat, lon);" % \
                                              (zone, '%s', zone, '%s', zone, '%s', zone, '%s')
            sql_alter_table_thermal_annee = "ALTER TABLE zz_hotvolc_thermal_%s_%s" \
                                            " ADD CONSTRAINT thermal_%s_zone_%s FOREIGN KEY (zone) " \
                                            "REFERENCES hotvolc_zones(zone)," \
                                            "ADD CONSTRAINT thermal_%s_volcan_%s FOREIGN KEY (volcan) " \
                                            "REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                            "ADD CONSTRAINT thermal_%s_unique_%s PRIMARY KEY (zone, volcan, date);" % \
                                            (zone, '%s', zone, '%s', zone, '%s', zone, '%s')
            sql_alter_table_atmos_annee = "ALTER TABLE zz_hotvolc_atmos_%s_%s " \
                                          "ADD CONSTRAINT atmos_%s_zone_%s FOREIGN KEY (zone) " \
                                          "REFERENCES hotvolc_zones(zone)," \
                                          "ADD CONSTRAINT atmos_%s_volcan_%s FOREIGN KEY (volcan) " \
                                          "REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                          "ADD CONSTRAINT atmos_%s_unique_%s PRIMARY KEY (zone, volcan, date);" % \
                                          (zone, '%s', zone, '%s', zone, '%s', zone, '%s')
            sql_alter_table_quality_annee = "ALTER TABLE zz_hotvolc_quality_%s_%s " \
                                            "ADD CONSTRAINT quality_%s_zone_%s FOREIGN KEY (zone) " \
                                            "REFERENCES hotvolc_zones(zone)," \
                                            "ADD CONSTRAINT quality_%s_volcan_%s FOREIGN KEY (volcan) " \
                                            "REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                            "ADD CONSTRAINT quality_%s_unique_%s PRIMARY KEY (zone, volcan, date);" % \
                                            (zone, '%s', zone, '%s', zone, '%s', zone, '%s')

            sql = []
            sql2 = []

            for an in annee:
                sql.append((an, an, an, an))
                sql2.append((an, an, an))

            with con:
                with con.cursor() as cur:
                    cur.executemany(sql_alter_table_nti_annee, sql)
                    cur.executemany(sql_alter_table_legend_annee, sql2)
                    cur.executemany(sql_alter_table_anomalies_annee, sql)
                    cur.executemany(sql_alter_table_thermal_annee, sql)
                    cur.executemany(sql_alter_table_atmos_annee, sql)
                    cur.executemany(sql_alter_table_quality_annee, sql)
            del sql
            del sql2

        # Creation de la table mère hotvolc_lava_volume c'est dans ces tables que sont stockes les calculs des volumes
        # de lave en cas d'eruption.
        with con:
            with con.cursor() as cur:
                cur.execute("CREATE TABLE hotvolc_lava_volume("
                            "id SERIAL,"
                            "date TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',"
                            "zone VARCHAR(50) NOT NULL REFERENCES hotvolc_zones(zone),"
                            "volcan VARCHAR(50) NOT NULL REFERENCES hotvolc_volcanoes_monitoring(volcan),"
                            "volume REAL DEFAULT 0.0,"
                            "Tamb REAL DEFAULT 300.0,"
                            "Tsurf REAL DEFAULT 973.15,"
                            "h REAL DEFAULT 0.75,"
                            "CONSTRAINT volume_unique PRIMARY KEY (date, zone, volcan));")
                cur.execute("COMMENT ON TABLE hotvolc_lava_volume IS "
                            "'TABLE OUTPUT: Dans cette table sont stockees les informations relatives au "
                            "volume de lave emis lors d eruption';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.date IS "
                            "'La date au format yyyy-mm-dd HH:MM:SS';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.zone IS "
                            "'La zone de l enregistrement, cette colonne fait reference a la table hotvolc_zones';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.volcan IS "
                            "'Le nom du volcan proche de l anomalie, cette colonne fait reference a la table "
                            "hotvolc_volcanoes_monitoring';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.volume IS "
                            "'Le volume de lave en metres cube';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.Tamb IS "
                            "'La valeur de Temperature Ambiante qui a servie de parametre lors du calcul du volume';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.Tsurf IS "
                            "'La valeur de Temperature de Surface de coulee qui a servie de parametre lors du calcul"
                            "du volume';")
                cur.execute("COMMENT ON COLUMN hotvolc_lava_volume.h IS "
                            "'La valeur de l epaisseur de coulee qui a servie de parametre lors du calcul du volume';")

        # Creation des tables filles de hotvolc_lava_volume *****ATTENTION*******
        # Comme ce produit n'est actuellement applique qu a une seule cible volcanique les tables filles ne sont cette
        # fois ci par divise par zone mais uniquement par annee d enregistrement.

        sql = []
        i = 0
        sql_create_table_volume_annee = "CREATE TABLE z_hotvolc_volume_%s (" \
                                        "CHECK (date >= '%s-01-01 00:00:00' AND date < '%s-01-01 00:00:00')) " \
                                        "INHERITS (hotvolc_lava_volume);"
        while i < len(annee):
            if annee[i] == 2050:
                sql.append((annee[i], annee[i], 2051))
            else:
                sql.append((annee[i], annee[i], annee[i + 1]))
            i += 1

        with con:
            with con.cursor() as cur:
                cur.executemany(sql_create_table_volume_annee, sql)
        del sql

        sql_alter_table_volume_annee = "ALTER TABLE z_hotvolc_volume_%s" \
                                       " ADD CONSTRAINT volume_zone_%s FOREIGN KEY (zone) REFERENCES hotvolc_zones(zone)," \
                                       " ADD CONSTRAINT volume_volcan_%s FOREIGN KEY (volcan)" \
                                       " REFERENCES hotvolc_volcanoes_monitoring(volcan)," \
                                       " ADD CONSTRAINT volume_unique_%s PRIMARY KEY (date, zone, volcan);"

        sql = []
        for an in annee:
            sql.append((an, an, an, an))

        with con:
            with con.cursor() as cur:
                cur.executemany(sql_alter_table_volume_annee, sql)
        del sql

        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        DB_close(con)


def delete_tables():
    """Fonction pour dÃ©truire toutes les tables utiles de la BDD hotvolc"""

    con = DB_connection()

    try:
        # Suppression de la table Enregistrement thermique et des tables qui en heritent
        for zone in zones:
            sql = "DROP TABLE IF EXISTS z_hotvolc_thermal_%s CASCADE;" % zone
            sql1 = "DROP TABLE IF EXISTS z_hotvolc_nti_%s CASCADE;" % zone
            sql2 = "DROP TABLE IF EXISTS z_hotvolc_anomalies_%s CASCADE;" % zone
            sql3 = "DROP TABLE IF EXISTS z_hotvolc_legend_%s CASCADE;" % zone
            sql4 = "DROP TABLE IF EXISTS z_hotvolc_atmos_%s CASCADE;" % zone
            sql5 = "DROP TABLE IF EXISTS z_hotvolc_quality_%s CASCADE;" % zone

            with con:
                with con.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(sql1)
                    cur.execute(sql2)
                    cur.execute(sql3)
                    cur.execute(sql4)
                    cur.execute(sql5)
        with con:
            with con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS hotvolc_thermal_monitoring CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_thermal_anomalies CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_legend_label CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_atmospheric_monitoring CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_nti_monitoring CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_lava_volume CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_zones CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_products_archive CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_volcanoes CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_path CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_volcanoes_monitoring CASCADE;")
                cur.execute("DROP TABLE IF EXISTS hotvolc_quality_flag CASCADE;")

        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        DB_close(con)
