#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

import hotvolc_database
import psycopg2


def hotvolc_supp_function():
    """Suppression des fonctions UPERT_INTO"""
    sql_atmos = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_atmospheric_monitoring(" \
                "f_date TIMESTAMP, " \
                "f_zone VARCHAR(50), " \
                "f_volcan VARCHAR(50), " \
                "f_nb_ASH2 INT, " \
                "f_minTb108_ASH2 REAL, " \
                "f_minBTD_ASH2 REAL, " \
                "f_nb_ASH3 INT, " \
                "f_minTb108_ASH3 REAL, " \
                "f_minBTD_ASH3 REAL, " \
                "f_nbSO2_LA INT, " \
                "f_maxSRD_SO2_LA REAL," \
                "f_nbSO2_HA INT," \
                "f_maxSRD_SO2_HA REAL);"
    sql_thermal = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_thermal_monitoring(" \
                  "f_date TIMESTAMP, " \
                  "f_zone VARCHAR(50), " \
                  "f_volcan VARCHAR(50), " \
                  "f_sizeROI INT, " \
                  "f_nbanomalie INT, " \
                  "f_TSR REAL, " \
                  "f_r39mean REAL, " \
                  "f_r39max REAL, " \
                  "f_r39min REAL, " \
                  "f_r12mean REAL, " \
                  "f_r12max REAL, " \
                  "f_r12min REAL);"
    sql_anomalie = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_thermal_anomalies(" \
                   "f_date TIMESTAMP, " \
                   "f_zone VARCHAR(50), " \
                   "f_volcan VARCHAR(50), " \
                   "f_lat REAL, " \
                   "f_lon REAL, " \
                   "f_r39 REAL, " \
                   "f_r12 REAL, " \
                   "f_nti REAL);"
    sql_legend = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_legend_label(" \
                 "f_date TIMESTAMP, " \
                 "f_zone VARCHAR(50), " \
                 "f_minASH2 REAL, " \
                 "f_maxASH2 REAL, " \
                 "f_minASH3 REAL, " \
                 "f_maxASH3 REAL, " \
                 "f_minRAD REAL, " \
                 "f_maxRAD REAL, " \
                 "f_minCLOUD REAL, " \
                 "f_maxCLOUD REAL, " \
                 "f_coverCLOUD REAL, " \
                 "f_minSO2_LA REAL, " \
                 "f_maxSO2_LA REAL, " \
                 "f_minSO2_HA REAL, " \
                 "f_maxSO2_HA REAL);"
    sql_nti = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_nti_monitoring(" \
              "f_date TIMESTAMP, " \
              "f_zone VARCHAR(50), " \
              "f_volcan VARCHAR(50), " \
              "f_pixID INT, " \
              "f_nti REAL, " \
              "f_anomaly BOOLEAN);"
    sql_lava = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_lava_volume(" \
               "f_date TIMESTAMP, " \
               "f_zone VARCHAR(50), " \
               "f_volcan VARCHAR(50), " \
               "f_volume REAL, " \
               "f_Tamb REAL, " \
               "f_Tsurf REAL, " \
               "f_h REAL);"
    sql_quality = "DROP FUNCTION IF EXISTS UPERT_INTO_hotvolc_quality_flag(" \
                  "f_date TIMESTAMP, " \
                  "f_zone VARCHAR(50), " \
                  "f_volcan VARCHAR(50), " \
                  "f_Tb108_mean REAL, " \
                  "f_Cloud_Cover REAL, " \
                  "f_Quality_Flag INT);"

    con = hotvolc_database.DB_connection()

    try:
        with con:
            with con.cursor() as cur:
                cur.execute(sql_atmos)
                cur.execute(sql_thermal)
                cur.execute(sql_anomalie)
                cur.execute(sql_legend)
                cur.execute(sql_nti)
                cur.execute(sql_lava)
                cur.execute(sql_quality)

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        hotvolc_database.DB_close(con)


def hotvolc_create_function():
    """Creation des fonctions d'insertion de donnees dans la BDD"""

    sql_atmos = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_atmospheric_monitoring(" \
                "f_date TIMESTAMP, " \
                "f_zone VARCHAR(50), " \
                "f_volcan VARCHAR(50), " \
                "f_nb_ASH2 INT, " \
                "f_minTb108_ASH2 REAL, " \
                "f_minBTD_ASH2 REAL, " \
                "f_nb_ASH3 INT, " \
                "f_minTb108_ASH3 REAL, " \
                "f_minBTD_ASH3 REAL, " \
                "f_nbSO2_LA INT, " \
                "f_maxSRD_SO2_LA REAL, " \
                "f_nbSO2_HA INT, " \
                "f_maxSRD_SO2_HA REAL)\n" \
                "RETURNS VOID AS $$\n" \
                "BEGIN\n" \
                "LOOP\n" \
                "UPDATE hotvolc_atmospheric_monitoring SET " \
                "nb_ASH2 = f_nb_ASH2, " \
                "minTb108_ASH2 = f_minTb108_ASH2, " \
                "minBTD_ASH2 = f_minBTD_ASH2, " \
                "nb_ASH3 = f_nb_ASH3, " \
                "minTb108_ASH3 = f_minTb108_ASH3, " \
                "minBTD_ASH3 = f_minBTD_ASH3, " \
                "nbSO2_LA = f_nbSO2_LA, " \
                "maxSRD_SO2_LA = f_maxSRD_SO2_LA, " \
                "nbSO2_HA = f_nbSO2_HA, " \
                "maxSRD_SO2_HA = f_maxSRD_SO2_HA " \
                "WHERE " \
                "date = f_date AND volcan = f_volcan;\n" \
                "IF found THEN\n" \
                "RETURN;\n" \
                "END IF;\n" \
                "BEGIN\n" \
                "INSERT INTO hotvolc_atmospheric_monitoring(" \
                "date, " \
                "zone, " \
                "volcan, " \
                "nb_ASH2, " \
                "minTb108_ASH2, " \
                "minBTD_ASH2, " \
                "nb_ASH3, " \
                "minTb108_ASH3, " \
                "minBTD_ASH3, " \
                "nbSO2_LA, " \
                "maxSRD_SO2_LA, " \
                "nbSO2_HA, " \
                "maxSRD_SO2_HA)" \
                "VALUES (" \
                "f_date, " \
                "f_zone, " \
                "f_volcan, " \
                "f_nb_ASH2, " \
                "f_minTb108_ASH2, " \
                "f_minBTD_ASH2, " \
                "f_nb_ASH3, " \
                "f_minTb108_ASH3, " \
                "f_minBTD_ASH3, " \
                "f_nbSO2_LA, " \
                "f_maxSRD_SO2_LA, " \
                "f_nbSO2_HA, " \
                "f_maxSRD_SO2_HA);\n" \
                "RETURN;\n" \
                "EXCEPTION WHEN unique_violation THEN\n" \
                "END;\n" \
                "END LOOP;\n" \
                "END;\n" \
                "$$\n" \
                "LANGUAGE plpgsql;"

    sql_thermal = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_thermal_monitoring(" \
                  "f_date TIMESTAMP, " \
                  "f_zone VARCHAR(50), " \
                  "f_volcan VARCHAR(50), " \
                  "f_sizeROI INT, " \
                  "f_nbanomalie INT, " \
                  "f_TSR REAL, " \
                  "f_r39mean REAL, " \
                  "f_r39max REAL, " \
                  "f_r39min REAL, " \
                  "f_r12mean REAL, " \
                  "f_r12max REAL, " \
                  "f_r12min REAL)\n" \
                  "RETURNS VOID AS $$\n" \
                  "BEGIN\n" \
                  "LOOP\n" \
                  "UPDATE hotvolc_thermal_monitoring SET " \
                  "sizeROI = f_sizeROI, " \
                  "nbanomalie = f_nbanomalie, " \
                  "TSR = f_TSR, " \
                  "r39mean = f_r39mean, " \
                  "r39max = f_r39max, " \
                  "r39min = f_r39min, " \
                  "r12mean = f_r12mean, " \
                  "r12max = f_r12max, " \
                  "r12min = f_r12min " \
                  "WHERE " \
                  "date = f_date AND volcan = f_volcan;\n" \
                  "IF found THEN\n" \
                  "RETURN;\n" \
                  "END IF;\n" \
                  "BEGIN\n" \
                  "INSERT INTO hotvolc_thermal_monitoring(" \
                  "date, " \
                  "zone, " \
                  "volcan, " \
                  "sizeROI, " \
                  "nbanomalie, " \
                  "TSR, " \
                  "r39mean, " \
                  "r39max, " \
                  "r39min, " \
                  "r12mean, " \
                  "r12max, " \
                  "r12min) " \
                  "VALUES (" \
                  "f_date, " \
                  "f_zone, " \
                  "f_volcan, " \
                  "f_sizeROI, " \
                  "f_nbanomalie, " \
                  "f_TSR, " \
                  "f_r39mean, " \
                  "f_r39max, " \
                  "f_r39min, " \
                  "f_r12mean, " \
                  "f_r12max, " \
                  "f_r12min);\n" \
                  "RETURN;\n" \
                  "EXCEPTION WHEN unique_violation THEN\n" \
                  "END;\n" \
                  "END LOOP;\n" \
                  "END;\n" \
                  "$$\n" \
                  "LANGUAGE plpgsql;"

    sql_anomalie = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_thermal_anomalies(" \
                   "f_date TIMESTAMP, " \
                   "f_zone VARCHAR(50), " \
                   "f_volcan VARCHAR(50), " \
                   "f_lat REAL, " \
                   "f_lon REAL, " \
                   "f_r39 REAL, " \
                   "f_r12 REAL, " \
                   "f_nti REAL)\n" \
                   "RETURNS VOID AS $$\n" \
                   "BEGIN\n" \
                   "LOOP\n" \
                   "UPDATE hotvolc_thermal_anomalies SET " \
                   "r39 = f_r39, " \
                   "r12 = f_r12, " \
                   "nti = f_nti " \
                   "WHERE " \
                   "date = f_date AND lat = f_lat AND lon = f_lon;\n" \
                   "IF found THEN\n" \
                   "RETURN;\n" \
                   "END IF;\n" \
                   "BEGIN\n" \
                   "INSERT INTO hotvolc_thermal_anomalies(" \
                   "date, " \
                   "zone, " \
                   "volcan, " \
                   "lat, " \
                   "lon, " \
                   "r39, " \
                   "r12, " \
                   "nti) " \
                   "VALUES (" \
                   "f_date, " \
                   "f_zone, " \
                   "f_volcan, " \
                   "f_lat, " \
                   "f_lon, " \
                   "f_r39, " \
                   "f_r12, " \
                   "f_nti);\n" \
                   "RETURN;\n" \
                   "EXCEPTION WHEN unique_violation THEN\n" \
                   "END;\n" \
                   "END LOOP;\n" \
                   "END;\n" \
                   "$$\n" \
                   "LANGUAGE plpgsql;"

    sql_legend = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_legend_label(" \
                 "f_date TIMESTAMP, " \
                 "f_zone VARCHAR(50), " \
                 "f_minASH2 REAL, " \
                 "f_maxASH2 REAL, " \
                 "f_minASH3 REAL, " \
                 "f_maxASH3 REAL, " \
                 "f_minRAD REAL, " \
                 "f_maxRAD REAL, " \
                 "f_minCLOUD REAL, " \
                 "f_maxCLOUD REAL, " \
                 "f_coverCLOUD REAL, " \
                 "f_minSO2_LA REAL, " \
                 "f_maxSO2_LA REAL, " \
                 "f_minSO2_HA REAL, " \
                 "f_maxSO2_HA REAL)\n" \
                 "RETURNS VOID AS $$\n" \
                 "BEGIN\n" \
                 "LOOP\n" \
                 "UPDATE hotvolc_legend_label SET " \
                 "minASH2 = f_minASH2, " \
                 "maxASH2 = f_maxASH2, " \
                 "minASH3 = f_minASH3, " \
                 "maxASH3 = f_maxASH3, " \
                 "minRAD = f_minRAD, " \
                 "maxRAD =  f_maxRAD, " \
                 "minCLOUD = f_minCLOUD, " \
                 "maxCLOUD = f_maxCLOUD, " \
                 "coverCLOUD = f_coverCLOUD, " \
                 "minSO2_LA = f_minSO2_LA, " \
                 "maxSO2_LA = f_maxSO2_LA, " \
                 "minSO2_HA = f_minSO2_HA, " \
                 "maxSO2_HA = f_maxSO2_HA " \
                 "WHERE " \
                 "date = f_date AND zone = f_zone;\n" \
                 "IF found THEN\n" \
                 "RETURN;\n" \
                 "END IF;\n" \
                 "BEGIN\n" \
                 "INSERT INTO hotvolc_legend_label(" \
                 "date, " \
                 "zone, " \
                 "minASH2, " \
                 "maxASH2, " \
                 "minASH3, " \
                 "maxASH3, " \
                 "minRAD, " \
                 "maxRAD, " \
                 "minCLOUD, " \
                 "maxCLOUD, " \
                 "coverCLOUD, " \
                 "minSO2_LA, " \
                 "maxSO2_LA, " \
                 "minSO2_HA, " \
                 "maxSO2_HA) " \
                 "VALUES (" \
                 "f_date, " \
                 "f_zone, " \
                 "f_minASH2, " \
                 "f_maxASH2, " \
                 "f_minASH3, " \
                 "f_maxASH3, " \
                 "f_minRAD, " \
                 "f_maxRAD, " \
                 "f_minCLOUD, " \
                 "f_maxCLOUD, " \
                 "f_coverCLOUD, " \
                 "f_minSO2_LA, " \
                 "f_maxSO2_LA, " \
                 "f_minSO2_HA, " \
                 "f_maxSO2_HA);\n" \
                 "RETURN;\n" \
                 "EXCEPTION WHEN unique_violation THEN\n" \
                 "END;\n" \
                 "END LOOP;\n" \
                 "END;\n" \
                 "$$\n" \
                 "LANGUAGE plpgsql;"

    sql_nti = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_nti_monitoring(" \
              "f_date TIMESTAMP, " \
              "f_zone VARCHAR(50), " \
              "f_volcan VARCHAR(50), " \
              "f_pixID INT, " \
              "f_nti REAL, " \
              "f_anomaly BOOLEAN)\n" \
              "RETURNS VOID AS $$\n" \
              "BEGIN\n" \
              "LOOP\n" \
              "UPDATE hotvolc_nti_monitoring SET " \
              "nti = f_nti, " \
              "anomaly = f_anomaly " \
              "WHERE " \
              "date = f_date AND volcan = f_volcan AND pixID = f_pixID;\n" \
              "IF found THEN\n" \
              "RETURN;\n" \
              "END IF;\n" \
              "BEGIN\n" \
              "INSERT INTO hotvolc_nti_monitoring(" \
              "date, " \
              "zone, " \
              "volcan, " \
              "pixID, " \
              "nti, " \
              "anomaly) " \
              "VALUES(" \
              "f_date, " \
              "f_zone, " \
              "f_volcan, " \
              "f_pixID, " \
              "f_nti, " \
              "f_anomaly);\n" \
              "RETURN;\n" \
              "EXCEPTION WHEN unique_violation THEN\n" \
              "END;\n" \
              "END LOOP;\n" \
              "END;\n" \
              "$$\n" \
              "LANGUAGE plpgsql;"

    sql_lava = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_lava_volume(" \
               "f_date TIMESTAMP, " \
               "f_zone VARCHAR(50), " \
               "f_volcan VARCHAR(50), " \
               "f_volume REAL, " \
               "f_Tamb REAL, " \
               "f_Tsurf REAL, " \
               "f_h REAL)\n" \
               "RETURNS VOID AS $$\n" \
               "BEGIN\n" \
               "LOOP\n" \
               "UPDATE hotvolc_lava_volume SET " \
               "volume = f_volume, " \
               "Tamb = f_Tamb, " \
               "Tsurf = f_Tsurf, " \
               "h = f_h " \
               "WHERE " \
               "date = f_date AND volcan = f_volcan;\n" \
               "IF found THEN\n" \
               "RETURN;\n" \
               "END IF;\n" \
               "BEGIN\n" \
               "INSERT INTO hotvolc_lava_volume(" \
               "date, " \
               "zone, " \
               "volcan, " \
               "volume, " \
               "Tamb, " \
               "Tsurf, " \
               "h) " \
               "VALUES (" \
               "f_date, " \
               "f_zone, " \
               "f_volcan, " \
               "f_volume, " \
               "f_Tamb, " \
               "f_Tsurf, " \
               "f_h);\n" \
               "RETURN;\n" \
               "EXCEPTION WHEN unique_violation THEN\n" \
               "END;\n" \
               "END LOOP;\n" \
               "END;\n" \
               "$$\n" \
               "LANGUAGE plpgsql;"

    sql_quality = "CREATE OR REPLACE FUNCTION UPERT_INTO_hotvolc_quality_flag(" \
                  "f_date TIMESTAMP, " \
                  "f_zone VARCHAR(50), " \
                  "f_volcan VARCHAR(50), " \
                  "f_Tb108_mean REAL, " \
                  "f_Cloud_Cover REAL, " \
                  "f_Quality_Flag INT)\n" \
                  "RETURNS VOID AS $$\n" \
                  "BEGIN\n" \
                  "LOOP\n" \
                  "UPDATE hotvolc_quality_flag SET " \
                  "Tb108_mean = f_Tb108_mean, " \
                  "Cloud_Cover = f_Cloud_Cover, " \
                  "Quality_Flag = f_Quality_Flag " \
                  "WHERE " \
                  "date = f_date AND volcan = f_volcan;\n" \
                  "IF found THEN\n" \
                  "RETURN;\n" \
                  "END IF;\n" \
                  "BEGIN\n" \
                  "INSERT INTO hotvolc_quality_flag(" \
                  "date, " \
                  "zone, " \
                  "volcan, " \
                  "Tb108_mean, " \
                  "Cloud_Cover, " \
                  "Quality_Flag) " \
                  "VALUES (" \
                  "f_date, " \
                  "f_zone, " \
                  "f_volcan, " \
                  "f_Tb108_mean, " \
                  "f_Cloud_Cover, " \
                  "f_Quality_Flag);\n" \
                  "RETURN;\n" \
                  "EXCEPTION WHEN unique_violation THEN\n" \
                  "END;\n" \
                  "END LOOP;\n" \
                  "END;\n" \
                  "$$\n" \
                  "LANGUAGE plpgsql;"

    con = hotvolc_database.DB_connection()

    try:
        with con:
            with con.cursor() as cur:
                cur.execute(sql_atmos)
                cur.execute(sql_thermal)
                cur.execute(sql_anomalie)
                cur.execute(sql_legend)
                cur.execute(sql_nti)
                cur.execute(sql_lava)
                cur.execute(sql_quality)

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        hotvolc_database.DB_close(con)
