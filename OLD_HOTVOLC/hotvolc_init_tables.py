#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import os
import re

import hotvolc_database


def init_table_path():
    """Intialisation de la table path"""

    con = hotvolc_database.DB_connection()
    cur = con.cursor()

    SQL_path = "INSERT INTO hotvolc_path(" \
               "path_xrit, " \
               "path_raw, " \
               "path_products, " \
               "path_colorbars) " \
               "VALUES (" \
               "'/mnt/recv-class/', " \
               "'/mnt/recv-class/', " \
               "'/VD_data/PRODUITS/', " \
               "'/home/hotvolc/colorbars/');"
    cur.execute(SQL_path)
    con.commit()
    hotvolc_database.DB_close(con)


def init_table_zones():
    """ """

    con = hotvolc_database.DB_connection()
    cur = con.cursor()

    SQL_zone_Islande = "INSERT INTO hotvolc_zones( " \
                       "zone, " \
                       "latmin_sat, " \
                       "latmax_sat, " \
                       "lonmin_sat, " \
                       "lonmax_sat, " \
                       "latmin_merc, " \
                       "latmax_merc, " \
                       "lonmin_merc, " \
                       "lonmax_merc, " \
                       "x_min, " \
                       "x_max, " \
                       "y_min, " \
                       "y_max, " \
                       "ash_cutoff1, " \
                       "ash_cutoff2, " \
                       "nti_cutoff_day, " \
                       "nti_cutoff_night, " \
                       "so2_la_cutoff, " \
                       "so2_ha_cutoff, " \
                       "pcs_def_sat, " \
                       "xsize_sat, " \
                       "ysize_sat, " \
                       "area_extent_sat, " \
                       "pcs_def_merc, " \
                       "xsize_merc, " \
                       "ysize_merc, " \
                       "area_extent_merc) " \
                       "VALUES (" \
                       "'Islande', " \
                       "57.0, " \
                       "68.5, " \
                       "-28.0, " \
                       "-1.5, " \
                       "57.0, " \
                       "68.5, " \
                       "-28.0, " \
                       "-1.5, " \
                       "98, " \
                       "242, " \
                       "1357, " \
                       "1837, " \
                       "-0.25, " \
                       "-1.0, " \
                       "1.20, " \
                       "1.20, " \
                       "1.3, " \
                       "-999.0, " \
                       "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0', " \
                       "480, " \
                       "144, " \
                       "'(-1495664.5244834027, 4841752.7477971828, -55151.3048536735, 5274257.6838507326)'," \
                       "'proj=merc, ellps=WGS84, lon_0=0'," \
                       "720, " \
                       "692, " \
                       "'(-3116945.7422116599, 7760118.6729024565, -166979.2361899104, 10597209.2273383997)');"

    SQL_zone_Acores = "INSERT INTO hotvolc_zones(" \
                      "zone, " \
                      "latmin_sat, " \
                      "latmax_sat, " \
                      "lonmin_sat, " \
                      "lonmax_sat, " \
                      "latmin_merc, " \
                      "latmax_merc, " \
                      "lonmin_merc, " \
                      "lonmax_merc, " \
                      "x_min, " \
                      "x_max, " \
                      "y_min, " \
                      "y_max, " \
                      "ash_cutoff1, " \
                      "ash_cutoff2, " \
                      "nti_cutoff_day, " \
                      "nti_cutoff_night, " \
                      "so2_la_cutoff, " \
                      "so2_ha_cutoff, " \
                      "pcs_def_sat, " \
                      "xsize_sat, " \
                      "ysize_sat, " \
                      "area_extent_sat, " \
                      "pcs_def_merc, " \
                      "xsize_merc, " \
                      "ysize_merc, " \
                      "area_extent_merc) " \
                      "VALUES (" \
                      "'Acores', " \
                      "34.0, " \
                      "43.5, " \
                      "-34.5, " \
                      "-23.5, " \
                      "34.0, " \
                      "43.0, " \
                      "-34.5, " \
                      "-23.5, " \
                      "489, " \
                      "743, " \
                      "912, " \
                      "1275, " \
                      "-0.25, " \
                      "-1.0, " \
                      "1.20, " \
                      "1.20, " \
                      "1.3, " \
                      "-999.0," \
                      "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                      "363, " \
                      "254, " \
                      "'(-2832326.0481607458, 3337110.6330738319, -1742645.0742343799, 4099818.0237400075)', " \
                      "'proj=merc, ellps=WGS84, lon_0=0', " \
                      "536, " \
                      "562, " \
                      "'(-3840522.4323679376, 4028802.0261344067, -2616008.0336419288, 5311971.8469454711)');"

    SQL_zone_Italie = "INSERT INTO hotvolc_zones(" \
                      "zone," \
                      "latmin_sat," \
                      "latmax_sat," \
                      "lonmin_sat," \
                      "lonmax_sat," \
                      "latmin_merc," \
                      "latmax_merc," \
                      "lonmin_merc," \
                      "lonmax_merc," \
                      "x_min," \
                      "x_max," \
                      "y_min," \
                      "y_max," \
                      "ash_cutoff1," \
                      "ash_cutoff2," \
                      "nti_cutoff_day," \
                      "nti_cutoff_night," \
                      "so2_la_cutoff," \
                      "so2_ha_cutoff," \
                      "pcs_def_sat," \
                      "xsize_sat," \
                      "ysize_sat," \
                      "area_extent_sat," \
                      "pcs_def_merc," \
                      "xsize_merc," \
                      "ysize_merc," \
                      "area_extent_merc)" \
                      "VALUES (" \
                      "'Italie'," \
                      "33.0," \
                      "42.5," \
                      "9.0," \
                      "32.5," \
                      "33.15," \
                      "41.2," \
                      "10.5," \
                      "27.0," \
                      "525," \
                      "756," \
                      "2128," \
                      "2645," \
                      "-0.25, " \
                      "-1.0," \
                      "1.20," \
                      "1.20," \
                      "1.3," \
                      "-999.0," \
                      "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                      "517," \
                      "231," \
                      "'(817943.2295825722, 3299061.0692231092, 2367617.0438768314, 3991235.9445067868)'," \
                      "'proj=merc, ellps=WGS84, lon_0=0'," \
                      "557," \
                      "348," \
                      "'(1168854.6533293726, 3895303.9633938945, 3005626.2514183861, 5041886.5261552576)');"

    SQL_zone_Canaries = "INSERT INTO hotvolc_zones(" \
                        "zone," \
                        "latmin_sat," \
                        "latmax_sat," \
                        "lonmin_sat," \
                        "lonmax_sat," \
                        "latmin_merc," \
                        "latmax_merc," \
                        "lonmin_merc," \
                        "lonmax_merc," \
                        "x_min," \
                        "x_max," \
                        "y_min," \
                        "y_max," \
                        "ash_cutoff1," \
                        "ash_cutoff2," \
                        "nti_cutoff_day," \
                        "nti_cutoff_night," \
                        "so2_la_cutoff," \
                        "so2_ha_cutoff," \
                        "pcs_def_sat," \
                        "xsize_sat," \
                        "ysize_sat," \
                        "area_extent_sat," \
                        "pcs_def_merc," \
                        "xsize_merc," \
                        "ysize_merc," \
                        "area_extent_merc)" \
                        "VALUES (" \
                        "'Canaries'," \
                        "23.0," \
                        "33.5," \
                        "-22.45," \
                        "-11," \
                        "23.0," \
                        "33.0," \
                        "-22.48," \
                        "-11.0," \
                        "729," \
                        "1054," \
                        "1127," \
                        "1528," \
                        "-0.25, " \
                        "-1.0," \
                        "1.20," \
                        "1.20," \
                        "1.3," \
                        "-999.0," \
                        "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                        "401," \
                        "325," \
                        "'(-2186995.6153641711, 2404465.0248058694, -983952.5751392361, 3379810.9498228892)'," \
                        "'proj=merc, ellps=WGS84, lon_0=0'," \
                        "597," \
                        "589," \
                        "'(-2504688.5428486555, 2632018.6375864232, -1224514.3987260093, 3895303.9633938945)');"

    SQL_zone_Antilles = "INSERT INTO hotvolc_zones(" \
                        "zone," \
                        "latmin_sat," \
                        "latmax_sat," \
                        "lonmin_sat," \
                        "lonmax_sat," \
                        "latmin_merc," \
                        "latmax_merc," \
                        "lonmin_merc," \
                        "lonmax_merc," \
                        "x_min," \
                        "x_max," \
                        "y_min," \
                        "y_max," \
                        "ash_cutoff1," \
                        "ash_cutoff2," \
                        "nti_cutoff_day," \
                        "nti_cutoff_night," \
                        "so2_la_cutoff," \
                        "so2_ha_cutoff," \
                        "pcs_def_sat," \
                        "xsize_sat," \
                        "ysize_sat," \
                        "area_extent_sat," \
                        "pcs_def_merc," \
                        "xsize_merc," \
                        "ysize_merc," \
                        "area_extent_merc)" \
                        "VALUES (" \
                        "'Antilles'," \
                        "7.2," \
                        "21.8," \
                        "-67.2," \
                        "-56.0," \
                        "7.2," \
                        "21.5," \
                        "-67.2," \
                        "-56," \
                        "1139," \
                        "1620," \
                        "116," \
                        "356," \
                        "-0.25, " \
                        "-1.0," \
                        "1.20," \
                        "1.20," \
                        "1.3," \
                        "-999.0," \
                        "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                        "240," \
                        "481," \
                        "'(-5219662.6246563941, 707889.6668080697, -4500450.2602738813, 2148453.9085861910)'," \
                        "'proj=merc, ellps=WGS84, lon_0=0'," \
                        "354," \
                        "468," \
                        "'(-7480669.7813079841, 803618.1642596693, -6233891.4844233198, 2451599.0873778537)');"

    SQL_zone_CapVert = "INSERT INTO hotvolc_zones(" \
                       "zone," \
                       "latmin_sat," \
                       "latmax_sat," \
                       "lonmin_sat," \
                       "lonmax_sat," \
                       "latmin_merc," \
                       "latmax_merc," \
                       "lonmin_merc," \
                       "lonmax_merc," \
                       "x_min," \
                       "x_max," \
                       "y_min," \
                       "y_max," \
                       "ash_cutoff1," \
                       "ash_cutoff2," \
                       "nti_cutoff_day," \
                       "nti_cutoff_night," \
                       "so2_la_cutoff," \
                       "so2_ha_cutoff," \
                       "pcs_def_sat," \
                       "xsize_sat," \
                       "ysize_sat," \
                       "area_extent_sat," \
                       "pcs_def_merc," \
                       "xsize_merc," \
                       "ysize_merc," \
                       "area_extent_merc)" \
                       "VALUES (" \
                       "'CapVert'," \
                       "9.85," \
                       "20.0," \
                       "-29.34," \
                       "-19.34," \
                       "9.85," \
                       "19.85," \
                       "-29.34," \
                       "-19.34," \
                       "1149," \
                       "1504," \
                       "857," \
                       "1207," \
                       "-0.25, " \
                       "-1.0," \
                       "1.20," \
                       "1.20," \
                       "1.3," \
                       "-999.0," \
                       "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                       "350," \
                       "355," \
                       "'(-2996925.7523366422, 1053317.4193302148, -1944546.8809189394, 2119288.6268567517)'," \
                       "'proj=merc, ellps=WGS84, lon_0=0'," \
                       "525," \
                       "544," \
                       "'(-3266113.8598746466, 1101938.3518183348, -2152918.9519419107, 2255269.8082336430)');"

    SQL_zone_Rift = "INSERT INTO hotvolc_zones(" \
                    "zone," \
                    "latmin_sat," \
                    "latmax_sat," \
                    "lonmin_sat," \
                    "lonmax_sat," \
                    "latmin_merc," \
                    "latmax_merc," \
                    "lonmin_merc," \
                    "lonmax_merc," \
                    "x_min," \
                    "x_max," \
                    "y_min," \
                    "y_max," \
                    "ash_cutoff1," \
                    "ash_cutoff2," \
                    "nti_cutoff_day," \
                    "nti_cutoff_night," \
                    "so2_la_cutoff," \
                    "so2_ha_cutoff," \
                    "pcs_def_sat," \
                    "xsize_sat," \
                    "ysize_sat," \
                    "area_extent_sat," \
                    "pcs_def_merc," \
                    "xsize_merc," \
                    "ysize_merc," \
                    "area_extent_merc)" \
                    "VALUES (" \
                    "'Rift'," \
                    "10.0," \
                    "19.0," \
                    "36.0," \
                    "49.5," \
                    "10.5," \
                    "17.5," \
                    "38.0," \
                    "45.5," \
                    "1217," \
                    "1503," \
                    "3039," \
                    "3279," \
                    "-0.25, " \
                    "-1.0," \
                    "1.20," \
                    "1.20," \
                    "1.3," \
                    "-999.0," \
                    "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                    "240," \
                    "286," \
                    "'(3551637.2574445615, 1056263.4122301580, 4271879.7287397943, 1915055.8832827925)'," \
                    "'proj=merc, ellps=WGS84, lon_0=0'," \
                    "191," \
                    "184," \
                    "'(4230140.6501443963, 1175452.6085934294, 5065036.8310939474, 1979106.4997243879)');"

    SQL_zone_Cameroon = "INSERT INTO hotvolc_zones(" \
                        "zone," \
                        "latmin_sat," \
                        "latmax_sat," \
                        "lonmin_sat," \
                        "lonmax_sat," \
                        "latmin_merc," \
                        "latmax_merc," \
                        "lonmin_merc," \
                        "lonmax_merc," \
                        "x_min," \
                        "x_max," \
                        "y_min," \
                        "y_max," \
                        "ash_cutoff1," \
                        "ash_cutoff2," \
                        "nti_cutoff_day," \
                        "nti_cutoff_night," \
                        "so2_la_cutoff," \
                        "so2_ha_cutoff," \
                        "pcs_def_sat," \
                        "xsize_sat," \
                        "ysize_sat," \
                        "area_extent_sat," \
                        "pcs_def_merc," \
                        "xsize_merc," \
                        "ysize_merc," \
                        "area_extent_merc)" \
                        "VALUES (" \
                        "'Cameroon'," \
                        "-3.0," \
                        "10.0," \
                        "3.0," \
                        "15.0," \
                        "-2.51," \
                        "9.14," \
                        "3.76," \
                        "14.16," \
                        "1492," \
                        "1966," \
                        "1967," \
                        "2392," \
                        "-0.25, " \
                        "-1.0," \
                        "1.20," \
                        "1.20," \
                        "1.3," \
                        "-999.0," \
                        "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                        "425," \
                        "474," \
                        "'(333179.7438409246, -331389.2281830559, 1610802.1838009807, 1089348.4116132094)'," \
                        "'proj=merc, ellps=WGS84, lon_0=0'," \
                        "555," \
                        "624," \
                        "'(418561.2853827086, -279501.3356375839, 1576283.9896327537, 1021803.1268512544)');"

    SQL_zone_East_Africa = "INSERT INTO hotvolc_zones(" \
                           "zone," \
                           "latmin_sat," \
                           "latmax_sat," \
                           "lonmin_sat," \
                           "lonmax_sat," \
                           "latmin_merc," \
                           "latmax_merc," \
                           "lonmin_merc," \
                           "lonmax_merc," \
                           "x_min," \
                           "x_max," \
                           "y_min," \
                           "y_max," \
                           "ash_cutoff1," \
                           "ash_cutoff2," \
                           "nti_cutoff_day," \
                           "nti_cutoff_night," \
                           "so2_la_cutoff," \
                           "so2_ha_cutoff," \
                           "pcs_def_sat," \
                           "xsize_sat," \
                           "ysize_sat," \
                           "area_extent_sat," \
                           "pcs_def_merc," \
                           "xsize_merc," \
                           "ysize_merc," \
                           "area_extent_merc)" \
                           "VALUES (" \
                           "'East_Africa'," \
                           "-8.5," \
                           "8.0," \
                           "24.19," \
                           "42.5," \
                           "-8.31," \
                           "7.53," \
                           "24.19," \
                           "41.75," \
                           "1577," \
                           "2161," \
                           "2701," \
                           "3207," \
                           "-0.25, " \
                           "-1.0," \
                           "1.20," \
                           "1.20," \
                           "1.3," \
                           "-999.0," \
                           "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                           "506," \
                           "584," \
                           "'(2536484.9052373152, -917955.0345382314, 4054092.8499170886, 835771.2628262660)'," \
                           "'proj=merc, ellps=WGS84, lon_0=0'," \
                           "735," \
                           "665," \
                           "'(2692818.4822892877, -928325.3594419303, 4647588.7406191714, 840659.2504674494)');"

    SQL_zone_Karthala = "INSERT INTO hotvolc_zones(" \
                        "zone," \
                        "latmin_sat," \
                        "latmax_sat," \
                        "lonmin_sat," \
                        "lonmax_sat," \
                        "latmin_merc," \
                        "latmax_merc," \
                        "lonmin_merc," \
                        "lonmax_merc," \
                        "x_min," \
                        "x_max," \
                        "y_min," \
                        "y_max," \
                        "ash_cutoff1," \
                        "ash_cutoff2," \
                        "nti_cutoff_day," \
                        "nti_cutoff_night," \
                        "so2_la_cutoff," \
                        "so2_ha_cutoff," \
                        "pcs_def_sat," \
                        "xsize_sat," \
                        "ysize_sat," \
                        "area_extent_sat," \
                        "pcs_def_merc," \
                        "xsize_merc," \
                        "ysize_merc," \
                        "area_extent_merc)" \
                        "VALUES (" \
                        "'Karthala'," \
                        "-17.2," \
                        "-6.91," \
                        "38.37," \
                        "48.37," \
                        "-16.91," \
                        "-6.91," \
                        "38.37," \
                        "48.37," \
                        "2093," \
                        "2450," \
                        "3058," \
                        "3335," \
                        "-0.25, " \
                        "-1.0," \
                        "1.20," \
                        "1.20," \
                        "1.3," \
                        "-999.0," \
                        "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                        "277," \
                        "357," \
                        "'(3609020.8371763113, -1783205.9560315453, 4439250.9910881668, -713027.8808371510)'," \
                        "'proj=merc, ellps=WGS84, lon_0=0'," \
                        "413," \
                        "423," \
                        "'(4271328.8617379069, -1910351.0222467028, 5384523.7696706429, -771089.1900584853)');"

    SQL_zone_Reunion = "INSERT INTO hotvolc_zones(" \
                       "zone," \
                       "latmin_sat," \
                       "latmax_sat," \
                       "lonmin_sat," \
                       "lonmax_sat," \
                       "latmin_merc," \
                       "latmax_merc," \
                       "lonmin_merc," \
                       "lonmax_merc," \
                       "x_min," \
                       "x_max," \
                       "y_min," \
                       "y_max," \
                       "ash_cutoff1," \
                       "ash_cutoff2," \
                       "nti_cutoff_day," \
                       "nti_cutoff_night," \
                       "so2_la_cutoff," \
                       "so2_ha_cutoff," \
                       "pcs_def_sat," \
                       "xsize_sat," \
                       "ysize_sat," \
                       "area_extent_sat," \
                       "pcs_def_merc," \
                       "xsize_merc," \
                       "ysize_merc," \
                       "area_extent_merc)" \
                       "VALUES (" \
                       "'Reunion'," \
                       "-26.7," \
                       "-16.31," \
                       "50.71," \
                       "60.71," \
                       "-26.31," \
                       "-16.31," \
                       "50.71," \
                       "60.71," \
                       "2392," \
                       "2729," \
                       "3215," \
                       "3472," \
                       "-0.25, " \
                       "-1.0," \
                       "1.20," \
                       "1.20," \
                       "1.3," \
                       "-999.0," \
                       "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                       "257," \
                       "337," \
                       "'(4078465.4051886955, -2622226.8572377432, 4849187.2913070256, -1609994.3810110700)'," \
                       "'proj=merc, ellps=WGS84, lon_0=0'," \
                       "378," \
                       "406," \
                       "'(5645011.3781269025, -3037526.7077845572, 6758206.2860596385, -1840650.5558424362)');"

    SQL_zone_France = "INSERT INTO hotvolc_zones(" \
                      "zone," \
                      "latmin_sat," \
                      "latmax_sat," \
                      "lonmin_sat," \
                      "lonmax_sat," \
                      "latmin_merc," \
                      "latmax_merc," \
                      "lonmin_merc," \
                      "lonmax_merc," \
                      "x_min," \
                      "x_max," \
                      "y_min," \
                      "y_max," \
                      "ash_cutoff1," \
                      "ash_cutoff2," \
                      "nti_cutoff_day," \
                      "nti_cutoff_night," \
                      "so2_la_cutoff," \
                      "so2_ha_cutoff," \
                      "pcs_def_sat," \
                      "xsize_sat," \
                      "ysize_sat," \
                      "area_extent_sat," \
                      "pcs_def_merc," \
                      "xsize_merc," \
                      "ysize_merc," \
                      "area_extent_merc)" \
                      "VALUES (" \
                      "'France'," \
                      "41.5," \
                      "52.0," \
                      "-5.5," \
                      "10.0," \
                      "42.0," \
                      "51.5," \
                      "-5.0," \
                      "8.25," \
                      "306," \
                      "521," \
                      "1709," \
                      "2073," \
                      "-0.25, " \
                      "-1.0," \
                      "1.20," \
                      "1.20," \
                      "1.3," \
                      "-999.0," \
                      "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                      "333," \
                      "215," \
                      "'(-438688.6614315703, 4005392.7397313472, 559627.5390319135, 4650545.9505798584)'," \
                      "'proj=merc, ellps=WGS84, lon_0=0'," \
                      "465," \
                      "488," \
                      "'(-556597.4539663679, 5160979.4440497831, 918385.7990445070, 6710219.0832207408)');"

    SQL_zone_globe = "INSERT INTO hotvolc_zones(" \
                     "zone," \
                     "latmin_sat," \
                     "latmax_sat," \
                     "lonmin_sat," \
                     "lonmax_sat," \
                     "latmin_merc," \
                     "latmax_merc," \
                     "lonmin_merc," \
                     "lonmax_merc," \
                     "x_min," \
                     "x_max," \
                     "y_min," \
                     "y_max," \
                     "ash_cutoff1," \
                     "ash_cutoff2," \
                     "nti_cutoff_day," \
                     "nti_cutoff_night," \
                     "so2_la_cutoff," \
                     "so2_ha_cutoff," \
                     "pcs_def_sat," \
                     "xsize_sat," \
                     "ysize_sat," \
                     "area_extent_sat," \
                     "pcs_def_merc," \
                     "xsize_merc," \
                     "ysize_merc," \
                     "area_extent_merc)" \
                     "VALUES (" \
                     "'met09globeFull'," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0," \
                     "3712," \
                     "0," \
                     "3712," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "0.0," \
                     "'proj=geos, lon_0=0.0, a=6378169.00, b=6356583.80, h=35785831.0'," \
                     "3712," \
                     "3712," \
                     "'(-5570248.4773392612, -5567248.074173444, 5567248.074173444, 5570248.4773392612)'," \
                     "''," \
                     "0," \
                     "0," \
                     "'');"

    cur.execute(SQL_zone_Islande)
    cur.execute(SQL_zone_Acores)
    cur.execute(SQL_zone_Italie)
    cur.execute(SQL_zone_Canaries)
    cur.execute(SQL_zone_Antilles)
    cur.execute(SQL_zone_CapVert)
    cur.execute(SQL_zone_Rift)
    cur.execute(SQL_zone_Cameroon)
    cur.execute(SQL_zone_East_Africa)
    cur.execute(SQL_zone_Karthala)
    cur.execute(SQL_zone_Reunion)
    cur.execute(SQL_zone_France)
    cur.execute(SQL_zone_globe)

    con.commit()

    hotvolc_database.DB_close(con)


def init_table_volcans_monitoring():
    """ """

    con = hotvolc_database.DB_connection()
    cur = con.cursor()

    SQL_volcan_hekla = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Hekla',\
                            63.98,\
                            -19.7,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Eyjaf = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Eyjafjallajokull',\
                            63.63,\
                            -19.62,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Katla = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Katla',\
                            63.63,\
                            -19.05,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Bardarbunga = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Bardarbunga',\
                            64.63,\
                            -17.53,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Grimsvotn = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Grimsvotn',\
                            64.42,\
                            -17.33,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Askja = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Askja',\
                            65.03,\
                            -16.75,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Krafla = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Islande',\
                            'Krafla',\
                            65.73,\
                            -16.78,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Fayal = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Acores',\
                            'Fayal',\
                            38.6,\
                            -28.73,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Etna = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Italie',\
                            'Etna',\
                            37.734,\
                            15.004,\
                            3,\
                            3,\
                            3,\
                            8,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Vulcano = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Italie',\
                            'Vulcano',\
                            38.404,\
                            14.962,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Stromboli = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Italie',\
                            'Stromboli',\
                            38.789,\
                            15.213,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Vesuvius = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Italie',\
                            'Vesuvius',\
                            40.821,\
                            14.426,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Santorin = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Italie',\
                            'Santorini',\
                            36.404,\
                            25.396,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_LaPalma = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Canaries',\
                            'La Palma',\
                            28.57,\
                            -17.83,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Tenerife = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Canaries',\
                            'Tenerife',\
                            28.271,\
                            -16.641,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_SoufriereHills = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Soufriere Hills',\
                            16.72,\
                            -62.18,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_SoufriereGuadeloupe = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Soufriere de Guadeloupe',\
                            16.044,\
                            -61.664,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Watt = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Morne Watt',\
                            15.307,\
                            -61.305,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Pelee = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Pelee',\
                            14.809,\
                            -61.165,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_SoufStVincent = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Soufriere St. Vincent',\
                            13.33,\
                            -61.18,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_KickEmJenny = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Antilles',\
                            'Kick em Jenny',\
                            12.3,\
                            -61.64,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Fogo = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'CapVert',\
                            'Fogo',\
                            14.95,\
                            -24.35,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_YarJabal = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Jabal Yar',\
                            17.05,\
                            42.83,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Harras = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Harras of Dhamar',\
                            14.57,\
                            44.67,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Jebel = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Jebel at Tair',\
                            15.55,\
                            41.83,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Dalafilla = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Dalaffilla',\
                            13.793,\
                            40.553,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_ErtaAle = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Erta Ale',\
                            13.6,\
                            40.67,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Dabbahu = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Dabbahu',\
                            12.595,\
                            40.48,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Manda = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Rift',\
                            'Manda Hararo',\
                            12.17,\
                            40.82,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Cameroon = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Cameroon',\
                            'Cameroon',\
                            4.203,\
                            9.17,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_SantaIsabel = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Cameroon',\
                            'Santa Isabel',\
                            3.58,\
                            8.75,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Barrier = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'The Barrier',\
                            2.32,\
                            36.57,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Nyamuragira = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'Nyamuragira',\
                            -1.408,\
                            29.2,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Nyiragongo = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'Nyiragongo',\
                            -1.52,\
                            29.25,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Visoke = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'Visoke',\
                            -1.458,\
                            29.485,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Lengai = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'Ol Doinyo Lengai',\
                            -2.764,\
                            35.914,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Meru = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'East_Africa',\
                            'Meru',\
                            -3.25,\
                            36.75,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Karthala = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Karthala',\
                            'Karthala',\
                            -11.75,\
                            43.38,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_Reunion = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'Reunion',\
                            'Piton de la Fournaise',\
                            -21.244,\
                            55.708,\
                            1,\
                            2,\
                            3,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            TRUE,\
                            31,\
                            300,\
                            973,\
                            0.75);"

    SQL_volcan_ChaineDesPuys = "INSERT INTO hotvolc_volcanoes_monitoring (\
                            zone,\
                            volcan,\
                            lat,\
                            lon,\
                            offsetN_thermal,\
                            offsetS_thermal,\
                            offsetE_thermal,\
                            offsetW_thermal,\
                            offsetN_atmos,\
                            offsetS_atmos,\
                            offsetE_atmos,\
                            offsetW_atmos,\
                            volume_calculation,\
                            pix_area,\
                            Tamb,\
                            Tsurf,\
                            h)\
                    VALUES (\
                            'France',\
                            'Chaine des Puys',\
                            45.775,\
                            2.97,\
                            1,\
                            1,\
                            1,\
                            1,\
                            20,\
                            20,\
                            20,\
                            20,\
                            FALSE,\
                            9,\
                            300,\
                            973,\
                            0.75);"

    cur.execute(SQL_volcan_hekla)
    cur.execute(SQL_volcan_Eyjaf)
    cur.execute(SQL_volcan_Katla)
    cur.execute(SQL_volcan_Bardarbunga)
    cur.execute(SQL_volcan_Grimsvotn)
    cur.execute(SQL_volcan_Askja)
    cur.execute(SQL_volcan_Krafla)
    cur.execute(SQL_volcan_Fayal)
    cur.execute(SQL_volcan_Etna)
    cur.execute(SQL_volcan_Vulcano)
    cur.execute(SQL_volcan_Stromboli)
    cur.execute(SQL_volcan_Vesuvius)
    cur.execute(SQL_volcan_Santorin)
    cur.execute(SQL_volcan_LaPalma)
    cur.execute(SQL_volcan_Tenerife)
    cur.execute(SQL_volcan_SoufriereHills)
    cur.execute(SQL_volcan_SoufriereGuadeloupe)
    cur.execute(SQL_volcan_Watt)
    cur.execute(SQL_volcan_Pelee)
    cur.execute(SQL_volcan_SoufStVincent)
    cur.execute(SQL_volcan_KickEmJenny)
    cur.execute(SQL_volcan_Fogo)
    cur.execute(SQL_volcan_YarJabal)
    cur.execute(SQL_volcan_Harras)
    cur.execute(SQL_volcan_Jebel)
    cur.execute(SQL_volcan_Dalafilla)
    cur.execute(SQL_volcan_ErtaAle)
    cur.execute(SQL_volcan_Dabbahu)
    cur.execute(SQL_volcan_Manda)
    cur.execute(SQL_volcan_Cameroon)
    cur.execute(SQL_volcan_SantaIsabel)
    cur.execute(SQL_volcan_Barrier)
    cur.execute(SQL_volcan_Nyamuragira)
    cur.execute(SQL_volcan_Nyiragongo)
    cur.execute(SQL_volcan_Visoke)
    cur.execute(SQL_volcan_Lengai)
    cur.execute(SQL_volcan_Meru)
    cur.execute(SQL_volcan_Karthala)
    cur.execute(SQL_volcan_Reunion)
    cur.execute(SQL_volcan_ChaineDesPuys)

    con.commit()

    hotvolc_database.DB_close(con)


def init_table_volcans():
    """ """

    con = hotvolc_database.DB_connection()
    cur = con.cursor()

    file_csv = '/home/hotvolc/volcanoes.csv'

    if os.path.isfile(file_csv):
        with open(file_csv) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            header = next(csvreader)

            for line in csvreader:
                buffer_volcan = {}
                buffer_volcan['gnvid'] = line[0]
                buffer_volcan['name'] = line[4]
                buffer_volcan['latx'] = line[7]
                buffer_volcan['longx'] = line[10]
                buffer_volcan['elevation'] = int(line[12])
                buffer_volcan['type'] = line[13]
                buffer_volcan['status'] = line[6]

                boolean = buffer_volcan['status'] == 'Historical'

                name = re.sub("'", " ", buffer_volcan['name'])

                SQL = "INSERT INTO hotvolc_volcanoes (\
                            name,\
                            position,\
                            elevation,\
                            gnvid,\
                            type,\
                            status,\
                            display)\
                        VALUES (\
                            '%s',\
                            ST_GeographyFromText('SRID=4326;POINT(%s %s)'),\
                            %d,\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s');" % (name,
                                       buffer_volcan['longx'],
                                       buffer_volcan['latx'],
                                       buffer_volcan['elevation'],
                                       buffer_volcan['gnvid'],
                                       buffer_volcan['type'],
                                       buffer_volcan['status'],
                                       boolean)
                cur.execute(SQL)
    con.commit()
    hotvolc_database.DB_close(con)
