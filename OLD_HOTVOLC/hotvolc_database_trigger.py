#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

import hotvolc_database
import psycopg2

zones = ['Cameroon', 'CapVert', 'Antilles', 'Islande', 'Rift', 'France', 'Acores', 'East_Africa', 'Italie', 'Canaries',
         'Karthala', 'Reunion']
annee = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
         2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035,
         2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049,
         2050]


def drop_trigger_mere_fille():
    """Fonction qui permet de supprimer les triggers et fonctions associees qui gerent la redirection automatique
    des donnees de la table mere vers les tables filles"""
    con = hotvolc_database.DB_connection()

    try:
        with con:
            with con.cursor() as cur:
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_atmos_trigger() CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_anomalies_trigger() CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_legend_trigger() CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_nti_trigger() CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_thermal_trigger() CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS hotvolc_quality_trigger() CASCADE;")
        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        hotvolc_database.DB_close(con)


def create_trigger_mere_fille():
    """Fonction de creation des triggers qui vont permettre, lors d'une insertion de donnees dans la BDD,
    d'automatiquement rediriger les donnees de la table mere vers la table fille correspondante en fonction du nom
    de la zone volcanique"""

    con = hotvolc_database.DB_connection()

    try:
        # Creation Trigger sur la table mere hotvolc_atmospheric monitoring
        with con:
            with con.cursor() as cur:
                sql_atmos = "CREATE OR REPLACE FUNCTION hotvolc_atmos_trigger()\n" \
                            "RETURNS TRIGGER AS $$\n" \
                            "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_atmos_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_atmos += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_atmos_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_atmos += sql_tmp
                sql_atmos += "ELSE\n" \
                             "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                             "END IF;\n" \
                             "RETURN NULL;\n" \
                             "END;\n" \
                             "$$\n" \
                             "LANGUAGE plpgsql;"
                cur.execute(sql_atmos)

                cur.execute("CREATE TRIGGER insert_hotvolc_atmos_trigger BEFORE INSERT ON "
                            "hotvolc_atmospheric_monitoring FOR EACH ROW EXECUTE PROCEDURE hotvolc_atmos_trigger();")

        # Creation du Trigger sur la table mere hotvolc_thermal_anomalies
        with con:
            with con.cursor() as cur:
                sql_anomalies = "CREATE OR REPLACE FUNCTION hotvolc_anomalies_trigger()\n" \
                                "RETURNS TRIGGER AS $$\n" \
                                "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_anomalies_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_anomalies += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_anomalies_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_anomalies += sql_tmp
                sql_anomalies += "ELSE\n" \
                                 "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                                 "END IF;\n" \
                                 "RETURN NULL;\n" \
                                 "END;\n" \
                                 "$$\n" \
                                 "LANGUAGE plpgsql;"
                cur.execute(sql_anomalies)

                cur.execute("CREATE TRIGGER insert_hotvolc_anomalies_trigger BEFORE INSERT ON "
                            "hotvolc_thermal_anomalies FOR EACH ROW EXECUTE PROCEDURE hotvolc_anomalies_trigger();")

        # Creation du Trigger sur la table mere hotvolc_legend_label
        with con:
            with con.cursor() as cur:
                sql_legend = "CREATE OR REPLACE FUNCTION hotvolc_legend_trigger()\n" \
                             "RETURNS TRIGGER AS $$\n" \
                             "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_legend_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_legend += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_legend_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_legend += sql_tmp
                sql_legend += "ELSE\n" \
                              "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                              "END IF;\n" \
                              "RETURN NULL;\n" \
                              "END;\n" \
                              "$$\n" \
                              "LANGUAGE plpgsql;"
                cur.execute(sql_legend)

                cur.execute("CREATE TRIGGER insert_hotvolc_legend_trigger BEFORE INSERT ON "
                            "hotvolc_legend_label FOR EACH ROW EXECUTE PROCEDURE hotvolc_legend_trigger();")

        # Creation du Trigger sur la table mere hotvolc_nti_monitoring
        with con:
            with con.cursor() as cur:
                sql_nti = "CREATE OR REPLACE FUNCTION hotvolc_nti_trigger()\n" \
                          "RETURNS TRIGGER AS $$\n" \
                          "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_nti_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_nti += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_nti_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_nti += sql_tmp
                sql_nti += "ELSE\n" \
                           "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                           "END IF;\n" \
                           "RETURN NULL;\n" \
                           "END;\n" \
                           "$$\n" \
                           "LANGUAGE plpgsql;"
                cur.execute(sql_nti)

                cur.execute("CREATE TRIGGER insert_hotvolc_nti_trigger BEFORE INSERT ON "
                            "hotvolc_nti_monitoring FOR EACH ROW EXECUTE PROCEDURE hotvolc_nti_trigger();")

        # Creation du Trigger sur la table mere hotvolc_thermal_monitoring
        with con:
            with con.cursor() as cur:
                sql_thermal = "CREATE OR REPLACE FUNCTION hotvolc_thermal_trigger()\n" \
                              "RETURNS TRIGGER AS $$\n" \
                              "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_thermal_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_thermal += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_thermal_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_thermal += sql_tmp
                sql_thermal += "ELSE\n" \
                               "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                               "END IF;\n" \
                               "RETURN NULL;\n" \
                               "END;\n" \
                               "$$\n" \
                               "LANGUAGE plpgsql;"
                cur.execute(sql_thermal)

                cur.execute("CREATE TRIGGER insert_hotvolc_thermal_trigger BEFORE INSERT ON "
                            "hotvolc_thermal_monitoring FOR EACH ROW EXECUTE PROCEDURE hotvolc_thermal_trigger();")

        with con:
            with con.cursor() as cur:
                sql_quality = "CREATE OR REPLACE FUNCTION hotvolc_quality_trigger()\n" \
                              "RETURNS TRIGGER AS $$\n" \
                              "BEGIN\n"
                for index, zone in enumerate(zones):
                    if index == 0:
                        sql_tmp = "IF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_quality_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_quality += sql_tmp
                    else:
                        sql_tmp = "ELSIF (NEW.zone = '%s') THEN INSERT INTO z_hotvolc_quality_%s VALUES (NEW.*);\n" % \
                                  (zone, zone)
                        sql_quality += sql_tmp
                sql_quality += "ELSE\n" \
                               "RAISE EXCEPTION 'Le nom de la zone ne correspond pas';\n" \
                               "END IF;\n" \
                               "RETURN NULL;\n" \
                               "END;\n" \
                               "$$\n" \
                               "LANGUAGE plpgsql;"
                cur.execute(sql_quality)

                cur.execute("CREATE TRIGGER insert_hotvolc_quality_trigger BEFORE INSERT ON "
                            "hotvolc_quality_flag FOR EACH ROW EXECUTE PROCEDURE hotvolc_quality_trigger();")

        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)
    finally:
        hotvolc_database.DB_close(con)


def drop_trigger_fille_petitefille():
    """Fonction qui permet de supprimer les triggers et fonctions associees qui gerent la redirection automatique
    des donnees des tables filles vers les tables petites-filles"""

    con = hotvolc_database.DB_connection()

    try:
        for zone in zones:
            with con:
                with con.cursor() as cur:
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_anomalies_%s_date_trigger() CASCADE;" % zone)
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_atmos_%s_date_trigger() CASCADE;" % zone)
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_legend_%s_date_trigger() CASCADE;" % zone)
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_nti_%s_date_trigger() CASCADE;" % zone)
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_thermal_%s_date_trigger() CASCADE;" % zone)
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_volume_date_trigger() CASCADE;")
                    cur.execute("DROP FUNCTION IF EXISTS hotvolc_quality_%s_date_trigger() CASCADE;" % zone)

        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)

    finally:
        hotvolc_database.DB_close(con)


def create_trigger_fille_petitefille():
    """Fonction de creation des triggers qui vont permettre lors d'une insertion de donnees dans la BDD
    d'automatiquement rediriger les donnees des tables filles vers les tables petite-filles correspondantes
    en fonction de la date de l'enregistrement"""

    con = hotvolc_database.DB_connection()
    try:
        # Creation des Triggers sur les tables filles z_hotvolc_anomalies_ZONE
        for zone in zones:
            sql_anomalie = "CREATE OR REPLACE FUNCTION hotvolc_anomalies_%s_date_trigger()\n" \
                           "RETURNS TRIGGER AS $$\n" \
                           "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_anomalies_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_anomalie += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_anomalies_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_anomalie += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_anomalies_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_anomalie += sql_tmp
                i += 1

            sql_anomalie += "ELSE\n" \
                            "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                            "END IF;\n" \
                            "RETURN NULL;\n" \
                            "END;\n" \
                            "$$\n" \
                            "LANGUAGE plpgsql;"

            sql_anomalie_trigger = "CREATE TRIGGER insert_hotvolc_anomalies_%s_date_trigger BEFORE INSERT ON " \
                                   "z_hotvolc_anomalies_%s FOR EACH ROW EXECUTE PROCEDURE " \
                                   "hotvolc_anomalies_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_anomalie)
                    cur.execute(sql_anomalie_trigger)

        # Creation des Triggers sur les tables filles z_hotvolc_atmos_ZONE
        for zone in zones:
            sql_atmos = "CREATE OR REPLACE FUNCTION hotvolc_atmos_%s_date_trigger()\n" \
                        "RETURNS TRIGGER AS $$\n" \
                        "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_atmos_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_atmos += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_atmos_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_atmos += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_atmos_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_atmos += sql_tmp
                i += 1

            sql_atmos += "ELSE\n" \
                         "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                         "END IF;\n" \
                         "RETURN NULL;\n" \
                         "END;\n" \
                         "$$\n" \
                         "LANGUAGE plpgsql;"

            sql_atmos_trigger = "CREATE TRIGGER insert_hotvolc_atmos_%s_date_trigger BEFORE INSERT ON " \
                                "z_hotvolc_atmos_%s FOR EACH ROW EXECUTE PROCEDURE " \
                                "hotvolc_atmos_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_atmos)
                    cur.execute(sql_atmos_trigger)

        # Creation des Trigger sur les tables filles z_hotvolc_legend_ZONE
        for zone in zones:
            sql_legend = "CREATE OR REPLACE FUNCTION hotvolc_legend_%s_date_trigger()\n" \
                         "RETURNS TRIGGER AS $$\n" \
                         "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_legend_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_legend += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_legend_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_legend += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_legend_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_legend += sql_tmp
                i += 1

            sql_legend += "ELSE\n" \
                          "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                          "END IF;\n" \
                          "RETURN NULL;\n" \
                          "END;\n" \
                          "$$\n" \
                          "LANGUAGE plpgsql;"

            sql_legend_trigger = "CREATE TRIGGER insert_hotvolc_legend_%s_date_trigger BEFORE INSERT ON " \
                                 "z_hotvolc_legend_%s FOR EACH ROW EXECUTE PROCEDURE " \
                                 "hotvolc_legend_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_legend)
                    cur.execute(sql_legend_trigger)

        # Creation des Triggers sur les tables filles z_hotvolc_nti_ZONE
        for zone in zones:
            sql_nti = "CREATE OR REPLACE FUNCTION hotvolc_nti_%s_date_trigger()\n" \
                      "RETURNS TRIGGER AS $$\n" \
                      "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_nti_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_nti += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_nti_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_nti += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_nti_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_nti += sql_tmp
                i += 1

            sql_nti += "ELSE\n" \
                       "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                       "END IF;\n" \
                       "RETURN NULL;\n" \
                       "END;\n" \
                       "$$\n" \
                       "LANGUAGE plpgsql;"

            sql_nti_trigger = "CREATE TRIGGER insert_hotvolc_nti_%s_date_trigger BEFORE INSERT ON " \
                              "z_hotvolc_nti_%s FOR EACH ROW EXECUTE PROCEDURE " \
                              "hotvolc_nti_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_nti)
                    cur.execute(sql_nti_trigger)

        # Creation des Triggers sur les tables filles z_hotvolc_thermal_ZONE
        for zone in zones:
            sql_thermal = "CREATE OR REPLACE FUNCTION hotvolc_thermal_%s_date_trigger()\n" \
                          "RETURNS TRIGGER AS $$\n" \
                          "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_thermal_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_thermal += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_thermal_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_thermal += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_thermal_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_thermal += sql_tmp
                i += 1

            sql_thermal += "ELSE\n" \
                           "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                           "END IF;\n" \
                           "RETURN NULL;\n" \
                           "END;\n" \
                           "$$\n" \
                           "LANGUAGE plpgsql;"

            sql_thermal_trigger = "CREATE TRIGGER insert_hotvolc_thermal_%s_date_trigger BEFORE INSERT ON " \
                                  "z_hotvolc_thermal_%s FOR EACH ROW EXECUTE PROCEDURE " \
                                  "hotvolc_thermal_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_thermal)
                    cur.execute(sql_thermal_trigger)

        # Creation des Triggers sur les tables filles z_hotvolc_thermal_ZONE
        for zone in zones:
            sql_quality = "CREATE OR REPLACE FUNCTION hotvolc_quality_%s_date_trigger()\n" \
                          "RETURNS TRIGGER AS $$\n" \
                          "BEGIN\n" % zone
            i = 0
            while i < len(annee):
                if i == 0:
                    sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_quality_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_quality += sql_tmp
                elif annee[i] == 2050:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_quality_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], 2051, zone, annee[i])
                    sql_quality += sql_tmp
                else:
                    sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                              "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                              "THEN INSERT INTO zz_hotvolc_quality_%s_%s VALUES (NEW.*);\n" % \
                              (annee[i], annee[i + 1], zone, annee[i])
                    sql_quality += sql_tmp
                i += 1

            sql_quality += "ELSE\n" \
                           "RAISE EXCEPTION 'La date est en dehors des limites ';\n" \
                           "END IF;\n" \
                           "RETURN NULL;\n" \
                           "END;\n" \
                           "$$\n" \
                           "LANGUAGE plpgsql;"

            sql_quality_trigger = "CREATE TRIGGER insert_quality_thermal_%s_date_trigger BEFORE INSERT ON " \
                                  "z_hotvolc_quality_%s FOR EACH ROW EXECUTE PROCEDURE " \
                                  "hotvolc_quality_%s_date_trigger();" % (zone, zone, zone)

            with con:
                with con.cursor() as cur:
                    cur.execute(sql_quality)
                    cur.execute(sql_quality_trigger)

        # Cas particulier de la table mere hotvolc_lava_volume qui n'a pas de tables filles definies par la zone
        # de leur enregistrement mais qui stocke directement ses donnees dans des tables classees par date
        sql_volume = "CREATE OR REPLACE FUNCTION hotvolc_volume_date_trigger() \n" \
                     "RETURNS TRIGGER AS $$\n" \
                     "BEGIN\n"

        i = 0

        while i < len(annee):
            if i == 0:
                sql_tmp = "IF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                          "THEN INSERT INTO z_hotvolc_volume_%s VALUES (NEW.*);\n" % \
                          (annee[i], annee[i + 1], annee[i])
                sql_volume += sql_tmp
            elif annee[i] == 2050:
                sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                          "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                          "THEN INSERT INTO z_hotvolc_volume_%s VALUES (NEW.*);\n" % \
                          (annee[i], 2051, annee[i])
                sql_volume += sql_tmp
            else:
                sql_tmp = "ELSIF (NEW.date >= TIMESTAMP '%s-01-01 00:00:00' " \
                          "AND NEW.date < TIMESTAMP '%s-01-01 00:00:00') " \
                          "THEN INSERT INTO z_hotvolc_volume_%s VALUES (NEW.*);\n" % \
                          (annee[i], annee[i + 1], annee[i])
                sql_volume += sql_tmp
            i += 1

        sql_volume += "ELSE\n" \
                      "RAISE EXCEPTION 'La date est en dehors des limites';\n" \
                      "END IF;\n" \
                      "RETURN NULL;\n" \
                      "END;\n" \
                      "$$\n" \
                      "LANGUAGE plpgsql;"

        sql_volume_trigger = "CREATE TRIGGER insert_hotvolc_volume_date_trigger BEFORE INSERT ON " \
                             "hotvolc_lava_volume FOR EACH ROW EXECUTE PROCEDURE " \
                             "hotvolc_volume_date_trigger();"

        with con:
            with con.cursor() as cur:
                cur.execute(sql_volume)
                cur.execute(sql_volume_trigger)

        con.commit()

    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
            hotvolc_database.DB_close(con)
        print
        'Error %s' % e
        sys.exit(1)
    finally:
        hotvolc_database.DB_close(con)
