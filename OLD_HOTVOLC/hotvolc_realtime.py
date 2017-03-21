#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from multiprocessing import Process

import hotvolc_database
import hotvolc_main


def hotvolc_realtime(date, raw=False):
    """ Calcul des produits pour toutes les zones et pour une date
    argument: date au format YYYYMMDDHHMM avec MM = 00, 15, 30 ou 45
    argument: raw=True si utilisation des donnÃ©es MSG en raw"""
    yyyy = int(date[0:4])
    mm = int(date[4:6])
    dd = int(date[6:8])
    HH = int(date[8:10])
    MM = int(date[10:12])

    con = hotvolc_database.DB_connection()
    cur = con.cursor()
    cur.execute('SELECT zone FROM hotvolc_zones;')
    zones = cur.fetchall()
    con.commit()
    cur.close()
    hotvolc_database.DB_close(con)
    for zone in zones:
        print
        "\n***** DÃ©but du traitement de la zone: {0} *****".format(zone[0])
        p = Process(target=hotvolc_main.hotvolc_main, args=(zone[0], yyyy, mm, dd, HH, MM,), kwargs={'raw': raw})
        p.start()


def ParseBoolean(b):
    # Traduction des booleens
    if len(b) < 1:
        raise ValueError('Cannot parse empty string into boolean.')
    b = b[0].lower()
    if b == 't' or b == 'y' or b == '1':
        return True
    if b == 'f' or b == 'n' or b == '0':
        return False
    raise ValueError('Cannot parse string into boolean.')


if __name__ == "__main__":
    # print len(sys.argv)
    if len(sys.argv) == 2:
        hotvolc_realtime(sys.argv[1], False)
    else:
        if len(sys.argv) == 3:
            hotvolc_realtime(sys.argv[1], ParseBoolean(sys.argv[2]))
        else:
            print
            'This function can take only two argument:\n \
                       ->(required) The date : a string in the format "yyyymmddHHMM" example : "201412141430"\n \
                       ->(optional) a Boolean set to True if you want to use .raw files'
