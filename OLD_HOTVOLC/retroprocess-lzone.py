#!/usr/bin/python
# retroprocessing pour une zone specifique
# entre 2 dates

import datetime

import hotvolc_main

date1 = '201509281230'
date2 = '201510091330'
zone = 'Reunion'
debut = datetime.datetime.strptime(date1, '%Y%m%d%H%M')
fin = datetime.datetime.strptime(date2, '%Y%m%d%H%M')
step = datetime.timedelta(minutes=15)
i = 0

while debut <= fin:
    print
    debut.strftime('%Y%m%d%H%M')
    yy = debut.year
    mm = debut.month
    dd = debut.day
    HH = debut.hour
    MM = debut.minute
    hotvolc_main.hotvolc_main(zone, yy, mm, dd, HH, MM, raw=True)
    debut += step
    i += 1

print
i
