#!/usr/bin/python
# -*-coding:Utf-8 -*

import bz2
import os
import pickle

import numpy


def readFullMSGRaw(chemin, can, yyyy, mm, dd, HH, MM):
    """Fonction de lecture des donnÃ©es au format 'raw', ouvre et assemble tous les segments
    renvoie les donnÃ©es full globe en CN.

    @param can: NumÃ©ro du canal MSG ex: 1 -> VIS0.6, 4 -> IR3.9, etc...
    @type can: int
    @param yyyy: year
    @type yyyy: int
    @param mm: month
    @type mm: int
    @param dd: day
    @type dd: int
    @param HH: hour
    @type HH: int
    @param MM: minutes
    @type MM: int"""

    canx = ['VIS006', 'VIS008', 'IR_016', 'IR_039', 'WV_062', 'WV_073',
            'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134', 'HRV__']

    if canx[can - 1] != 'HRV__':

        # prepare une matrice vide de la taille d'une image MSG
        full = numpy.empty([3712, 3712])

        # Initialisation de l'offset
        offset = 0

        # Liste des segments
        seg = ['_01', '_02', '_03', '_04', '_05', '_06', '_07', '_08']

        for c in seg:  # on parcourt tous les segments

            # prÃ©paration du chemin d'accÃ¨s au fichier
            file_path = "%s%d%02d%02d/MSG_0/%s%d%02d%02d%02d%02d%s.raw" % (chemin, yyyy, mm, dd, canx[can - 1],
                                                                           yyyy, mm, dd, HH, MM, c)

            if os.path.isfile(file_path):  # on teste si le fichier non compressÃ© existe

                # print file_path+' est déjà décompressé'
                if os.stat(file_path).st_size == 0:
                    data = numpy.ones((464, 3712)) * -1
                else:
                    with open(file_path) as fp:  # On ouvre le fichier
                        data = numpy.fromfile(fp, dtype=numpy.uint16).reshape(464,
                                                                              3712)  # RÃ©cupÃ¨re les donnÃ©es dans un buffer

                full[offset:offset + 464,
                0:3712] = data  # On place le buffer dans la matrice globale initialement crÃ©e
                offset += 464  # on incrÃ©mente l'offset pour placer le segments suivant Ã  la suite

            # Si le fichier non compressÃ© n'existe pas on teste si le fichier compressÃ© existe
            elif os.path.isfile(file_path + '.bz2'):

                # print file_path+'.bz2'+' est compressÃ©'

                if os.stat(file_path + '.bz2').st_size == 0:
                    data = numpy.ones((464, 3712)) * -1
                else:
                    zipfile = bz2.BZ2File(file_path + '.bz2').read()  # On lit les donnÃ©es compressÃ©s
                    data = numpy.frombuffer(zipfile, dtype=numpy.uint16).reshape(464,
                                                                                 3712)  # On les stocke dans un buffer numpy

                full[offset:offset + 464, 0:3712] = data  # On place le buffer dans la matrice globale
                offset += 464  # on incrÃ©mente l'offset

            # Si le fichier n'existe ni en version compressÃ© et non compressÃ© bah Ã§a marche plus...
            else:
                print
                "Le fichier %s n'existe pas..." % file_path

                # Du coup on remplie le segment Ã  coup de valeurs nÃ©gatives qui seront masquÃ©es par la suite.
                data = numpy.ones((464, 3712)) * -1
                full[offset:offset + 464, 0:3712] = data
                offset += 464

    else:  # parce que le HRV c'est carrÃ¨ment chiant Ã  gÃ©rer
        print
        "SÃ©rieusement vous voulez travailler avec du HRV ?"
        # A FAIRE...

    # On remet l'image dans le bon sens (car acquisition MSG du S vers N et d'E vers W
    full = full[3712:0:-1, 3712:0:-1]

    # voir le rÃ©sultat:
    # plt.imshow(full, interpolation='nearest')
    # plt.show()

    return full


def readRoiMSGRaw(chemin, can, x_max, x_min, y_max, y_min, yyyy, mm, dd, HH, MM):
    """Fonction d'extraction de donnÃ©es au format 'raw' en spÃ©cifiant la zone d'extraction
    avec les indices x, y (ligne, colonne).

    Attention les coordonnÃ©es x, y sont spÃ©cifiÃ©es pour une image MSG remise dans le 'bon' sens.
    C'est Ã  dire avec le coin sup/gauche correspondant au nord-ouest et le coin inf/droit au
    sud-est. (C'est l'inverse lors de l'acquisition)

    @param can: NumÃ©ro du canal MSG ex: 1 -> VIS0.6, 4 -> IR3.9, etc...
    @type can: int
    @param x_max: ligne maximale
    @type x_max: int
    @param x_min: ligne minimale
    @type x_min: int
    @param y_max: colonne maximale
    @type y_max: int
    @param y_min: colonne minimale
    @type y_min: int
    @param yyyy: year
    @type yyyy: int
    @param mm: month
    @type mm: int
    @param dd: day
    @type dd: int
    @param HH: hour
    @type HH: int
    @param MM: minutes
    @type MM: int"""

    # On rÃ©cupÃ¨re l'image globale
    tmp = readFullMSGRaw(chemin, can, yyyy, mm, dd, HH, MM)
    # On coupe la zone qui nous interesse
    ROI = tmp[x_min:x_max, y_min:y_max]

    # voir le rÃ©sultat:
    # plt.imshow(ROI, interpolation='nearest')
    # plt.show()
    #
    return ROI


def readZoneMSGRaw(chemin, can, latmin, latmax, lonmin, lonmax, yyyy, mm, dd, HH, MM):
    """Fonction qui permet d'extraire d'une image 'raw' la meme zone que celle obtenue
    Ã  l'aide de mpop.

    @param can: NumÃ©ro du canal MSG ex: 1 -> VIS0.6, 4 -> IR3.9, etc...
    @type can: int
    @param latmin: latitude coin inf/droit
    @type latmin: float
    @param latmax: latitude coin sup/gauche
    @type latmax: float
    @param lonmin: longitude coin sup/gauche
    @type lonmin: float
    @param lonmax: longitude coin inf/droit
    @type lonmax: float
    @param yyyy: year
    @type yyyy: int
    @param mm: month
    @type mm: int
    @param dd: day
    @type dd: int
    @param HH: hour
    @type HH: int
    @param MM: minutes
    @type MM: int"""

    file_latlon = '/home/jdecriem/dev/HOTVOLC/latlon.pckl'

    UL = [latmax, lonmin]
    DR = [latmin, lonmax]

    # RÃ©cupÃ¨re les matrices de lat et lon full globe dans le fichier pickle
    f = open(file_latlon)
    LAT, LON = pickle.load(f)
    f.close()

    LAT = numpy.round(LAT, 9)
    LON = numpy.round(LON, 9)

    # Retrouve les coordonnÃ©es x, y correspondant au lat/lon du coin UL
    tmp = numpy.where(LAT == UL[0])
    tmp2 = numpy.where(LON == UL[1])
    idxUL = []
    idxUL.append(numpy.unique([val for val in tmp[0] if val in tmp2[0]]))
    idxUL.append(numpy.unique([val for val in tmp[1] if val in tmp2[1]]))

    # Retrouve les coordonnÃ©es x, y correspondant au lat/lon du coin DR
    tmp = numpy.where(LAT == DR[0])
    tmp2 = numpy.where(LON == DR[1])
    idxDR = []
    idxDR.append(numpy.unique([val for val in tmp[0] if val in tmp2[0]]))
    idxDR.append(numpy.unique([val for val in tmp[1] if val in tmp2[1]]))

    # print '%d - %d / %d - %d' % (idxDR[0][0], idxUL[0][0], idxDR[1][0], idxUL[1][0])

    # Extrait la zone spÃ©cifiÃ©e
    zone = readRoiMSGRaw(chemin, can, idxDR[0][0] + 1, idxUL[0][0], idxDR[1][0] + 1, idxUL[1][0], yyyy, mm, dd, HH, MM)

    ##voir le rÃ©sultat:
    # plt.imshow(zone, interpolation='nearest')
    # plt.show()

    return zone

###########################################################################################################
##z = "Acores"
##zone = z+"_sp"
##zone_mp = z+"_mp"
##yyyy = 2014
##mm = 12
##dd = 14
##HH = 14
##MM = 30
##
##chemin = '/home/dvb/recv-class/'
##
##time_slot = datetime.datetime(yyyy, mm, dd, HH, MM)
##data = GeostationaryFactory.create_scene("meteosat", "09", "seviri", time_slot)
##data_area = get_area_def(zone)
##data.load([12.0], area_extent = data_area.area_extent, calibrate = 0)
##
##data[12.0].data = numpy.ma.masked_where(data[12.0].data < 0, data[12.0].data)
##lon, lat = data[12.0].area.get_lonlats()
##
##plt.imshow(data[12.0].data, interpolation='nearest')
##plt.show()
##   
##lat = numpy.round(lat, 9)
##lon = numpy.round(lon, 9)
##
##latmax = lat[0,0]
##latmin = lat[lat.shape[0]-1, lat.shape[1]-1]
##lonmax = lon[lon.shape[0]-1, lon.shape[1]-1]
##lonmin = lon[0,0]
##
##img  = readZoneMSGRaw(chemin, 10, latmin, latmax, lonmin, lonmax, yyyy, mm, dd, HH, MM)
####img  = readRoiMSGRaw(chemin, 10, 242,98,1837,1357, yyyy, mm, dd, HH, MM)
####img = numpy.ma.masked_where(img<0, img)
####
##plt.imshow(img, interpolation='nearest')
##plt.show()
##
##A = data[12.0].data-img
##plt.imshow(A, interpolation='nearest')
##plt.show()
##
##
##
##
